import logging
import boto3
import os

from slovar import slovar

from prf.s3 import S3
from prf.csv import CSV

import impala

from jobs.job import BaseJob

log = logging.getLogger(__name__)

BACKENDS = [
    's3',
    'csv',
    'mongo'
]

def csv_to_s3(src, trg, callback=None):
    src_ob = CSV(src, root_path=impala.Settings.get('csv.root'))
    trg_ob = S3(trg, create=True)

    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(src_ob.file_name, trg_ob.bucket.name, trg_ob.path, Callback=callback)


def s3_to_csv(src, trg, callback=None):
    src_ob = S3(src)
    trg_ob = CSV(trg, create=True, root_path=impala.Settings.get('csv.root'))

    s3 = boto3.resource('s3')
    key = src_ob.file_name.split(src_ob.bucket.name)[1].strip('/')
    s3.meta.client.download_file(src_ob.bucket.name, key, trg_ob.file_name, Callback=callback)


def csv_to_mongo(src, trg, callback=None):
    src_ob = CSV(src, root_path=impala.Settings.get('csv.root'))

    if not os.path.isfile(src_ob.file_name):
        log.warning('%s is not a file. skipping.' % src_ob.file_name)
        return

    cmd = 'mongoimport --type csv --headerline -d %s %s%s' % (trg.ns, '--drop ' if trg.get('drop') else '', src_ob.file_name)

    if trg.get('drop'):
        log.warning('DROPPING before import !')
    log.warning('RUNNING >>>\n`%s`' % cmd)

    success = os.system(cmd)


class CPJob(BaseJob):

    @classmethod
    def progress(cls, bytes):
        cls.inc_progress(bytes, 'target', flush=True)

    @classmethod
    def read_loop(cls, params):
        src = params.source
        trg = params.target

        sbe = src.backend
        tbe = trg.backend

        if sbe not in BACKENDS:
            raise prf_exc.HTTPBadRequest('%s backend is not supported.' % sbe)

        if tbe not in BACKENDS:
            raise prf_exc.HTTPBadRequest('%s backend is not supported.' % tbe)

        if sbe == 's3' and tbe == 'csv':
            s3_to_csv(src, trg, cls.progress)

        elif sbe == 'csv' and tbe == 's3':
            csv_to_s3(src, trg, cls.progress)

        elif sbe == 'csv' and tbe == 'mongo':
            csv_to_mongo(src, trg, cls.progress)

        else:
            raise KeyError('%s are not supported' % ([sbe, tbe]))
