import logging
import boto3
import os

from slovar import slovar

from datasets.backends.s3 import S3
from datasets.backends.csv import CSV

from jobs.job import BaseJob

log = logging.getLogger(__name__)

BACKENDS = [
    's3',
    'csv',
    'mongo'
]

def csv_to_s3(src, trg, callback=None):
    srv_ob = CSV(src)
    trg_ob = S3(trg)

    s3 = boto3.resource('s3')
    key = trg_ob.file_name.split(trg_ob.bucket.name)[1].strip('/')
    s3.meta.client.upload_file(src_ob.file_name, trg_ob.bucket.name, key, Callback=callback)


def s3_to_csv(src, trg, callback=None):
    src_ob = S3(src)
    trg_ob = CSV(trg, create=True)

    s3 = boto3.resource('s3')
    key = src_ob.file_name.split(src_ob.bucket.name)[1].strip('/')
    s3.meta.client.download_file(src_ob.bucket.name, key, trg_ob.file_name, Callback=callback)


def csv_to_mongo(src, trg, callback=None):
    src_ob = CSV(src)

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
