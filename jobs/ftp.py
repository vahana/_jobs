import logging
import ftplib
from slovar import slovar

import prf
from prf.utils import DValueError

from jobs.job import BaseJob

log = logging.getLogger(__name__)

class FTPJob(BaseJob):

    @classmethod
    def login(cls, params):
        try:
            settings = params.source.get('ftp', slovar())

            ftp = ftplib.FTP(settings.url)
            ftp.login(settings.get('user'), settings.get('password'))
            ftp.cwd(settings.get('dir', '.'))
            return ftp

        except ftplib.all_errors as e:
            raise DValueError('FTP error: %s' % e)

    @classmethod
    def read(cls, params):
        #put some dummy data so post_to_target gets triggered
        yield {'dummy':'dummy'}

    def create(self):
        if 'ls' in self._params:
            self._params.set_default('dry_run', True)
            self._params.set_default('source.name', 'DRY_RUN')

        obj = super(FTPJobView, self).create()

        self.setup(self._params)

        if 'ls' in self._params:
            ftp = self.login(self._params)
            return slovar(
                id = None,
                ls = ftp.nlst()
            )

        return obj

    @classmethod
    def post_to_target(cls, params, dataset):
        target_fh = None

        def progress(block):
            target_fh.write(block)
            cls.inc_progress(len(block), 'source')
            cls.inc_progress(len(block), 'target')
            cls.update_job()

        try:
            target_fh = open(params.target.file_path, 'wb')
            ftp = cls.login(params)
            ftp.retrbinary("RETR " + params.source.name, progress, 1000000)

        except ftplib.all_errors as e:
            raise DValueError('FTP error: %s' % e)

        finally:
            if target_fh:
                target_fh.close()

