import datetime
import redis
import rq

from slovar import slovar

from jobs.job import BaseJob, JobHelper
from . import PyramidTestCase


class TestJobMixin(PyramidTestCase):

    def _setup(self, config):
        self.request.params = slovar({
                'job_name': 'test-job',
                'job_url': 'http://localhost:6544/test',
                'source': {},
                'target': {'name': 'test-ds'}
        })
        # job = BaseJob('queue_name', self.request.params)

    def test_job_setup(self):
        """
        Ensure setup configures the job correctly
        """
        with rq.Connection(redis.Redis()):
            job = BaseJob('queue_name', self.request.params)
            job.setup(self.request.params)

        self.assertEqual(
            self.request.params.job_url,
            'http://localhost:6544/test')


    def test_job2dict(self):
        job = rq.job.Job(connection=redis.Redis())
        job._data = {}
        job.enqueued_at = datetime.datetime.utcnow()
        assert isinstance(job.created_at, datetime.datetime)
        assert isinstance(job.to_dict()['created_at'], str)
        assert isinstance(JobHelper.job2dict(job)['created_at'], datetime.datetime)
