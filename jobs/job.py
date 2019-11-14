import rq
import time
import logging
import cProfile

from slovar import slovar
from prf.utils import (chunks, sanitize_url, DValueError, maybe_dotted)
from prf.request import Request
import prf.exc
from prf.utils import NOW

from datasets.backends import Backend
from datasets.backends.mongo import connect_dataset_aliases, registered_namespaces

import jobs
from jobs.model import Job

log = logging.getLogger(__name__)


def enable_pylog2es(es_index='jobs', log_level=logging.INFO, logger=None, **extra):
    from jobs.job import JobHelper
    from cmreslogging.handlers import CMRESHandler

    job = JobHelper.add_job_info({})
    if job:
        es_index = 'logs.job_%s' % job.job.uid
        extra['job'] = job.job

    handler = CMRESHandler(
                hosts=[{'host': 'localhost', 'port': 9200}],
                # buffer_size = 4096,
                # flush_frequency_in_sec = 5,
                es_index_name=es_index,
                es_additional_fields=extra,
                raise_on_indexing_exceptions=True,
                )

    #override built-in behavior of naming indexes
    handler._index_name_func = staticmethod(lambda x:x)

    log = logger or logging.getLogger()
    log.setLevel(log_level)
    log.addHandler(handler)
    return log


def _profiler(method):

    def decorated(cls, params):
        if params.asbool('profile', default=False):
            profiler = cProfile.Profile()
            profiler.enable()

        method(cls, params)

        if params.profile:
            profiler.disable()
            profiler.dump_stats('%s.pstat' % NOW('_'))

    return decorated


class _BaseJob(object):
    JOB_DEFAULT_TIMEOUT = 60*60*24 # a day

    def __init__(self, name, params):
        self.params = params
        self.queue = rq.Queue(name, async=self.is_async(params))

    @classmethod
    def setup(cls, params):
        pass

    @classmethod
    def run(cls, paras):
        pass

    @classmethod
    def process(cls, params, job_log, data):
        return Backend(params['target'], job_log).process(data)

    @classmethod
    def get_api(cls, url=None, **kw):
        return Request(base_url=url, **kw)

    @classmethod
    def is_async(cls, params):
        return params.asbool('async',
                    default=jobs.Settings.asbool('async', default=True))

    @classmethod
    def update_job(cls, **kw):
        try:
            rq_job = JobHelper.job2dict(JobHelper.get_current_job())
            rq_job.update(kw)
            Job.get(id=rq_job.id).update(**rq_job)
        except Exception as e:
            log.error(e)

    @classmethod
    def inc_progress(cls, inc=1, prefix='UNSET', flush=False):
        rq_job = JobHelper.get_current_job()
        if not rq_job:
            return
        key = '%s_progress' % prefix
        rq_job.meta[key] = rq_job.meta.get(key, 0) + inc
        rq_job.save()

        if flush:
            cls.update_job()


    @classmethod
    def teardown(cls, params):
        cls.update_job(status='finished')

    @classmethod
    def _run(cls, params):
        cls.setup(params)
        cls.update_job(status='started')
        cls.run(params)
        cls.teardown(params)

    def enqueue(self, job, timeout=None, result_ttl=500):
        log.debug('Queuing job `%s` id=%s, params=%s', self.queue.name, job.id, self.params)
        job.reload()

        timeout = timeout or self.JOB_DEFAULT_TIMEOUT

        if self.params.get('cron'):
            from rq_scheduler import Scheduler
            scheduler = Scheduler(queue=self.queue)
            rq_job = scheduler.cron(
                    self.params.cron,
                    func=self._run,
                    args=[self.params],
                    id=job.id_str,
                    timeout=timeout
                )
        else:
            rq_job = self.queue.enqueue(self._run, self.params, job_id = job.id_str,
                                        timeout=timeout, result_ttl=result_ttl)

        job.reload()
        if job.status:
            rq_job.status = job.status

        rq_job.save()

        job.reload()
        job.update_with(JobHelper.job2dict(rq_job)).save()
        return job


class BaseJob(_BaseJob):

    @classmethod
    def setup(cls, params):
        super().setup(params)
        ns = registered_namespaces(jobs.Settings)
        connect_dataset_aliases(jobs.Settings, aliases=ns, reconnect=True)

        if params.asbool('pylog2es', default=False):
            enable_pylog2es(log_level=params.asint('pylog2es_level', default=20))

    @classmethod
    @_profiler
    def run(cls, params):
        if params.target.asbool('fail_on_error', default=True):
            cls.read_loop(params)
        else:
            try:
                cls.read_loop(params)
            except:
                import traceback
                log.error(traceback.format_exc())
                traceback.print_exc()


    @classmethod
    def read(cls, params):
        #children of this class suppose to override this
        return {}

    @classmethod
    def read_loop(cls, params):
        dset = []

        for data in cls.read(params):
            if not data:
                continue

            dset.append(data)

            if len(dset) >= params.batch_size:
                cls.post_to_target(params, dset)
                dset = []

        if dset:
            cls.post_to_target(params, dset)

    @classmethod
    def post_to_target(cls, params, data):
        if not data:
            log.warning('Trying to post empty data')
            return

        if isinstance(data, dict):
            data = [data]

        cls.process(params, JobHelper.add_job_info(params), data)
        cls.inc_progress(len(data), 'target', flush=True)


class ScriptJob(_BaseJob):
    @classmethod
    def run(cls, params):
        script = maybe_dotted(params['script'])
        script(params, jobs.Settings)


class JobHelper(object):
    NOT_FOUND_DATASET = '%s_not_found'

    @classmethod
    def get_settings(cls, name):
        settings = jobs.Settings.subset([
            'batch_size',
            'async',
            'cached',
        ])
        settings.update(jobs.Settings.unflat().extract('%s.*'% name))
        return settings.unflat()

    @classmethod
    def job2dict(cls, job):
        _dict = slovar()

        if job:
            _dict = slovar(job.to_dict()).subset(
                    ['-data', '-description', '-meta'])\
                    .update({'id':job.id})\
                    .update(job.meta)
            # `to_dict` returns a serialized format, we want actual datetime objects
            _dict.update_with({
                'created_at': _dict.asdt('created_at', allow_missing=True),
                'enqueued_at': _dict.asdt('enqueued_at', allow_missing=True),
            })

        return _dict

    @classmethod
    def get_current_job(cls):
        try:
            return rq.get_current_job()
        except rq.exceptions.NoSuchJobError as e:
            log.warn(e)


    @classmethod
    def add_job_info(cls, params):
        job = slovar()
        curr_job = JobHelper.get_current_job()
        rq_job = Job.get_resource(_id=curr_job.id)
        if curr_job:
            job.origin = curr_job.origin
            job.id = curr_job.id
            job.uid = str(rq_job.uid)
            job.contid = params.get('contid') or  job.uid

        _d = slovar({
            'job': job,
            'created_at':curr_job.enqueued_at,
            'comment': params.get('comment', '')
        })

        for ds in ['source', 'target', 'merger']:
            _d[ds] = params.get(ds, slovar()).extract('name,backend,ns')

        return _d

    @classmethod
    def log_if_cached(cls, api, resp):
        if api.from_cache(resp):
            log.debug('CACHED: `%s`', sanitize_url(resp.url, list(cls.settings.keys())))

    @classmethod
    def not_found(cls, params, search_term=None, logger=None, resp=None, msg=''):
        (logger or log).warning('%ssearch_term=`%s`: %s',
                                '(NOT SAVED) '  if not params.save_not_found else '',
                                search_term, msg or 'no match found',
        )

        if not params.save_not_found:
            return

        _p=slovar()
        _p['search_term'] = search_term

        if resp is not None:
            if isinstance(resp, dict):
                _p['response'] = resp
            else: # response object
                _p['status_code'] = resp.status_code
                try:
                    # will remove all `.` from key names which are not allowed in mongodb
                    _p['response'] = slovar(resp.json()).flat().unflat()
                except:
                    _p['response'] = resp.text

        if msg:
            _p['message'] = msg

