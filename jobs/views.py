import logging
from datetime import datetime
from itertools import groupby
from bson import ObjectId
import rq

import prf.exc

from slovar import slovar
from prf.utils import resolve_host_to, maybe_dotted
from prf.view import BaseView
from prf.auth import BaseACL

import jobs
from jobs.job import JobHelper, BaseJob, ScriptJob
from jobs.model import Job

from datasets import get_dataset

log = logging.getLogger(__name__)

class _BaseJobView(BaseView):
    _job_class = None
    _model = Job

    def __init__(self, *arg):
        super().__init__(*arg)

        self.settings = JobHelper.get_settings(self.resource.collection_name)
        self._job_class = maybe_dotted(self._job_class)

        if self.request.method == 'POST':
            self._params.setdefault('query', slovar())
            self._params = self._params.unflat()

        _limit = self._params.asint('_limit', allow_missing=True)
        if _limit:
            if _limit == -1 and not jobs.Settings.asbool('unlimited_page_size', default=True):
                raise prf.exc.HTTPBadRequest('`_limit=-1` not allowed')

        if self.request.method == 'GET':
            self._params.setdefault('_limit',
                    jobs.Settings.asint('default_page_size', default=20))

    def index(self):
        return Job.get_collection(job=self.resource.uid, **self._params)

    def show(self, id):
        return Job.get_resource(id=id)

    def create(self):
        self._params.asbool('async', default=\
                    self.settings.asbool('async', default=True))
        self._params.asstr('qname')
        return self.create_job(self._params.qname)

    def update(self, id):
        obj = Job.get_resource(id=id)
        obj.update_with(self._params)
        obj.save()

    def delete(self, id):

        obj = Job.get(id=id)
        if not obj:
            return

        rq.cancel_job(str(obj.id))

        if obj.status == 'canceled':
            obj.delete()
        else:
            obj.status = 'canceled'
            obj.save()

    def delete_many(self):
        self._params.setdefault('_limit', 20)

        if self.needs_confirmation():
            raise prf.exc.HTTPBadRequest('deleting %s jobs? add %s to confirm' %
                            (Job.get_collection(_count=1, **self._params), self._conf_keyword))
        else:
            for each in Job.get_collection(**self._params):
                rq.cancel_job(str(each.id))
                each.delete()

    def create_job(self, qname):
        uid = str(ObjectId())

        _id = ObjectId()
        self.job = Job(id=_id, uid=uid, job=self.resource.uid).save()

        self._params['job_id'] = _id
        self._params['job_url'] = resolve_host_to(
                self.request.current_route_url(_id),
                jobs.Settings.get('jobs.api_host', 'localhost:'))

        self.job.params = self._params
        self.job.save()

        if self._params.asbool('dry_run', default=False):
            self.job.status='dry_run'
            self.job.save()
            return self.job

        return self._job_class(qname, self._params).enqueue(self.job)


class BaseJobView(_BaseJobView):
    def create(self):
        self._params.asint('batch_size', default=\
                    self.settings.asint('batch_size', default=1000))
        self._params.asbool('async', default=\
                    self.settings.asbool('async', default=True))
        self._params.asbool('cached', default=\
                    self.settings.asbool('cached', default=False))
        self._params.asbool('save_not_found', default=\
                    self.settings.asbool('save_not_found', default=True))
        self._params.asstr('qname', default='normal')

        self.set_source()
        self.set_target()
        self.set_limits()

        return self.create_jobs()

    @classmethod
    def add_namespace(cls, params, name):
        ds = params.get(name)
        if ds.get('backend'):
            return params

        _name = ds.name
        if ds.ns:
            if '.' not in _name:
                ds.name = '%s.%s' % (ds.ns, ds.name)
        else:
            if '.' not in _name:
                raise prf.exc.HTTPBadRequest(
                    'Missing namespace: `<namespace>.<%s_name>`' % name)
        return params

    def set_source(self):
        self._params.has('source', dict)
        source = self._params.source
        self._params.has('source.name')

        source.setdefault('query', slovar())
        source.has('query', slovar)

        if 'qs' in source:
            qs_params = typecast(qs2dict(source.qs))

            if 'query' in source:
                source.query = qs_params.merge(source.query)
            else:
                source.query = qs_params

    def set_target(self):
        self._params.has('target', slovar)
        target = self._params.target

        if target.asbool('dry_run', default=False):
            target.setdefault('name', 'dry_run')

        self.add_namespace(self._params, 'target')

    def set_limits(self):
        source = self._params.source

        source.query.asint('_limit', default=-1)
        source.query.asint('_start', default=0)

        if source.query._limit == -1:
            ds = get_dataset(source, define=True)
            _total = ds.get_total(**source.query.flat())
            source.query._total_limit = max(_total - source.query._start, 0)
        else:
            source.query._total_limit = source.query._limit

        nb_workers = self._params.asint('workers', default=1)
        per_worker, rem = divmod(source.query._total_limit, nb_workers)
        self._params.worker_limits = [per_worker] * nb_workers

        if rem > 0:
            self._params.worker_limits[-1] += rem

        self._params.worker_limits.insert(0, source.query._start)

    def create_jobs(self):
        uid = str(ObjectId())

        def _create_job():
            _id = ObjectId()
            self.job = Job(id=_id, uid=uid, job=self.resource.uid).save()

            self._params['job_id'] = _id
            self._params['job_url'] = resolve_host_to(
                    self.request.current_route_url(_id),
                    jobs.Settings.get('jobs.api_host', 'localhost:'))

            self.job.params = self._params
            self.job.save()

            if self._params.asbool('dry_run', default=False):
                self.job.status='dry_run'
                self.job.save()
                return self.job

            return self._job_class(
                    self._params.qname,
                    # uid,
                    self._params).enqueue(self.job)

        job = None
        source = self._params.source
        start = 0

        for ix in range(len(self._params.worker_limits)-1):
            start = start + self._params.worker_limits[ix]
            limit = self._params.worker_limits[ix+1]
            source.query._start = start
            source.query._limit = limit
            job = _create_job()

        return job


class BaseAPIJobView(BaseJobView):

    def set_limits(self):
        source = self._params.source
        source.query._total_limit = 1000000
        self._params.worker_limits = [0,1000000]


class JobStatusView(BaseView):
    _default_params = {
        '_limit': 1,
    }

    def show(self):
        self.returns_many = True

        def add_workers(_d):
            workers = _d['workers']
            for kk, gg in groupby(sorted(workers.pop('status', []))):
                workers[kk] = len(list(gg))
            _d['1_workers'] = workers
            _d.pop_many(['workers', 'status'])
            return _d

        def add_counters(_d):
            try:
                time_passed = (datetime.now()-_d.asdt('created_at')).total_seconds()/60
                counters = _d['counters']
                tspeed = counters['tprogress'] / time_passed
                sspeed = counters['sprogress'] / time_passed

                if tspeed == 0: tspeed = 1
                if sspeed == 0: sspeed = 1

                time_left = (counters['total'] - counters['tprogress'])/sspeed

                counters.update({
                    'time_passed': '%s min' % int(time_passed),
                    'time_left': '%s min' %  int(time_left),
                    'tspeed': '%s/min' % int(tspeed),
                    'sspeed': '%s/min' % int(sspeed),
                })

                _d['0_counters'] = _d.pop('counters')

            except (KeyError, AttributeError) as e:
                pass

            return _d

        def add_links(_d):
            dsets = {}
            _d.setdefault('backend', 'mongo')

            for name in ['source', 'merger', 'target']:
                if name not in _d:
                    continue

                dsets[name] = 'http://localhost:6544/api/{backend}/{ns}/{name}'.format(
                    **_d[name].with_defaults(backend='NA', ns='NA', name='NA'))

            _d['2_datasets'] = dsets

            links = {}
            links['job'] = 'http://localhost:6544/api/jobs/_all?uid=%s' % _d['uid']
            links['self'] = 'http://localhost:6544/api/job_status?uid=%s' % _d['uid']
            links['started'] = 'http://localhost:6544/api/jobs/_all?uid=%s&status=started' % _d['uid']
            links['failed'] = 'http://localhost:6544/api/jobs/_all?uid=%s&status=failed' % _d['uid']
            links['logs'] = 'http://localhost:6544/api/es/logs/job_%s?_group=levelname,funcName' % _d['uid']

            _d['3_links'] = links
            _d.pop_many(['source', 'merger', 'target'])
            return _d

        _fields=[ 'uid',
                'list.0.*',
                'target_progress_sum__as__counters.tprogress',
                'source_progress_sum__as__counters.sprogress',
                'params__source__query___total_limit__as__counters.total',
                'params__source__as__source',
                'params__merger__as__merger',
                'params__target__as__target',
                'params__contid__as__contid',
                'count__as__workers.total',
                'list.status__as__workers.status'
            ]

        params = slovar(
                _group='uid',
                _group_list='params.source.query._total_limit,created_at,params.contid,params.merger,params.source,params.target,status',
                _group_sum='target_progress,source_progress',
                _sort='-created_at',
            ).update(self._params)

        resp = []

        for each in Job.get_collection(**params):
            _dict = each.to_dict(_fields).unflat()
            resp.append(
                add_links(
                    add_workers(
                        add_counters(_dict))
                    ).extract('0_counters,1_workers,2_datasets,3_links,uid').extract(self._specials._fields)
                )

        return resp


class JobsView(BaseView):
    _default_params = {
        '_limit': 20
    }

    def index(self):
        return Job.get_collection(**self._params)

    def show(self, id=None):
        if id:
            return Job.get_resource(id=id)

        self.returns_many = True
        return [self.request.route_url(each.uid)
                    for each in self.resource.children]


class CronJobsView(BaseView):
    _model = Job
    _default_params = {
        '_limit': 20
    }

    def init(self):
        from rq_scheduler import Scheduler
        self.scheduler = Scheduler()
        self.jobs = []

        for it in self.scheduler.get_jobs():
            _d = JobHelper.job2dict(it)
            job = self._model.get(id=it.id)
            if job:
                _d._rq_job = job.to_dict()

            self.jobs.append(_d)

    def get_by_id(self, id):
        for job in self.jobs:
            if job.id == id:
                return job

        return {}

    def index(self):
        return self.jobs

    def show(self, id):
        return self.get_by_id(id)

    def delete(self, id):
        self.scheduler.cancel(id)

    def delete_many(self):
        for job in self.jobs:
            self.scheduler.cancel(job.id)


class ScriptJobsView(BaseJobView):
    _job_class = ScriptJob

