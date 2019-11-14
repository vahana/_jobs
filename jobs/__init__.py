import logging
import os
from slovar import slovar
from pyramid.config import Configurator

log = logging.getLogger(__name__)
Settings = slovar()

from jobs.job import BaseJob, JobHelper
from jobs.model import Job
from jobs.views import BaseJobView, CronJobsView, ScriptJobsView

def main(global_config, **settings):
    config = Configurator(settings=settings)

    config.include('prf')
    config.include('prf.mongodb')
    includeme(config)
    root = config.get_root_resource()

    config.add_tween('prf.tweens.GET_tunneling')

    return config.make_wsgi_app()


def includeme(config):
    global Settings

    config.add_tween('jobs.rq_tween_factory')

    Settings.update(config.prf_settings())

    if not Settings.asbool('async', default=True):
        log.warning('Jobs will run in async=False mode')


def rq_tween_factory(handler, registry):
    import rq
    import redis

    settings = slovar(registry.settings).unflat()
    settings.has('redis', dict, err='`redis` is not found in config')
    settings.redis.asint('port', allow_missing=True)

    connection = redis.StrictRedis(**settings.redis)
    registry._rq_redis = connection

    def rq_tween(request):
        with rq.Connection(registry._rq_redis):
            return handler(request)

    return rq_tween

