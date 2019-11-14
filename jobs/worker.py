import os
import sys
import logging
import traceback

import pyramid
from pyramid.paster import bootstrap, setup_logging

import rq
from redis import StrictRedis
from redis.exceptions import ConnectionError

from slovar import slovar
from prf.utils import split_strip

from jobs.model import Job

log = logging.getLogger(__name__)

def error_handler(job, exc_type, exc_value, tb):
    if isinstance(exc_value, pyramid.httpexceptions.HTTPError):
        exception = exc_value.json
    else:
        exception = str(exc_value)

    rq_obj = Job.get(id=job.id)
    if not rq_obj:
        log.error('Job with id=%s not found' % job.id)
        return True

    rq_obj.status = 'failed'
    rq_obj['error']= {
                'exception': str(exception),
                'traceback': traceback.format_tb(tb)
            }
    rq_obj.save()
    # if exception is saved in Job, then return False to stop the chain of exception handling
    return False


class Worker(object):

    def __init__(self, argv):
        self.queues = []

        #bootstrap fails if this is done after it, even if we only pass the first argv!
        if len(argv) > 2:
            self.queues = split_strip(argv.pop(2))

        setup_logging(argv[1])
        app = bootstrap(argv[1])
        request = app['request']
        settings = slovar(app['registry'].settings).unflat()

        if not self.queues:
            self.queues = settings.aslist('queues')

        log.info('Queues:', self.queues)

        settings.has('redis', dict, err='`redis` not found in config')
        self.redis = settings.redis
        self.redis.asint('port', allow_missing=True)


    def run(self):

        with rq.Connection(StrictRedis(**self.redis)):
            try:
                queues = list(map(rq.Queue, self.queues))
                w = rq.Worker(queues, exc_handler=error_handler)
                w.work()
            except ConnectionError as e:
                log.error(e)

def worker():
    Worker(sys.argv).run()
