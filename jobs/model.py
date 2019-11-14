import logging
import mongoengine as mongo

import prf
from prf.mongodb import DynamicBase

log = logging.getLogger(__name__)

class Job(DynamicBase):
    meta = {
        'ordering': ['-id'],
        'collection': 'rq_jobs',
        'indexes': [
            'queue',
            'job',
            'job_name',
            'created_at'
        ]
    }

    job = mongo.StringField(required=True)
    status = mongo.StringField()
    params = mongo.DictField(default={})
