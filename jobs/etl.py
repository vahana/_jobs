import re
import logging
import types
from pprint import pformat

from slovar import slovar
from slovar.strings import split_strip

import prf
from prf.utils import DKeyError, DValueError, typecast, rextract
from prf.utils import maybe_dotted, Throttler

from datasets import get_dataset

from jobs.job import BaseJob, JobHelper

log = logging.getLogger(__name__)

MDIRECTIONS=['s2m', 'm2s']
MERGER_QUERY_DELIM = '#'


def get_transformers(params, logger=None, **tr_args):
    transformers = {}

    for call, trs in params.get('transformer', {}).items():
        transformers[call] = []
        for tr in split_strip(trs):
            trans, _, trans_as = tr.partition('__as__')

            if trans_as:
                tr_args['trans_as'] = trans_as

            tr_args.update(params.get('transformer_args', {}))

            tr_module = maybe_dotted(trans)
            if type(tr_module) == types.FunctionType:
                transformers[call].append(tr_module)
            elif type(tr_module) == types.ClassType:
                transformers[call].append(maybe_dotted(trans)(logger=logger, **tr_args))
            else:
                raise ValueError('Bad type for transformer. Must be either function or class, got %s instead' % type(tr_module))

    return transformers


def transform(obj, trans, call, fail_on_error=True, **kw):

    def do_trans(tran, item):
        try:
            return tran(slovar.copy(item), **kw)
        except Exception as e:
            if fail_on_error:
                raise e
            else:
                log.error(e)

    _obj = obj.copy()

    if trans and call in trans:
        for tr in trans[call]:
            _obj = do_trans(tr, _obj)
            if not _obj:
                break

    return _obj

def add_job_info(params):
    _d = JobHelper.add_job_info(params)

    if 'merger' in params:
        _d['merger'] = params.merger.name

    return _d


class ETLJob(BaseJob):

    @classmethod
    def read(cls, params):
        source = Source(params, cls)
        merger = Merger(source, params)

        for src in source.read():
            if src is None:
                continue

            if merger.ok:
                for it in merger.merge(src):
                    yield it
            else:
                yield src


class Source:
    def __init__(self, all_params, job_cls):

        self.job_cls = job_cls
        self.params = all_params.get('source', slovar())
        self.params.set_default('query._fields', [])
        self.params.query = self.params.query.flat()
        self.params.aslist('throttle', allow_missing=True, itype=int)

        self.klass = get_dataset(self.params, define=True)
        self.trans = get_transformers(self.params, job=add_job_info(all_params))

        self.pagination = all_params.get('_pagination')
        self.batch_size = all_params.batch_size

        if self.params.get('throttle'):
            self.throttle = Throttler(*self.params.throttle)
        else:
            self.throttle = lambda:1

    def transform(self, data):
        return transform(data, self.trans, 'post_read',
                         fail_on_error=self.params.asbool('fail_on_error', default=True))

    def read(self):
        read_counter = thr_limit = thr_period = 0

        for chunk in self.klass.get_collection_paged(
                                                    self.batch_size,
                                                    _pagination = self.pagination,
                                                    **self.params.query):
            for each in chunk:
                tr_result = self.transform(each.to_dict(self.params.query._fields))
                if isinstance(tr_result, (list, types.GeneratorType)):
                    for it in tr_result:
                        read_counter +=1
                        yield it
                else:
                    read_counter +=1
                    yield tr_result

                self.throttle()

            self.job_cls.inc_progress(read_counter, 'source', flush=True)
            read_counter = 0

class Merger:
    _operations = slovar()

    @classmethod
    def define_op(cls, params, _type, name, **kw):

        if not kw.get('allow_missing') and 'default' not in kw:
            kw.setdefault('raise_on_values', [None, '', []])

        ret_val = getattr(params, _type)(name, **kw)
        cls._operations[name] = type(ret_val)
        return ret_val

    @classmethod
    def validate_ops(cls, params):
        log.debug('params:\n%s', pformat(params))

        invalid_ops = set(params.keys()) - set(cls._operations.keys())
        if invalid_ops:
            raise KeyError('Invalid operations %s' % list(invalid_ops))

    def __init__(self, source, all_params):
        self.source = source

        self.ok = True
        if not all_params.get('merger'):
            self.ok = False
            return

        self.all_params = all_params
        params = all_params.get('merger')

        self.merge_rules = None

        params.set_default('query._fields', [])
        params.query = params.query.flat()

        self.define_op(params, 'asstr', 'backend')
        self.define_op(params, 'asstr', 'name', allow_missing=True)
        self.define_op(params, 'asstr', 'ns', allow_missing=True)
        self.define_op(params, 'aslist', 'merge_keys', allow_missing=True)
        self.define_op(params, 'asstr', 'merge_direction')

        self.define_op(params, 'asbool', 'match_one', default=True)
        self.define_op(params, 'asbool', 'require_match', default=True)
        self.define_op(params, 'asbool', 'require_no_match', default=False)
        self.define_op(params, 'asbool', 'strict_match', default=True)

        if self.define_op(params, 'asbool', 'overwrite', _raise=False, default=True) is None:
            self.define_op(params, 'aslist', 'overwrite', default=[])

        if self.define_op(params, 'asbool', 'flatten', _raise=False, default=False) is None:
            self.define_op(params, 'aslist', 'flatten', default=[])

        self.define_op(params, 'aslist', 'append_to', default=[])
        self.define_op(params, 'aslist', 'append_to_set', default=[])
        self.define_op(params, 'aslist', 'merge_to', default=[])
        self.define_op(params, 'aslist', 'remove_from', default=[])
        self.define_op(params, 'aslist', 'fields', allow_missing=True)
        self.define_op(params, 'asstr', 'merge_rules', allow_missing=True)
        self.define_op(params, 'asstr', 'merge_rules_scm', allow_missing=True)
        self.define_op(params, 'asstr', 'merge_as', allow_missing=True)
        self.define_op(params, 'asbool', 'unwind', default=True)
        self.define_op(params, 'asbool', 'fail_on_error', default=True)

        if params.get('merge_rules'):
            self.merge_rules = maybe_dotted(params.merge_rules)(scm=params.get('merge_rules_scm'))

        self._operations['query'] = dict
        self._operations['transformer'] = dict

        self.validate_ops(params)

        if (params.append_to_set or params.append_to) and not params.flatten:
            for kk in params.append_to_set+params.append_to:
                if '.' in kk:
                    log.warning('`%s` for append_to/appent_to_set is nested but `flatten` is not set' % kk)

        if params.match_one:
            params.query._limit = 1
        else:
            params.query.asint('_limit', default=-1)

        if not params.match_one:
            log.warning('merger.match_one is set to `False` for the merger. '
                        'Will match ALL documents from the merger')

        self.trans = get_transformers(params, job=add_job_info(params))
        self.klass = get_dataset(params, define=True)

        self.params = params
        self.source_params = all_params.source
        self.process_all_es(all_params)

    def process_all_es(self, all_params):
        #if all backends are es, then do some preps.

        self.all_be_es = self.params.backend == \
                            all_params.get('source')['backend'] ==\
                            all_params.get('target')['backend'] == 'es'


        if self.all_be_es:
            self._source_klass = get_dataset(self.source_params, define=True)
            self._target_klass = get_dataset(all_params.get('target'), define=True)

    def consolidate_es_index(self, src, mrg):
        '''
            Consolidates the index data coming from both source and merger
            Rules:
                if source ds is same as target ds then keep src._index
                if merge ds is same as target ds then keep mrg._index
        '''

        # if source data index is in target
        if '_index' in src and src._index in self._target_klass.index_map:
            mrg.pop('_index', None)

        # if merger data index is in target
        elif '_index' in mrg and mrg._index in self._target_klass.index_map:
            src.pop('_index', None)

        # let downstream decide
        else:
            src.pop('_index', None)
            mrg.pop('_index', None)

    def transform(self, data, action_name):
        return transform(data, self.trans, action_name,
                            self.params.fail_on_error)

    def get_query(self, src):
        _query = slovar()

        def is_empty_query(query):
            KEYS = set(['_fields', '_count', '_limit'])

            if (set(query.keys()) - KEYS):
                return False

            return True

        for kk, vv in self.params.query.items():

            if isinstance(vv, str) and MERGER_QUERY_DELIM in vv:
                vv = rextract(vv, src, delim=MERGER_QUERY_DELIM, _raise=self.params.strict_match)
                if vv is None:
                    log.warning('Merger query data for `%s` is `%s`.Skipping.' % (kk, vv))
                    continue

            _query[kk] = vv

        if is_empty_query(_query):
            if self.params.require_match:
                raise ValueError('missing fields in the source.\nmerge query: %s\nsource keys: %s' %
                                        (_query, src.keys()))

        return _query

    def read(self, src):
        query = self.get_query(src)

        if not query:
            return []

        matched = self.klass.get_collection(**query, _count=1)

        if matched == 0:
            if not self.params.require_match:
                log.debug('UNMATCHED(require_match=False): \n%s' % query)

            elif self.params.require_no_match:
                log.debug('UNMATCHED(require_no_match=True): \n%s' % query)

            return []

        if matched > 1 and not self.params.require_no_match:
            msg = 'Multiple (%s) matches found in merger (%s) with source (%s) using query `%s` ' % (
                                                        matched, self.params.name, self.source_params.name, query)
            if self.params.match_one:
                if self.params.strict_match:
                    raise ValueError(msg+ '(match_one=True, strict_match=True)')
                else:
                    log.warning(msg+ '(match_one=True, strict_match=False)')

        # -1 is same as default for ES, so make sure we pass actual value
        # b/c matched could be more than the default
        if query._limit == -1:
            query._limit = matched

        for it in self.klass.get_collection(**query):
            yield self.transform(it.to_dict(query._fields), 'post_read')

    def merge(self, src):

        def _merge(src, mrg, _params):

            if self.params.get('merge_as'):
                mrg = slovar({self.params.merge_as:mrg})

            #call this before the direction detection
            #applies only if all datasets are ES
            if self.all_be_es:
                self.consolidate_es_index(src, mrg)

            #default is `m2s` - merge mrg into src
            if self.params.merge_direction == 's2m':
                src, mrg = mrg, src

            if self.merge_rules:
                morpher_params = self.merge_rules.run(src, mrg)
                if morpher_params is None:
                    log.debug('merge rules returned `None`, SKIP the merge.')
                    return []

                _params.update(morpher_params)

            return self.transform(src.update_with(mrg, **typecast(_params)), 'post_merge')

        merged2list = []
        matched = False

        if self.params.merge_direction not in MDIRECTIONS:
            raise ValueError('Merge directions must be %s. Got merge_direction=`%s`' %
                                                (MDIRECTIONS, self.params.merge_direction))

        update_params = self.params.extract(['append_to', 'append_to_set',
                                             'overwrite', 'flatten', 'merge_to', 'remove_from'])

        for mrg in self.read(src):
            matched = True

            if self.params.require_no_match:
                log.debug('SKIP matched merger with require_no_match: id=`%s`' % src.get('id'))
                yield {}

            else:
                if self.params.unwind:
                    yield _merge(src, mrg, update_params.copy())
                else:
                    if mrg:
                        merged2list.append(mrg)

        if matched:
            if not self.params.unwind:
                self.params.has('merge_as', err='`merger.merge_as` is mandatory for `merger.unwind=0`')
                if merged2list:
                    yield _merge(src, merged2list, update_params.copy())
                    merged2list = []

        elif not self.params.require_match or self.params.require_no_match:
            yield src


class Diff(Merger):
    @classmethod
    def do_diff(src, params):

        def _diff(src, mrg):

            diff_keys=params.aslist('diff', default=[])
            flat_keys=params.aslist('diff_flat_keys', default=[])
            merge_keys=self.merge_keys
            merge_direction=self.merge_direction
            diff_context=params.aslist('diff_context', default=[]) + merge_keys

            #default is `m2s` - merge mrg into src
            if merge_direction == 's2m':
                src, mrg = mrg, src

            if '*' in diff_keys:
                diff_keys = list(src.keys())

            if '*' in flat_keys:
                flat_keys = list(src.keys())

            src.pop('logs', None)
            mrg.pop('logs', None)

            _diff = slovar()
            counts = slovar()

            def do_counts(v1, v2):
                v1_count = v2_count = 0

                if isinstance(v1, (list, dict)):
                    v1_count = len(v1)
                elif v1:
                    v1_count = 1

                if isinstance(v2, (list, dict)):
                    v2_count = len(v2)
                elif v2:
                    v2_count = 1

                return v1_count - v2_count

            src_diff, mrg_diff = mrg.diff(src, diff_keys, flat_keys)

            for kk,vv in src_diff.items():
                mrg_v = mrg_diff.get(kk)
                _diff[kk] = {'src': vv, 'mrg': mrg_v}
                counts[kk] = do_counts(vv, mrg_v)

            _diff['context'] = src.extract(diff_context)

            if counts:
                _diff['counts'] = counts
                _diff['counts']['total'] = sum(counts.values())

            return _diff.update_with(src.extract(self.merge_keys))

        for mrg in self.read(src):
            yield _diff(src, mrg)

