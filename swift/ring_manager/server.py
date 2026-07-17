#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import shlex
import subprocess
import sys

from eventlet.queue import Full
from swift import __version__ as swift_version
from swift.common import exceptions as swift_exceptions
from swift.common.concurrency import GreenPool, Queue, Semaphore, Timeout, \
    spawn, tpool
from swift.common.swob import HTTPBadRequest, HTTPConflict, \
    HTTPException, HTTPForbidden, HTTPInternalServerError, \
    HTTPMethodNotAllowed, HTTPNotFound, Request, Response, wsgi_to_str
from swift.common.utils import config_true_value, get_log_line, get_logger, \
    config_positive_float_value, config_positive_int_value, \
    LOG_LINE_DEFAULT_FORMAT, non_negative_float, parse_options
from swift.common.wsgi import run_wsgi
from swift.ring_manager.builder import DEFAULT_MAX_EXPLICIT_DEVICE_ID, \
    RingBuilderManager, RingBuilderManagerError
from swift.ring_manager.common import DEFAULT_BUILDER_LOCK_TIMEOUT, \
    DEFAULT_BUILD_JOB_LEASE_TIMEOUT, DEFAULT_RING_ARTIFACT_DIR, \
    DEFAULT_RING_BUILD_EXECUTOR, DEFAULT_RING_BUILD_MANAGER_WORKERS, \
    DEFAULT_RING_BUILDER_DIR, DEFAULT_RING_MANAGER_STATE_DIR, \
    DEFAULT_RING_MANAGER_SYNC_FRESHNESS_THRESHOLD, \
    DEFAULT_STATE_CHANGE_HOOK_TIMEOUT, NormalTimestamp, \
    RING_BUILD_EXECUTORS, RING_MANAGER_SYNC_JOURNAL, stats_increment, \
    stats_timing_since
from swift.ring_manager.controllers import ring as ring_controller
from swift.ring_manager import http, routing
from swift.ring_manager.builder_daemon import RingBuildWorker
from swift.ring_manager.publisher import RingBuilderPublisher
from swift.ring_manager.store import RingManagerStore, RingVersionNotFound


RING_MANAGER_API_VERSION = 'v1'
RING_MANAGER_API_PREFIX = '/api/%s' % RING_MANAGER_API_VERSION
DEFAULT_MAX_JSON_REQUEST_BODY_SIZE = 1024 * 1024
DEFAULT_MAX_PARTITIONS_AT_RISK_SELECTORS = 1000
DEFAULT_SYNC_TRIGGER_QUEUE_SIZE = 1
RING_MANAGER_MODES = ('primary', 'readonly', 'standby')
READONLY_RING_MANAGER_MODES = ('readonly', 'standby')


class RingManagerApplication(object):
    """WSGI application for Swift ring management."""

    server_type = 'ring-manager-server'

    def __init__(self, conf, logger=None, store=None, controller=None,
                 builder_manager=None):
        if conf is None:
            conf = {}
        self.conf = conf
        self.logger = logger or get_logger(
            conf, log_route='ring-manager-server',
            statsd_tail_prefix='ring-manager-server')
        self.log_requests = config_true_value(conf.get(
            'log_requests', 'true'))
        self.log_format = conf.get('log_format', LOG_LINE_DEFAULT_FORMAT)
        self.anonymization_method = conf.get('log_anonymization_method', 'md5')
        self.anonymization_salt = conf.get('log_anonymization_salt', '')
        state_dir = conf.get(
            'ring_manager_state_dir', DEFAULT_RING_MANAGER_STATE_DIR)
        ring_artifact_dir = conf.get(
            'ring_artifact_dir', DEFAULT_RING_ARTIFACT_DIR)
        self.store = store or RingManagerStore(
            state_dir=state_dir,
            ring_artifact_dir=ring_artifact_dir,
            state_change_hook=conf.get('ring_manager_state_change_hook'),
            state_change_hook_timeout=non_negative_float(conf.get(
                'ring_manager_state_change_hook_timeout',
                DEFAULT_STATE_CHANGE_HOOK_TIMEOUT)),
            logger=self.logger)
        self.ring_builder_dir = conf.get(
            'ring_builder_dir', DEFAULT_RING_BUILDER_DIR)
        self.builder_lock_timeout = non_negative_float(conf.get(
            'builder_lock_timeout', DEFAULT_BUILDER_LOCK_TIMEOUT))
        self.max_explicit_device_id = config_positive_int_value(conf.get(
            'max_explicit_device_id', DEFAULT_MAX_EXPLICIT_DEVICE_ID))
        self.max_json_request_body_size = config_positive_int_value(conf.get(
            'max_json_request_body_size',
            DEFAULT_MAX_JSON_REQUEST_BODY_SIZE))
        self.max_partitions_at_risk_selectors = config_positive_int_value(
            conf.get('max_partitions_at_risk_selectors',
                     DEFAULT_MAX_PARTITIONS_AT_RISK_SELECTORS))
        self.builder_manager = builder_manager or RingBuilderManager(
            self.ring_builder_dir,
            max_explicit_device_id=self.max_explicit_device_id,
            builder_lock_timeout=self.builder_lock_timeout)
        self.publisher = RingBuilderPublisher(
            self.store, ring_artifact_dir=ring_artifact_dir,
            builder_manager=self.builder_manager,
            builder_lock_timeout=self.builder_lock_timeout,
            logger=self.logger)
        self.mode = (conf.get('ring_manager_mode') or 'primary').lower()
        if self.mode not in RING_MANAGER_MODES:
            raise ValueError(
                'ring_manager_mode must be one of: %s' %
                ', '.join(RING_MANAGER_MODES))
        self.ring_build_executor = (conf.get(
            'ring_build_executor', DEFAULT_RING_BUILD_EXECUTOR) or
            DEFAULT_RING_BUILD_EXECUTOR).lower()
        if self.ring_build_executor not in RING_BUILD_EXECUTORS:
            raise ValueError(
                'ring_build_executor must be one of: %s' %
                ', '.join(RING_BUILD_EXECUTORS))
        self.build_job_lease_timeout = config_positive_float_value(conf.get(
            'build_job_lease_timeout', DEFAULT_BUILD_JOB_LEASE_TIMEOUT))
        self.sync_freshness_threshold = non_negative_float(conf.get(
            'ring_manager_sync_freshness_threshold',
            DEFAULT_RING_MANAGER_SYNC_FRESHNESS_THRESHOLD))
        self.sync_trigger_command = conf.get(
            'ring_manager_sync_trigger_command')
        self.sync_trigger_timeout = non_negative_float(conf.get(
            'ring_manager_sync_trigger_timeout', 0))
        self.sync_trigger_queue = None
        self.sync_trigger_worker = None
        self.sync_trigger_lock = Semaphore()
        self.sync_trigger_active = False
        self.sync_trigger_pending = False
        if self.sync_trigger_command:
            self.sync_trigger_queue = Queue(
                maxsize=DEFAULT_SYNC_TRIGGER_QUEUE_SIZE)
            self.sync_trigger_worker = spawn(self._run_sync_triggers)
        self.build_pool = None
        self.build_worker = None
        if self.ring_build_executor == 'manager':
            build_workers = config_positive_int_value(conf.get(
                'ring_build_manager_workers',
                DEFAULT_RING_BUILD_MANAGER_WORKERS))
            self.build_pool = GreenPool(size=build_workers)
            self.build_worker = RingBuildWorker(
                self.store, self.publisher, logger=self.logger,
                lease_timeout=self.build_job_lease_timeout)
        self.ring_controller = controller or ring_controller.RingController(
            store=self.store, builder_manager=self.builder_manager,
            ring_builder_dir=self.ring_builder_dir,
            publisher=self.publisher,
            ring_build_executor=self.ring_build_executor,
            build_pool=self.build_pool,
            build_worker=self.build_worker,
            max_json_request_body_size=self.max_json_request_body_size,
            max_partitions_at_risk_selectors=(
                self.max_partitions_at_risk_selectors),
            logger=self.logger)
        self.routes = self._make_routes()

    @property
    def writable(self):
        return self.mode not in READONLY_RING_MANAGER_MODES

    def _make_routes(self):
        return [
            routing.Route(r'^/?$', ('GET',), self.root),
            routing.Route(r'^/api/v1/?$', ('GET',), self.api_root),
            routing.Route(r'^/api/v1/ring_manager/status/?$',
                          ('GET',), self.ring_manager_status),
            routing.Route(r'^/api/v1/ring_manager/sync/trigger/?$',
                          ('POST',), self.ring_manager_sync_trigger,
                          read_only_methods=('POST',)),
        ] + self.ring_controller.routes()

    def _route_methods(self, route):
        methods = set(route.methods)
        if 'GET' in methods:
            methods.add('HEAD')
        return methods

    def _allowed_methods(self, route):
        return sorted(self._route_methods(route) | set(['OPTIONS']))

    def _route_options(self, req, route):
        return Response(
            request=req, status=200,
            headers={
                'Allow': ', '.join(self._allowed_methods(route)),
                'Server': '%s/%s' % (self.server_type, swift_version),
            })

    def _readonly_response(self, req):
        stats_increment(self.logger, 'readonly.rejected_mutations')
        return http.json_error(
            req, HTTPForbidden,
            'ring-manager is running in %s mode; mutating requests are '
            'disabled' % self.mode)

    def _dispatch(self, req):
        path = wsgi_to_str(req.path_info)
        for route in self.routes:
            match = route.regex.match(path)
            if not match:
                continue
            if req.method == 'OPTIONS':
                return self._route_options(req, route)
            if req.method not in self._route_methods(route):
                return HTTPMethodNotAllowed(
                    request=req,
                    headers={'Allow': ', '.join(
                        self._allowed_methods(route))})
            if (not self.writable and
                    req.method not in route.read_only_methods):
                return self._readonly_response(req)
            return route.handler(req, **match.groupdict())
        return HTTPNotFound(request=req)

    def _log_request(self, req, res, trans_time):
        if not self.log_requests:
            return
        log_msg = get_log_line(
            req, res, trans_time, '', self.log_format,
            self.anonymization_method, self.anonymization_salt)
        self.logger.info(log_msg)

    def __call__(self, env, start_response):
        start_time = float(NormalTimestamp.now())
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)
        stats_increment(self.logger, 'requests')
        try:
            res = self._dispatch(req)
        except HTTPException as error_response:
            res = error_response
        except ValueError as err:
            res = http.json_error(req, HTTPBadRequest, str(err))
        except (Exception, Timeout):
            self.logger.exception(
                'ERROR ring-manager __call__ error with %(method)s %(path)s',
                {'method': req.method, 'path': req.path})
            res = http.json_error(
                req, HTTPInternalServerError, 'Internal server error')
        trans_time = float(NormalTimestamp.now()) - start_time
        status_int = getattr(res, 'status_int', 500)
        stats_increment(self.logger, 'return_codes.%d' % (status_int // 100))
        if status_int >= 500:
            stats_increment(self.logger, 'errors')
        self.logger.timing_since('%s.timing' % req.method.lower(), start_time)
        stats_timing_since(self.logger, 'requests.timing', start_time)
        self._log_request(req, res, trans_time)
        return res(env, start_response)

    def _api_links(self):
        return {
            'status': '/api/v1/ring_manager/status/',
            'sync_trigger': '/api/v1/ring_manager/sync/trigger/',
            'rings': '/api/v1/rings/',
            'ring_builds': '/api/v1/rings/builds/',
            'ring_versions': '/api/v1/rings/releases/',
            'latest_ring_version': '/api/v1/rings/releases/latest/',
            'desired_ring_version': '/api/v1/rings/releases/desired/',
            'desired_ring_version_manifest':
                '/api/v1/rings/releases/desired/manifest/',
        }

    def _service_document(self, api_version=None):
        document = {
            'service': self.server_type,
            'version': swift_version,
            'api_versions': [{
                'id': RING_MANAGER_API_VERSION,
                'url': '%s/' % RING_MANAGER_API_PREFIX,
            }],
            'mode': self.mode,
            'writable': self.writable,
            'links': self._api_links(),
        }
        if api_version is not None:
            document['api_version'] = api_version
        return document

    def root(self, req):
        return http.json_response(req, self._service_document())

    def api_root(self, req):
        return http.json_response(
            req, self._service_document(api_version=RING_MANAGER_API_VERSION))

    def _base_sync_status(self):
        return {
            'applicable': self.mode in READONLY_RING_MANAGER_MODES,
            'source': None,
            'latest_ring_version': None,
            'last_synced_at': None,
            'age_seconds': None,
            'freshness_threshold': self.sync_freshness_threshold,
            'latest_matches_local': None,
            'synced': False,
            'fresh': False,
            'stale': False,
            'can_serve_published_reads': False,
            'published_state_promote_ready': False,
            'sync_transaction_pending': False,
            'sync_transaction': {
                'pending': False,
            },
            'promotion_blockers': [],
            'reasons': [],
        }

    def _sync_transaction_pending(self):
        if not self.store.state_dir:
            return False
        return os.path.exists(os.path.join(
            self.store.state_dir, RING_MANAGER_SYNC_JOURNAL))

    def _sync_trigger_status(self):
        self.sync_trigger_lock.acquire()
        try:
            return {
                'configured': bool(self.sync_trigger_command),
                'active': self.sync_trigger_active,
                'pending': self.sync_trigger_pending,
            }
        finally:
            self.sync_trigger_lock.release()

    def _queue_sync_trigger(self, payload):
        item = {
            'triggered_at': NormalTimestamp.now().internal,
            'payload': payload,
        }
        self.sync_trigger_lock.acquire()
        try:
            if self.sync_trigger_pending:
                return False, item
            self.sync_trigger_pending = True
        finally:
            self.sync_trigger_lock.release()
        try:
            self.sync_trigger_queue.put_nowait(item)
        except Full:
            # Do not make a transient queue-full condition permanently look
            # pending; otherwise later wake-ups would be silently dropped.
            self.sync_trigger_lock.acquire()
            try:
                self.sync_trigger_pending = False
            finally:
                self.sync_trigger_lock.release()
            return False, item
        return True, item

    def _run_sync_triggers(self):
        while True:
            item = self.sync_trigger_queue.get()
            self.sync_trigger_lock.acquire()
            try:
                self.sync_trigger_pending = False
                self.sync_trigger_active = True
            finally:
                self.sync_trigger_lock.release()
            try:
                try:
                    tpool.execute(self._run_sync_trigger_command, item)
                except Exception:
                    stats_increment(self.logger, 'sync_trigger.failures')
                    self.logger.exception(
                        'Unexpected error running ring-manager sync trigger')
            finally:
                self.sync_trigger_lock.acquire()
                try:
                    self.sync_trigger_active = False
                finally:
                    self.sync_trigger_lock.release()
                self.sync_trigger_queue.task_done()

    def _run_sync_trigger_command(self, item):
        try:
            argv = shlex.split(self.sync_trigger_command)
        except ValueError as err:
            stats_increment(self.logger, 'sync_trigger.failures')
            self.logger.warning(
                'Invalid ring-manager sync trigger command %r: %s',
                self.sync_trigger_command, err)
            return
        if not argv:
            stats_increment(self.logger, 'sync_trigger.failures')
            self.logger.warning('Ring-manager sync trigger command is empty')
            return
        payload = item.get('payload') or {}
        env = os.environ.copy()
        env.update({
            'RING_MANAGER_SYNC_TRIGGERED_AT':
                str(item.get('triggered_at') or ''),
            'RING_MANAGER_SYNC_TRIGGER_REASON':
                str(payload.get('reason') or ''),
            'RING_MANAGER_SYNC_TRIGGER_EXPECTED_LATEST':
                str(payload.get('expected_latest') or ''),
        })
        cwd = self.store.state_dir if self.store.state_dir else None
        started_at = float(NormalTimestamp.now())
        timeout = self.sync_trigger_timeout or None
        try:
            proc = subprocess.Popen(
                argv, cwd=cwd, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            stats_increment(self.logger, 'sync_trigger.timeouts')
            stats_increment(self.logger, 'sync_trigger.failures')
            stats_timing_since(self.logger, 'sync_trigger.timing',
                               started_at)
            self.logger.warning(
                'Ring-manager sync trigger timed out after %s seconds',
                self.sync_trigger_timeout)
            return
        except OSError as err:
            stats_increment(self.logger, 'sync_trigger.failures')
            stats_timing_since(self.logger, 'sync_trigger.timing',
                               started_at)
            self.logger.warning(
                'Unable to run ring-manager sync trigger command %r: %s',
                self.sync_trigger_command, err)
            return
        if proc.returncode:
            stats_increment(self.logger, 'sync_trigger.failures')
            stats_timing_since(self.logger, 'sync_trigger.timing',
                               started_at)
            output = (stderr or stdout or b'').decode('utf-8', 'replace')
            self.logger.warning(
                'Ring-manager sync trigger exited %s: %s',
                proc.returncode, output.strip())
            return
        stats_increment(self.logger, 'sync_trigger.successes')
        stats_timing_since(self.logger, 'sync_trigger.timing', started_at)

    def _ring_manager_sync_status(self, latest_version, index_error=None):
        status = self._base_sync_status()
        if not status['applicable']:
            status['reasons'].append('mode_not_replicated')
            return status

        status['stale'] = True
        if self._sync_transaction_pending():
            status['sync_transaction_pending'] = True
            status['sync_transaction']['pending'] = True
            status['reasons'].append('sync_transaction_pending')
            status['promotion_blockers'].append('sync_transaction_pending')
            return status

        if index_error is not None:
            status['reasons'].append('invalid_state_index')
            status['promotion_blockers'].append('invalid_state_index')
            status['error'] = index_error
            return status

        try:
            index = self.store.get_state_index()
        except (IOError, ValueError) as err:
            status['reasons'].append('invalid_state_index')
            status['promotion_blockers'].append('invalid_state_index')
            status['error'] = str(err)
            return status

        sync_info = index.get('ring_manager_sync')
        if sync_info is None:
            status['reasons'].append('no_sync_record')
            status['promotion_blockers'].append('no_sync_record')
            return status
        if not isinstance(sync_info, dict):
            status['reasons'].append('invalid_sync_record')
            status['promotion_blockers'].append('invalid_sync_record')
            status['error'] = 'ring_manager_sync must be an object'
            return status

        status['source'] = sync_info.get('source')
        sync_latest = sync_info.get('latest_ring_version')
        if sync_latest in (None, ''):
            status['reasons'].append('missing_sync_latest_ring_version')
            status['promotion_blockers'].append(
                'missing_sync_latest_ring_version')
            return status
        sync_latest = str(sync_latest)
        status['latest_ring_version'] = sync_latest

        synced_at = sync_info.get('synced_at') or \
            sync_info.get('last_synced_at')
        if synced_at in (None, ''):
            status['reasons'].append('never_synced')
            status['promotion_blockers'].append('never_synced')
            return status
        try:
            last_synced = NormalTimestamp(synced_at)
        except (TypeError, ValueError, AssertionError) as err:
            status['last_synced_at'] = str(synced_at)
            status['reasons'].append('invalid_synced_at')
            status['promotion_blockers'].append('invalid_synced_at')
            status['error'] = str(err)
            return status

        now = NormalTimestamp.now()
        status['last_synced_at'] = last_synced.internal
        status['synced'] = True
        if float(last_synced) > float(now):
            status['age_seconds'] = 0.0
            status['clock_skew'] = True
            status['reasons'].append('clock_skew')
            status['promotion_blockers'].append('clock_skew')
            return status

        age = float(now) - float(last_synced)
        status['age_seconds'] = age
        if age > self.sync_freshness_threshold:
            status['reasons'].append('freshness_threshold_exceeded')
            status['promotion_blockers'].append(
                'freshness_threshold_exceeded')
            return status

        if latest_version is None:
            status['reasons'].append('no_latest_ring_version')
            status['promotion_blockers'].append('no_latest_ring_version')
            return status

        if sync_latest != latest_version:
            status['latest_matches_local'] = False
            status['reasons'].append('latest_version_mismatch')
            status['promotion_blockers'].append('latest_version_mismatch')
            return status
        status['latest_matches_local'] = True

        status['fresh'] = True
        status['stale'] = False
        status['can_serve_published_reads'] = True
        if self.mode == 'standby':
            if status['source']:
                status['published_state_promote_ready'] = True
            else:
                status['promotion_blockers'].append('no_sync_source')
        else:
            status['promotion_blockers'].append('mode_not_standby')
        return status

    def _base_promotion_readiness(self, sync_status):
        published_ready = bool(
            sync_status.get('published_state_promote_ready'))
        return {
            'applicable': self.mode == 'standby',
            'ready': False,
            'published_state': {
                'ready': published_ready,
                'blockers': list(sync_status.get('promotion_blockers') or []),
            },
            'builders': {
                'ready': False,
                'required': 0,
                'checked': 0,
                'missing': [],
                'invalid': [],
                'version_mismatches': [],
                'unpublished_builder_changes': [],
                'skipped_disabled': [],
                'disabled_in_latest_manifest': [],
                'blockers': [],
            },
            'blockers': [],
        }

    def _latest_manifest_ring_state(self):
        try:
            manifest = self.store.get_ring_version_manifest('latest')
        except RingVersionNotFound:
            return set(), {}, None
        except (IOError, ValueError) as err:
            return set(), {}, str(err)
        ring_ids = set()
        ring_versions = {}
        for ring in manifest.get('rings', []):
            if isinstance(ring, dict) and ring.get('ring_id') is not None:
                ring_id = str(ring['ring_id'])
                ring_ids.add(ring_id)
                version = ring.get('swift_ring_version', ring.get('version'))
                if version not in (None, ''):
                    ring_versions[ring_id] = version
        return ring_ids, ring_versions, None

    def _builder_file_promotion_status(self):
        status = {
            'ready': False,
            'required': 0,
            'checked': 0,
            'missing': [],
            'invalid': [],
            'version_mismatches': [],
            'unpublished_builder_changes': [],
            'skipped_disabled': [],
            'disabled_in_latest_manifest': [],
            'blockers': [],
        }
        blockers = []

        def invalid_builder(ring_id, reason):
            status['invalid'].append({
                'ring_id': str(ring_id),
                'reason': reason,
            })

        try:
            rings = self.store.list_rings()
        except (IOError, ValueError) as err:
            status['error'] = str(err)
            return False, status, ['invalid_ring_state']
        latest_manifest_ring_ids, latest_manifest_versions, \
            latest_manifest_error = self._latest_manifest_ring_state()
        if latest_manifest_error:
            status['latest_manifest_error'] = latest_manifest_error
            status['blockers'] = ['invalid_latest_manifest']
            return False, status, ['invalid_latest_manifest']

        for ring in rings:
            ring_id = ring.get('id')
            if self.store.ring_is_disabled(ring):
                if str(ring_id) not in latest_manifest_ring_ids:
                    status['skipped_disabled'].append(str(ring_id))
                    continue
                status['disabled_in_latest_manifest'].append(str(ring_id))
            status['required'] += 1
            try:
                _builder_path, builder = self.builder_manager.load_builder(
                    ring)
            except swift_exceptions.FileNotFoundError:
                status['missing'].append(str(ring_id))
                continue
            except RingBuilderManagerError as err:
                if 'requires exactly one builder file' in str(err):
                    invalid_builder(ring_id, 'invalid_builder_metadata')
                else:
                    invalid_builder(ring_id, 'builder_unloadable')
                continue
            except Exception:
                invalid_builder(ring_id, 'builder_unloadable')
                continue

            status['checked'] += 1
            expected_version = ring.get('builder_version')
            if expected_version not in (None, '') and (
                    str(expected_version) != str(builder.version)):
                status['version_mismatches'].append({
                    'ring_id': str(ring_id),
                    'expected': expected_version,
                    'actual': builder.version,
                })
                continue
            published_version = ring.get('latest_swift_ring_version')
            if published_version in (None, ''):
                published_version = latest_manifest_versions.get(
                    str(ring_id))
            if published_version not in (None, ''):
                try:
                    actual_version = int(builder.version)
                    published_version_int = int(published_version)
                except (TypeError, ValueError):
                    invalid_builder(ring_id, 'invalid_builder_version')
                    continue
                if actual_version < published_version_int:
                    status['version_mismatches'].append({
                        'ring_id': str(ring_id),
                        'minimum': published_version,
                        'actual': builder.version,
                    })
                elif actual_version > published_version_int:
                    status['unpublished_builder_changes'].append({
                        'ring_id': str(ring_id),
                        'published': published_version,
                        'actual': builder.version,
                    })

        if status['required'] == 0:
            blockers.append('no_enabled_rings')
        if status['missing']:
            blockers.append('missing_builder_files')
        if status['invalid']:
            blockers.append('invalid_builder_files')
        if status['version_mismatches']:
            blockers.append('builder_version_mismatch')
        if status['unpublished_builder_changes']:
            blockers.append('unpublished_builder_changes')
        if status['disabled_in_latest_manifest']:
            blockers.append('disabled_ring_in_latest_manifest')
        status['ready'] = not blockers
        status['blockers'] = blockers
        return not blockers, status, blockers

    def _promotion_readiness(self, sync_status):
        readiness = self._base_promotion_readiness(sync_status)
        if not readiness['applicable']:
            readiness['blockers'].append('mode_not_standby')
            return readiness
        if not readiness['published_state']['ready']:
            readiness['blockers'].append('published_state_not_ready')

        builder_ready, builder_status, builder_blockers = \
            self._builder_file_promotion_status()
        readiness['builders'] = builder_status
        readiness['blockers'].extend(builder_blockers)
        readiness['ready'] = (
            readiness['published_state']['ready'] and
            builder_ready and
            not readiness['blockers'])
        return readiness

    def _operator_attention_status(self, sync_status,
                                   promotion_readiness=None):
        attention = {
            'needed': False,
            'reasons': [],
            'sync': {
                'needed': False,
                'reasons': [],
            },
            'promotion': {
                'needed': False,
                'blockers': [],
            },
        }

        def add_reason(reason):
            if reason not in attention['reasons']:
                attention['reasons'].append(reason)

        if sync_status.get('applicable'):
            sync_reasons = list(sync_status.get('reasons') or [])
            if (sync_status.get('sync_transaction_pending') or
                    not sync_status.get('can_serve_published_reads')):
                attention['sync']['needed'] = True
                attention['sync']['reasons'] = sync_reasons
                add_reason('sync_not_fresh')
            if sync_status.get('sync_transaction_pending'):
                add_reason('sync_transaction_pending')

        if self.mode == 'standby':
            if promotion_readiness is not None:
                promotion_blockers = list(
                    promotion_readiness.get('blockers') or [])
                promotion_ready = bool(promotion_readiness.get('ready'))
            else:
                promotion_blockers = list(
                    sync_status.get('promotion_blockers') or [])
                promotion_ready = bool(
                    sync_status.get('published_state_promote_ready'))
            if not promotion_ready:
                attention['promotion']['needed'] = True
                attention['promotion']['blockers'] = promotion_blockers
                add_reason('promotion_not_ready')

        attention['needed'] = bool(attention['reasons'])
        return attention

    def _metric_suffix(self, value):
        return re.sub(r'[^A-Za-z0-9_.-]+', '_', str(value))

    def _emit_operator_attention_metrics(self, attention):
        if not attention.get('needed'):
            return
        stats_increment(self.logger, 'operator_attention')
        if attention.get('sync', {}).get('needed'):
            stats_increment(self.logger, 'operator_attention.sync')
        if attention.get('promotion', {}).get('needed'):
            stats_increment(self.logger, 'operator_attention.promotion')
        for reason in attention.get('reasons') or []:
            stats_increment(
                self.logger,
                'operator_attention.%s' % self._metric_suffix(reason))

    def ring_manager_status(self, req):
        index_error = None
        try:
            latest_version = self.store.get_latest_ring_version_id()
            desired_version = self.store.get_desired_ring_version_id()
        except (IOError, ValueError) as err:
            latest_version = None
            desired_version = None
            index_error = str(err)
        body = {
            'service': self.server_type,
            'version': swift_version,
            'status': 'ok',
            'mode': self.mode,
            'writable': self.writable,
            'latest_ring_version': latest_version,
            'desired_ring_version': desired_version,
            'ring_build_executor': self.ring_build_executor,
            'build_job_lease_timeout': self.build_job_lease_timeout,
            'ring_builds': self.store.ring_build_queue_stats(
                lease_timeout=self.build_job_lease_timeout),
            'ring_manager_sync': self._ring_manager_sync_status(
                latest_version, index_error=index_error),
            'sync_trigger': self._sync_trigger_status(),
        }
        if config_true_value(req.params.get('promotion', 'false')):
            body['promotion_readiness'] = self._promotion_readiness(
                body['ring_manager_sync'])
        body['operator_attention'] = self._operator_attention_status(
            body['ring_manager_sync'], body.get('promotion_readiness'))
        self._emit_operator_attention_metrics(body['operator_attention'])
        return http.json_response(req, body)

    def ring_manager_sync_trigger(self, req):
        stats_increment(self.logger, 'sync_trigger.requests')
        if self.writable:
            return http.json_error(
                req, HTTPConflict,
                'ring-manager sync trigger is available only in readonly '
                'or standby mode')
        if not self.sync_trigger_command:
            stats_increment(self.logger, 'sync_trigger.disabled')
            return http.json_error(
                req, HTTPConflict,
                'ring_manager_sync_trigger_command is not configured')

        payload = http.json_request_body(req, self.max_json_request_body_size)
        trigger = {
            'reason': payload.get('reason') or
            req.params.get('reason') or 'api',
            'expected_latest': payload.get('expected_latest') or
            req.params.get('expected_latest') or '',
        }
        queued, _item = self._queue_sync_trigger(trigger)
        if queued:
            stats_increment(self.logger, 'sync_trigger.queued')
            state = 'queued'
        else:
            stats_increment(self.logger, 'sync_trigger.already_pending')
            state = 'already_pending'
        return http.json_response(req, {
            'state': state,
            'queued': queued,
            'sync_trigger': self._sync_trigger_status(),
        }, status=202)


def app_factory(global_conf, **local_conf):
    """Paste.deploy app factory for creating WSGI ring-manager apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    return RingManagerApplication(conf)


def main():
    conf_file, options = parse_options(test_config=True)
    sys.exit(run_wsgi(conf_file, 'ring-manager-server', **options))


if __name__ == '__main__':
    main()
