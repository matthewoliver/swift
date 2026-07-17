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

import sys

from swift import __version__ as swift_version
from swift.common.concurrency import GreenPool, Timeout
from swift.common.swob import HTTPBadRequest, HTTPException, \
    HTTPForbidden, HTTPInternalServerError, HTTPMethodNotAllowed, \
    HTTPNotFound, Request, Response, wsgi_to_str
from swift.common.utils import config_true_value, get_log_line, get_logger, \
    config_positive_float_value, config_positive_int_value, \
    LOG_LINE_DEFAULT_FORMAT, non_negative_float, parse_options
from swift.common.wsgi import run_wsgi
from swift.ring_manager.builder import DEFAULT_MAX_EXPLICIT_DEVICE_ID, \
    RingBuilderManager
from swift.ring_manager.common import DEFAULT_BUILDER_LOCK_TIMEOUT, \
    DEFAULT_BUILD_JOB_LEASE_TIMEOUT, DEFAULT_RING_ARTIFACT_DIR, \
    DEFAULT_RING_BUILD_EXECUTOR, DEFAULT_RING_BUILD_MANAGER_WORKERS, \
    DEFAULT_RING_BUILDER_DIR, DEFAULT_RING_MANAGER_STATE_DIR, \
    DEFAULT_STATE_CHANGE_HOOK_TIMEOUT, NormalTimestamp, RING_BUILD_EXECUTORS
from swift.ring_manager.controllers import ring as ring_controller
from swift.ring_manager import http, routing
from swift.ring_manager.builder_daemon import RingBuildWorker
from swift.ring_manager.publisher import RingBuilderPublisher
from swift.ring_manager.store import RingManagerStore


RING_MANAGER_API_VERSION = 'v1'
RING_MANAGER_API_PREFIX = '/api/%s' % RING_MANAGER_API_VERSION
DEFAULT_MAX_JSON_REQUEST_BODY_SIZE = 1024 * 1024
DEFAULT_MAX_PARTITIONS_AT_RISK_SELECTORS = 1000
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
                self.max_partitions_at_risk_selectors))
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
        self.logger.timing_since('%s.timing' % req.method.lower(), start_time)
        self._log_request(req, res, trans_time)
        return res(env, start_response)

    def _api_links(self):
        return {
            'status': '/api/v1/ring_manager/status/',
            'rings': '/api/v1/rings/',
            'ring_builds': '/api/v1/rings/builds/',
            'ring_versions': '/api/v1/rings/releases/',
            'latest_ring_version': '/api/v1/rings/releases/latest/',
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

    def ring_manager_status(self, req):
        return http.json_response(req, {
            'service': self.server_type,
            'version': swift_version,
            'status': 'ok',
            'mode': self.mode,
            'writable': self.writable,
            'latest_ring_version': self.store.get_latest_ring_version_id(),
            'ring_build_executor': self.ring_build_executor,
            'build_job_lease_timeout': self.build_job_lease_timeout,
            'ring_builds': self.store.ring_build_queue_stats(
                lease_timeout=self.build_job_lease_timeout),
        })


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
