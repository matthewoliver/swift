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
import time

from swift import __version__ as swift_version
from swift.common.concurrency import Timeout
from swift.common.swob import HTTPBadRequest, HTTPException, \
    HTTPInternalServerError, HTTPMethodNotAllowed, HTTPNotFound, Request, \
    Response, wsgi_to_str
from swift.common.utils import config_true_value, get_log_line, get_logger, \
    LOG_LINE_DEFAULT_FORMAT, parse_options
from swift.common.wsgi import run_wsgi
from swift.ring_manager.controllers import ring as ring_controller
from swift.ring_manager import http, routing


RING_MANAGER_API_VERSION = 'v1'
RING_MANAGER_API_PREFIX = '/api/%s' % RING_MANAGER_API_VERSION


class RingManagerApplication(object):
    """WSGI application for Swift ring management."""

    server_type = 'ring-manager-server'

    def __init__(self, conf, logger=None, controller=None):
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
        self.ring_controller = controller or ring_controller.RingController()
        self.routes = self._make_routes()

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
        start_time = time.time()
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
        trans_time = time.time() - start_time
        self.logger.timing_since('%s.timing' % req.method.lower(), start_time)
        self._log_request(req, res, trans_time)
        return res(env, start_response)

    def _api_links(self):
        return {
            'status': '/api/v1/ring_manager/status/',
        }

    def _service_document(self, api_version=None):
        document = {
            'service': self.server_type,
            'version': swift_version,
            'api_versions': [{
                'id': RING_MANAGER_API_VERSION,
                'url': '%s/' % RING_MANAGER_API_PREFIX,
            }],
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
