#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import unittest

from swift.common.swob import Request
from swift.ring_manager import routing
from swift.ring_manager.server import app_factory, RingManagerApplication
from test.debug_logger import debug_logger


class TestRingManagerApplication(unittest.TestCase):
    def setUp(self):
        self.logger = debug_logger()
        self.app = RingManagerApplication({}, logger=self.logger)

    def get_json(self, path, method='GET', app=None):
        req = Request.blank(path, method=method)
        resp = req.get_response(app or self.app)
        try:
            body = json.loads(resp.body.decode('ascii'))
        except ValueError:
            body = None
        return resp, body

    def test_app_factory(self):
        self.assertIsInstance(app_factory({}), RingManagerApplication)

    def test_discovery_documents(self):
        resp, body = self.get_json('/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('ring-manager-server', body['service'])
        self.assertEqual('/api/v1/', body['api_versions'][0]['url'])
        self.assertEqual(
            '/api/v1/ring_manager/status/', body['links']['status'])

        resp, body = self.get_json('/api/v1/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('v1', body['api_version'])

    def test_status(self):
        resp, body = self.get_json('/api/v1/ring_manager/status/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('ok', body['status'])

    def test_route_contract(self):
        self.assertEqual([
            ('^/?$', ('GET',), 'root'),
            ('^/api/v1/?$', ('GET',), 'api_root'),
            ('^/api/v1/ring_manager/status/?$',
             ('GET',), 'ring_manager_status'),
        ], [
            (route.regex.pattern, route.methods, route.handler.__name__)
            for route in self.app.routes
        ])

    def test_method_negotiation(self):
        resp, body = self.get_json('/', method='OPTIONS')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('GET, HEAD, OPTIONS', resp.headers['Allow'])
        self.assertIsNone(body)

        resp, body = self.get_json('/', method='POST')
        self.assertEqual(405, resp.status_int)
        self.assertEqual('GET, HEAD, OPTIONS', resp.headers['Allow'])
        self.assertIsNone(body)

    def test_head(self):
        resp, body = self.get_json('/', method='HEAD')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(b'', resp.body)
        self.assertIsNone(body)

    def test_not_found(self):
        resp, body = self.get_json('/not-found')
        self.assertEqual(404, resp.status_int)
        self.assertIsNone(body)

    def test_errors_do_not_expose_tracebacks(self):
        def fail(req):
            raise RuntimeError('private failure detail')

        self.app.routes.insert(0, routing.Route(
            r'^/fail/?$', ('GET',), fail))
        resp, body = self.get_json('/fail')
        self.assertEqual(500, resp.status_int)
        self.assertEqual({'error': 'Internal server error'}, body)
        self.assertNotIn(b'private failure detail', resp.body)

    def test_native_access_log_can_be_disabled(self):
        app = RingManagerApplication(
            {'log_requests': 'false'}, logger=self.logger)
        self.get_json('/', app=app)
        self.assertEqual([], self.logger.get_lines_for_level('info'))


if __name__ == '__main__':
    unittest.main()
