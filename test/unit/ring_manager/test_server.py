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
import os
import shutil
import tempfile
import unittest
from unittest import mock

from swift.common.swob import Request
from swift.ring_manager import routing
from swift.ring_manager.common import DEFAULT_RING_MANAGER_STATE_DIR
from swift.ring_manager.server import app_factory, RingManagerApplication
from swift.ring_manager.store import RingManagerStore
from test.debug_logger import debug_logger


class TestRingManagerApplication(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.state_dir = os.path.join(self.testdir, 'state')
        self.logger = debug_logger()
        self.app = RingManagerApplication(
            {'ring_manager_state_dir': self.state_dir}, logger=self.logger)

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def get_json(self, path, method='GET', app=None):
        req = Request.blank(path, method=method)
        resp = req.get_response(app or self.app)
        try:
            body = json.loads(resp.body.decode('ascii'))
        except ValueError:
            body = None
        return resp, body

    def test_app_factory(self):
        app = app_factory({}, ring_manager_state_dir=self.state_dir)
        self.assertIsInstance(app, RingManagerApplication)
        self.assertEqual(self.state_dir, app.store.state_dir)

    def test_app_factory_uses_sample_config_defaults(self):
        app = app_factory({})
        self.assertEqual(DEFAULT_RING_MANAGER_STATE_DIR, app.store.state_dir)

    def test_controller_receives_store(self):
        self.assertIs(self.app.store, self.app.ring_controller._store)

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


class TestRingManagerStateDirApplication(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.state_dir = os.path.join(self.testdir, 'state')
        os.makedirs(self.state_dir)
        self.store = RingManagerStore(self.state_dir)

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def test_store_uses_unique_temp_paths_for_same_state_file(self):
        path = os.path.join(self.state_dir, 'index.json')
        fd1, temp_path1 = self.store._temporary_state_file(path)
        fd2, temp_path2 = self.store._temporary_state_file(path)
        try:
            self.assertNotEqual(temp_path1, temp_path2)
            self.assertEqual(self.state_dir, os.path.dirname(temp_path1))
            self.assertTrue(os.path.basename(temp_path1).startswith(
                '.index.json.'))
        finally:
            os.close(fd1)
            os.close(fd2)
            os.unlink(temp_path1)
            os.unlink(temp_path2)

    def test_store_write_failure_preserves_existing_json(self):
        index_path = os.path.join(self.state_dir, 'index.json')
        self.store._write_json_file(index_path, {'value': 'before'})

        with mock.patch(
                'swift.ring_manager.store.fsync',
                side_effect=OSError('fsync failed')):
            self.assertRaises(
                OSError, self.store._write_json_file,
                index_path, {'value': 'after'})

        with open(index_path) as fp:
            self.assertEqual({'value': 'before'}, json.load(fp))
        self.assertEqual([], [
            name for name in os.listdir(self.state_dir)
            if name.startswith('.index.json.') and name.endswith('.tmp')])

    def test_store_write_fsyncs_file_and_parent_directory(self):
        path = os.path.join(self.state_dir, 'rings', 'durable.json')
        os.makedirs(os.path.dirname(path))
        with mock.patch('swift.ring_manager.store.fsync') as mock_fsync, \
                mock.patch('swift.ring_manager.store.fsync_dir') as mock_dir:
            self.store._write_json_file(path, {'id': 'durable'})

        self.assertEqual(1, mock_fsync.call_count)
        mock_dir.assert_called_once_with(os.path.dirname(path))

    def test_store_write_fsyncs_created_directory_parents(self):
        releases_dir = os.path.join(self.state_dir, 'releases')
        os.makedirs(releases_dir)
        path = os.path.join(releases_dir, 'release-2', 'manifest.json')
        target_dir = os.path.dirname(path)
        with mock.patch('swift.ring_manager.store.fsync'), \
                mock.patch('swift.ring_manager.store.fsync_dir') as mock_dir:
            self.store._write_json_file(path, {'version': 'release-2'})

        self.assertEqual([
            releases_dir,
            target_dir,
        ], [call[0][0] for call in mock_dir.call_args_list])


if __name__ == '__main__':
    unittest.main()
