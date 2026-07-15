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

from swift.common.ring.builder import RingBuilder
from swift.common.swob import Request, Response
from swift.ring_manager import routing
from swift.ring_manager.builder import DEFAULT_MAX_EXPLICIT_DEVICE_ID
from swift.ring_manager.common import DEFAULT_BUILDER_LOCK_TIMEOUT, \
    DEFAULT_RING_BUILDER_DIR, DEFAULT_RING_MANAGER_STATE_DIR
from swift.ring_manager.server import app_factory, RingManagerApplication
from swift.ring_manager.middleware.auth import RingManagerAuthMiddleware
from swift.ring_manager.store import RingManagerStore, RingNotFound
from test.debug_logger import debug_logger


class TestRingManagerApplication(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.state_dir = os.path.join(self.testdir, 'state')
        self.builder_path = os.path.join(self.testdir, 'object.builder')
        self._make_builder(self.builder_path)
        self._write_json('rings/1.json', {
            'id': 1,
            'name': 'Account & Container',
            'cluster_id': 7,
            'cluster': '/api/v1/clusters/7/',
            'storage_policy_index': None,
            'policy_type': 'replication',
            'ever_pushed': True,
            'builder_files': [self.builder_path],
        })
        self._write_json('rings/2.json', {
            'id': 2,
            'name': 'Policy-1',
            'cluster': '/api/v1/clusters/8/',
            'storage_policy_index': 1,
            'policy_type': 'erasure_coding',
        })
        self.logger = debug_logger()
        self.app = RingManagerApplication(
            {
                'ring_manager_state_dir': self.state_dir,
                'ring_builder_dir': self.testdir,
            }, logger=self.logger)

    def _make_builder(self, path):
        builder = RingBuilder(4, 3, 1)
        for index in range(3):
            builder.add_dev({
                'id': index,
                'region': 1,
                'zone': index,
                'ip': '10.0.0.%d' % index,
                'port': 6000,
                'device': 'sd%d' % index,
                'replication_ip': '10.0.0.%d' % index,
                'replication_port': 6003,
                'weight': 100,
            })
        builder.rebalance(seed=1)
        builder.save(path)

    def _write_json(self, relpath, value):
        path = os.path.join(self.state_dir, relpath)
        directory = os.path.dirname(path)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        with open(path, 'w') as fp:
            json.dump(value, fp)

    def _builder_temp_files(self, builder_path):
        directory = os.path.dirname(builder_path)
        prefix = '.%s.' % os.path.basename(builder_path)
        return [
            name for name in os.listdir(directory)
            if name.startswith(prefix) and name.endswith('.tmp')]

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

    def json_request(self, path, method, body, app=None):
        req = Request.blank(
            path, method=method,
            body=json.dumps(body).encode('ascii'),
            headers={'Content-Type': 'application/json'})
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
        self.assertEqual(DEFAULT_RING_BUILDER_DIR, app.ring_builder_dir)
        self.assertEqual(DEFAULT_MAX_EXPLICIT_DEVICE_ID,
                         app.max_explicit_device_id)
        self.assertEqual(DEFAULT_BUILDER_LOCK_TIMEOUT,
                         app.builder_lock_timeout)

    def test_controller_receives_store(self):
        self.assertIs(self.app.store, self.app.ring_controller._store)
        self.assertIs(
            self.app.builder_manager,
            self.app.ring_controller._builder_manager)

    def test_discovery_documents(self):
        resp, body = self.get_json('/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('ring-manager-server', body['service'])
        self.assertEqual('/api/v1/', body['api_versions'][0]['url'])
        self.assertEqual(
            '/api/v1/ring_manager/status/', body['links']['status'])
        self.assertEqual('/api/v1/rings/', body['links']['rings'])

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
            ('^/api/v1/rings/schema/?$', ('GET',), 'ring_schema'),
            ('^/api/v1/rings/?$', ('GET', 'POST'), 'ring_list'),
            ('^/api/v1/rings/membership/device/'
             '(?P<device_id>[0-9]+)/?$', ('GET',),
             'ring_membership_device'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/?$',
             ('GET', 'PUT', 'PATCH', 'DELETE'), 'ring_detail'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/devices/?$',
             ('GET', 'PUT'), 'ring_devices'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/devices/add/?$',
             ('POST',), 'ring_devices_add'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/devices/remove/?$',
             ('POST',), 'ring_devices_remove'),
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

    def test_ring_schema(self):
        resp, body = self.get_json('/api/v1/rings/schema/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(['get', 'put', 'patch', 'delete'],
                         body['allowed_detail_http_methods'])
        self.assertEqual(['get', 'post'], body['allowed_list_http_methods'])
        for field in ('id', 'name', 'cluster', 'policy_type',
                      'storage_policy_index', 'resource_uri'):
            self.assertIn(field, body['fields'])

    def test_ring_list(self):
        resp, body = self.get_json('/api/v1/rings/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(2, body['meta']['total_count'])
        self.assertEqual(
            '/api/v1/rings/1/', body['objects'][0]['resource_uri'])
        self.assertNotIn('num_replicas', body['objects'][0])
        self.assertNotIn('part_power', body['objects'][0])

    def test_ring_list_cluster_filter(self):
        resp, body = self.get_json('/api/v1/rings/?cluster_id=8')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(1, body['meta']['total_count'])
        self.assertEqual(2, body['objects'][0]['id'])

    def test_ring_detail(self):
        resp, body = self.get_json('/api/v1/rings/1/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Account & Container', body['name'])
        self.assertEqual('/api/v1/rings/1/', body['resource_uri'])
        self.assertEqual(4, body['part_power'])
        self.assertEqual(3, body['num_replicas'])
        self.assertEqual(1, body['min_part_hours'])
        self.assertEqual(3, body['device_count'])
        self.assertIn('builder_version', body)

    def test_ring_detail_not_found(self):
        resp, body = self.get_json('/api/v1/rings/404/')
        self.assertEqual(404, resp.status_int)
        self.assertIsNone(body)

    def test_ring_create(self):
        resp, body = self.json_request('/api/v1/rings/', 'POST', {
            'name': 'Policy-2',
            'cluster_id': 7,
            'storage_policy_index': 2,
            'policy_type': 'replication',
        })
        self.assertEqual(201, resp.status_int)
        self.assertEqual('object-2', body['id'])
        self.assertEqual('/api/v1/rings/object-2/', body['resource_uri'])

        resp, body = self.get_json('/api/v1/rings/object-2/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Policy-2', body['name'])

    def test_ring_create_writes_builder_settings_to_builder(self):
        builder_path = os.path.join(self.testdir, 'object-2.builder')
        resp, body = self.json_request('/api/v1/rings/', 'POST', {
            'name': 'Policy-2',
            'ring_type': 'object',
            'storage_policy_index': 2,
            'policy_type': 'replication',
            'part_power': 6,
            'num_replicas': 2.5,
            'min_part_hours': 4,
            'overload': 0.1,
            'builder_files': [builder_path],
        })
        self.assertEqual(201, resp.status_int)
        self.assertEqual('object-2', body['id'])
        self.assertEqual(6, body['part_power'])
        self.assertEqual(2.5, body['num_replicas'])
        self.assertEqual(4, body['min_part_hours'])
        self.assertEqual(0.1, body['overload'])
        self.assertEqual(0, body['device_count'])

        builder = RingBuilder.load(builder_path)
        self.assertEqual(6, builder.part_power)
        self.assertEqual(2.5, builder.replicas)
        self.assertEqual(4, builder.min_part_hours)
        self.assertEqual(0.1, builder.overload)

        with open(os.path.join(
                self.state_dir, 'rings', 'object-2.json')) as fp:
            stored = json.load(fp)
        self.assertNotIn('part_power', stored)
        self.assertNotIn('num_replicas', stored)
        self.assertNotIn('min_part_hours', stored)
        self.assertNotIn('overload', stored)

    def test_ring_create_conflict(self):
        resp, body = self.json_request('/api/v1/rings/', 'POST', {
            'id': 1,
            'name': 'Duplicate',
        })
        self.assertEqual(409, resp.status_int)
        self.assertEqual('Ring already exists', body['error'])

    def test_ring_update(self):
        resp, body = self.json_request('/api/v1/rings/1/', 'PATCH', {
            'name': 'Renamed',
            'rebalance_cork': True,
        })
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Renamed', body['name'])
        self.assertEqual(True, body['rebalance_cork'])
        self.assertNotIn('devices', body)
        self.assertEqual('/api/v1/rings/1/devices/',
                         body['devices_url'])

    def test_ring_update_writes_builder_settings_to_builder(self):
        resp, body = self.json_request('/api/v1/rings/1/', 'PATCH', {
            'num_replicas': 2.5,
            'min_part_hours': 6,
            'overload': 0.2,
        })
        self.assertEqual(200, resp.status_int)
        self.assertEqual(2.5, body['num_replicas'])
        self.assertEqual(6, body['min_part_hours'])
        self.assertEqual(0.2, body['overload'])
        self.assertEqual(3, body['device_count'])

        builder = RingBuilder.load(self.builder_path)
        self.assertEqual(2.5, builder.replicas)
        self.assertEqual(6, builder.min_part_hours)
        self.assertEqual(0.2, builder.overload)

        with open(os.path.join(self.state_dir, 'rings', '1.json')) as fp:
            stored = json.load(fp)
        self.assertNotIn('num_replicas', stored)
        self.assertNotIn('min_part_hours', stored)
        self.assertNotIn('overload', stored)

    def test_ring_update_rejects_part_power_change(self):
        resp, body = self.json_request('/api/v1/rings/1/', 'PATCH', {
            'part_power': 6,
        })
        self.assertEqual(400, resp.status_int)
        self.assertIn('part_power', body['error'])

        builder = RingBuilder.load(self.builder_path)
        self.assertEqual(4, builder.part_power)

    def test_ring_update_not_found(self):
        resp, body = self.json_request('/api/v1/rings/404/', 'PATCH', {
            'name': 'Nope',
        })
        self.assertEqual(404, resp.status_int)
        self.assertIsNone(body)

    def test_ring_update_rejects_id_mismatch(self):
        resp, body = self.json_request('/api/v1/rings/1/', 'PATCH', {
            'id': 2,
            'name': 'Wrong',
        })
        self.assertEqual(400, resp.status_int)
        self.assertEqual('Request body id does not match ring id',
                         body['error'])

    def test_ring_delete(self):
        req = Request.blank('/api/v1/rings/2/', method='DELETE')
        resp = req.get_response(self.app)
        self.assertEqual(204, resp.status_int)

        resp, body = self.get_json('/api/v1/rings/2/')
        self.assertEqual(404, resp.status_int)
        self.assertIsNone(body)

    def test_ring_create_invalid_json(self):
        req = Request.blank(
            '/api/v1/rings/', method='POST', body=b'{',
            headers={'Content-Type': 'application/json'})
        resp = req.get_response(self.app)
        self.assertEqual(400, resp.status_int)
        self.assertEqual({'error': 'Request body must be valid JSON'},
                         json.loads(resp.body.decode('ascii')))

    def test_ring_devices(self):
        resp, body = self.get_json('/api/v1/rings/1/devices/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(3, body['meta']['total_count'])
        self.assertEqual('sd0', body['devices'][0]['device'])
        self.assertEqual('10.0.0.0', body['devices'][0]['ip'])
        self.assertEqual(100.0, body['devices'][0]['weight'])

    def test_ring_devices_pagination(self):
        resp, body = self.get_json('/api/v1/rings/1/devices/?limit=2')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(3, body['meta']['total_count'])
        self.assertEqual(2, len(body['devices']))
        self.assertEqual('1', body['meta']['marker'])
        self.assertTrue(body['meta']['next'].endswith('marker=1'))

        resp, body = self.get_json(
            '/api/v1/rings/1/devices/?marker=1&limit=2')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(['sd2'],
                         [device['device'] for device in body['devices']])

    def test_ring_devices_replace(self):
        resp, body = self.json_request('/api/v1/rings/1/devices/', 'PUT', {
            'devices': [{
                'label': 'node-99:sdb',
                'region': 1,
                'zone': 9,
                'ip': '10.0.9.99',
                'port': 6000,
                'replication_ip': '10.1.9.99',
                'replication_port': 6003,
                'device': 'sdb',
                'weight': 50,
            }],
        })
        self.assertEqual(200, resp.status_int)
        self.assertEqual(1, body['device_count'])
        self.assertEqual('sdb', body['devices'][0]['device'])
        self.assertEqual('node-99:sdb', body['devices'][0]['label'])

        resp, body = self.get_json('/api/v1/rings/1/devices/')
        self.assertEqual(1, body['meta']['total_count'])
        self.assertEqual('sdb', body['devices'][0]['device'])
        resp, ring = self.get_json('/api/v1/rings/1/')
        self.assertNotIn('devices', ring)
        self.assertEqual(1, ring['device_count'])

    def test_ring_devices_add_nodes_payload(self):
        resp, body = self.json_request(
            '/api/v1/rings/1/devices/add/', 'POST', {
                'nodes': [{
                    'id': 'node-99',
                    'region': 1,
                    'zone': 9,
                    'ip': '10.0.9.99',
                    'port': 6000,
                    'replication_ip': '10.1.9.99',
                    'replication_port': 6003,
                    'devices': [
                        {'name': 'sdb', 'weight': 100},
                        {'name': 'sdc', 'weight': 100},
                    ],
                }],
            })
        self.assertEqual(200, resp.status_int)
        self.assertEqual('add_rings', body['action'])
        self.assertEqual('Account & Container', body['ring'])
        self.assertEqual(5, body['device_count'])
        self.assertEqual(['node-99:sdb', 'node-99:sdc'],
                         [dev['label'] for dev in body['devices']])

        resp, body = self.get_json('/api/v1/rings/1/devices/')
        self.assertEqual(5, body['meta']['total_count'])
        self.assertEqual(['sd0', 'sd1', 'sd2', 'sdb', 'sdc'],
                         [dev['device'] for dev in body['devices']])

    def test_ring_devices_remove_devices_payload(self):
        self.json_request('/api/v1/rings/1/devices/add/', 'POST', {
            'devices': [{
                'label': 'node-99:sdb',
                'region': 1,
                'zone': 9,
                'ip': '10.0.9.99',
                'port': 6000,
                'replication_ip': '10.1.9.99',
                'replication_port': 6003,
                'device': 'sdb',
                'weight': 100,
            }, {
                'label': 'node-99:sdc',
                'region': 1,
                'zone': 9,
                'ip': '10.0.9.99',
                'port': 6000,
                'replication_ip': '10.1.9.99',
                'replication_port': 6003,
                'device': 'sdc',
                'weight': 100,
            }],
        })
        resp, body = self.json_request(
            '/api/v1/rings/1/devices/remove/', 'POST', {
                'devices': [{'label': 'node-99:sdb'}],
            })
        self.assertEqual(200, resp.status_int)
        self.assertEqual('remove_rings', body['action'])
        self.assertEqual(4, body['device_count'])
        self.assertEqual('node-99:sdb', body['devices'][0]['label'])
        self.assertEqual(True, body['devices'][0]['pending_removal'])

        resp, body = self.get_json('/api/v1/rings/1/devices/')
        self.assertEqual(4, body['meta']['total_count'])
        self.assertEqual(['sd0', 'sd1', 'sd2', 'sdc'],
                         [dev['device'] for dev in body['devices']])

    def test_ring_create_rejects_invalid_builder_settings(self):
        cases = [{
            'field': 'part_power',
            'value': 1.5,
            'message': 'part_power must be a non-negative integer',
        }, {
            'field': 'part_power',
            'value': True,
            'message': 'part_power must be a non-negative integer',
        }, {
            'field': 'part_power',
            'value': '\u00b2',
            'message': 'part_power must be a non-negative integer',
        }, {
            'field': 'part_power',
            'value': '\u0663',
            'message': 'part_power must be a non-negative integer',
        }, {
            'field': 'num_replicas',
            'value': float('nan'),
            'message': 'num_replicas must be a finite number at least 1',
        }, {
            'field': 'num_replicas',
            'value': 1e308,
            'message': 'num_replicas creates too many replica slots',
        }, {
            'field': 'min_part_hours',
            'value': -1,
            'message': 'min_part_hours must be a non-negative integer',
        }, {
            'field': 'min_part_hours',
            'value': '9' * 5000,
            'message': 'min_part_hours must be a non-negative integer',
        }, {
            'field': 'overload',
            'value': float('inf'),
            'message': 'overload must be a finite number greater than '
                       'or equal to 0',
        }]
        for index, case in enumerate(cases):
            policy_index = 20 + index
            ring_id = 'object-%s' % policy_index
            builder_path = os.path.join(self.testdir, '%s.builder' % ring_id)
            payload = {
                'name': 'Policy-%s' % policy_index,
                'ring_type': 'object',
                'storage_policy_index': policy_index,
                'policy_type': 'replication',
                'part_power': 6,
                'num_replicas': 3,
                'min_part_hours': 1,
                'builder_files': [builder_path],
            }
            payload[case['field']] = case['value']
            resp, body = self.json_request('/api/v1/rings/', 'POST', payload)
            self.assertEqual(400, resp.status_int)
            self.assertIn(case['message'], body['error'])
            self.assertFalse(os.path.exists(builder_path))
            self.assertFalse(os.path.exists(os.path.join(
                self.state_dir, 'rings', '%s.json' % ring_id)))

    def test_ring_create_accepts_string_builder_settings(self):
        builder_path = os.path.join(self.testdir, 'object-2.builder')
        resp, body = self.json_request('/api/v1/rings/', 'POST', {
            'name': 'Policy-2',
            'ring_type': 'object',
            'storage_policy_index': 2,
            'policy_type': 'replication',
            'part_power': '6',
            'num_replicas': '2.5',
            'min_part_hours': '4',
            'overload': '0.1',
            'builder_files': [builder_path],
        })
        self.assertEqual(201, resp.status_int)
        self.assertEqual(6, body['part_power'])
        self.assertEqual(2.5, body['num_replicas'])
        self.assertEqual(4, body['min_part_hours'])
        self.assertEqual(0.1, body['overload'])

        builder = RingBuilder.load(builder_path)
        self.assertEqual(6, builder.part_power)
        self.assertEqual(2.5, builder.replicas)
        self.assertEqual(4, builder.min_part_hours)
        self.assertEqual(0.1, builder.overload)

    def test_ring_update_accepts_string_builder_settings(self):
        resp, body = self.json_request('/api/v1/rings/1/', 'PATCH', {
            'num_replicas': '2.5',
            'min_part_hours': '6',
            'overload': '0.2',
        })
        self.assertEqual(200, resp.status_int)
        self.assertEqual(2.5, body['num_replicas'])
        self.assertEqual(6, body['min_part_hours'])
        self.assertEqual(0.2, body['overload'])
        self.assertEqual(3, body['device_count'])

        builder = RingBuilder.load(self.builder_path)
        self.assertEqual(2.5, builder.replicas)
        self.assertEqual(6, builder.min_part_hours)
        self.assertEqual(0.2, builder.overload)

        with open(os.path.join(self.state_dir, 'rings', '1.json')) as fp:
            stored = json.load(fp)
        self.assertNotIn('num_replicas', stored)
        self.assertNotIn('min_part_hours', stored)
        self.assertNotIn('overload', stored)

    def test_ring_update_rejects_invalid_builder_settings(self):
        cases = [{
            'payload': {'part_power': 1.5},
            'message': 'part_power must be a non-negative integer',
        }, {
            'payload': {'part_power': '\u00b2'},
            'message': 'part_power must be a non-negative integer',
        }, {
            'payload': {'part_power': '\u0663'},
            'message': 'part_power must be a non-negative integer',
        }, {
            'payload': {'num_replicas': float('inf')},
            'message': 'num_replicas must be a finite number at least 1',
        }, {
            'payload': {'num_replicas': True},
            'message': 'num_replicas must be a finite number at least 1',
        }, {
            'payload': {'num_replicas': 1e308},
            'message': 'num_replicas creates too many replica slots',
        }, {
            'payload': {'min_part_hours': -1},
            'message': 'min_part_hours must be a non-negative integer',
        }, {
            'payload': {'min_part_hours': '9' * 5000},
            'message': 'min_part_hours must be a non-negative integer',
        }, {
            'payload': {'overload': float('nan')},
            'message': 'overload must be a finite number greater than '
                       'or equal to 0',
        }, {
            'payload': {'num_replicas': 4, 'overload': float('nan')},
            'message': 'overload must be a finite number greater than '
                       'or equal to 0',
        }]
        for case in cases:
            before = RingBuilder.load(self.builder_path)
            before_values = (
                before.version, before.part_power, before.replicas,
                before.min_part_hours, before.overload)

            resp, body = self.json_request(
                '/api/v1/rings/1/', 'PATCH', case['payload'])
            self.assertEqual(400, resp.status_int)
            self.assertIn(case['message'], body['error'])

            after = RingBuilder.load(self.builder_path)
            self.assertEqual(before_values, (
                after.version, after.part_power, after.replicas,
                after.min_part_hours, after.overload))

    def test_ring_update_rejects_existing_replica_slot_overflow(self):
        builder = RingBuilder.load(self.builder_path)
        builder.replicas = 1e308
        builder.save(self.builder_path)

        resp, body = self.json_request('/api/v1/rings/1/', 'PATCH', {
            'num_replicas': 3,
        })
        self.assertEqual(400, resp.status_int)
        self.assertIn('num_replicas creates too many replica slots',
                      body['error'])

        after = RingBuilder.load(self.builder_path)
        self.assertEqual(1e308, after.replicas)

    def test_ring_devices_add_rejects_invalid_topology(self):
        cases = [{
            'field': 'region',
            'value': -1,
            'message': 'field region must be a non-negative integer',
        }, {
            'field': 'zone',
            'value': 1.5,
            'message': 'field zone must be a non-negative integer',
        }, {
            'field': 'id',
            'value': -1,
            'message': 'field id must be a non-negative integer',
        }, {
            'field': 'region',
            'value': '\u00b2',
            'message': 'field region must be a non-negative integer',
        }, {
            'field': 'zone',
            'value': '\u0663',
            'message': 'field zone must be a non-negative integer',
        }, {
            'field': 'id',
            'value': '9' * 5000,
            'message': 'field id must be a non-negative integer',
        }, {
            'field': 'port',
            'value': 0,
            'message': 'field port must be an integer from 1 to 65535',
        }, {
            'field': 'replication_port',
            'value': 65536,
            'message': 'field replication_port must be an integer from '
                       '1 to 65535',
        }, {
            'field': 'device',
            'value': '../sdb',
            'message': 'field device must be a device name, not a path',
        }, {
            'field': 'ip',
            'value': 'bad host name',
            'message': 'field ip has invalid address',
        }, {
            'field': 'weight',
            'value': float('nan'),
            'message': 'field weight must be a finite non-negative number',
        }]
        for case in cases:
            device = {
                'label': 'node-99:sdb',
                'region': 1,
                'zone': 9,
                'ip': '10.0.9.99',
                'port': 6000,
                'replication_ip': '10.1.9.99',
                'replication_port': 6003,
                'device': 'sdb',
                'weight': 100,
            }
            device[case['field']] = case['value']
            resp, body = self.json_request(
                '/api/v1/rings/1/devices/add/', 'POST', {
                    'devices': [device],
                })
            self.assertEqual(400, resp.status_int)
            self.assertIn(case['message'], body['error'])

    def test_ring_devices_add_rejects_duplicate_explicit_id(self):
        resp, body = self.json_request(
            '/api/v1/rings/1/devices/add/', 'POST', {
                'devices': [{
                    'id': 10,
                    'label': 'node-99:sdb',
                    'region': 1,
                    'zone': 9,
                    'ip': '10.0.9.99',
                    'port': 6000,
                    'replication_ip': '10.1.9.99',
                    'replication_port': 6003,
                    'device': 'sdb',
                    'weight': 100,
                }, {
                    'id': 10,
                    'label': 'node-99:sdc',
                    'region': 1,
                    'zone': 9,
                    'ip': '10.0.9.99',
                    'port': 6000,
                    'replication_ip': '10.1.9.99',
                    'replication_port': 6003,
                    'device': 'sdc',
                    'weight': 100,
                }],
            })
        self.assertEqual(400, resp.status_int)
        self.assertIn('duplicate desired device id 10', body['error'])

    def test_ring_devices_add_rejects_sparse_explicit_id(self):
        before = RingBuilder.load(self.builder_path)
        before_version = before.version
        before_devices = [
            dev['device'] for dev in before.devs if dev is not None]

        resp, body = self.json_request(
            '/api/v1/rings/1/devices/add/', 'POST', {
                'devices': [{
                    'id': DEFAULT_MAX_EXPLICIT_DEVICE_ID + 1,
                    'label': 'node-99:sdb',
                    'region': 1,
                    'zone': 9,
                    'ip': '10.0.9.99',
                    'port': 6000,
                    'replication_ip': '10.1.9.99',
                    'replication_port': 6003,
                    'device': 'sdb',
                    'weight': 100,
                }],
            })
        self.assertEqual(400, resp.status_int)
        self.assertIn(
            'field id must be less than or equal to '
            'max_explicit_device_id %s' % DEFAULT_MAX_EXPLICIT_DEVICE_ID,
            body['error'])

        after = RingBuilder.load(self.builder_path)
        self.assertEqual(before_version, after.version)
        self.assertEqual(before_devices, [
            dev['device'] for dev in after.devs if dev is not None])

    def test_ring_devices_add_accepts_structured_hostnames_and_ipv6(self):
        resp, body = self.json_request(
            '/api/v1/rings/1/devices/add/', 'POST', {
                'devices': [{
                    'label': 'node-99:sdb',
                    'region': 1,
                    'zone': 9,
                    'ip': 'NODE99.EXAMPLE.COM',
                    'port': 6000,
                    'replication_ip': 'fe80::0204:61ff:ff9d:1234',
                    'replication_port': 6003,
                    'device': 'sdb',
                    'weight': 100,
                }],
            })
        self.assertEqual(200, resp.status_int)
        self.assertEqual('node99.example.com', body['devices'][0]['ip'])
        self.assertEqual('fe80::204:61ff:ff9d:1234',
                         body['devices'][0]['replication_ip'])

    def test_ring_devices_add_validation_is_all_or_nothing(self):
        before = RingBuilder.load(self.builder_path)
        before_version = before.version
        before_devices = [
            dev['device'] for dev in before.devs if dev is not None]

        resp, body = self.json_request(
            '/api/v1/rings/1/devices/add/', 'POST', {
                'devices': [{
                    'label': 'node-99:sdb',
                    'region': 1,
                    'zone': 9,
                    'ip': '10.0.9.99',
                    'port': 6000,
                    'replication_ip': '10.1.9.99',
                    'replication_port': 6003,
                    'device': 'sdb',
                    'weight': 100,
                }, {
                    'label': 'node-99:sdc',
                    'region': 1,
                    'zone': 9,
                    'ip': '10.0.9.99',
                    'port': 6000,
                    'replication_ip': '10.1.9.99',
                    'replication_port': 6003,
                    'device': 'sdc',
                    'weight': -1,
                }],
            })
        self.assertEqual(400, resp.status_int)
        self.assertIn('field weight must be a finite non-negative number',
                      body['error'])

        after = RingBuilder.load(self.builder_path)
        self.assertEqual(before_version, after.version)
        self.assertEqual(before_devices, [
            dev['device'] for dev in after.devs if dev is not None])

    def test_ring_devices_replace_validation_is_all_or_nothing(self):
        before = RingBuilder.load(self.builder_path)
        before_version = before.version
        before_devices = [
            dev['device'] for dev in before.devs if dev is not None]

        resp, body = self.json_request('/api/v1/rings/1/devices/', 'PUT', {
            'devices': [{
                'label': 'node-99:sdb',
                'region': 1,
                'zone': 9,
                'ip': '10.0.9.99',
                'port': 6000,
                'replication_ip': '10.1.9.99',
                'replication_port': 6003,
                'device': 'sdb',
                'weight': 100,
            }, {
                'label': 'node-99:sdc',
                'region': 1,
                'zone': 9,
                'ip': '10.0.9.99',
                'port': 6000,
                'replication_ip': '10.1.9.99',
                'replication_port': 6003,
                'device': 'sdc',
                'weight': float('inf'),
            }],
        })
        self.assertEqual(400, resp.status_int)
        self.assertIn('field weight must be a finite non-negative number',
                      body['error'])

        after = RingBuilder.load(self.builder_path)
        self.assertEqual(before_version, after.version)
        self.assertEqual(before_devices, [
            dev['device'] for dev in after.devs if dev is not None])

    def test_builder_save_fsyncs_file_and_parent_directory(self):
        builder_path = os.path.join(self.testdir, 'durable.builder')
        builder = RingBuilder(4, 3, 1)

        with mock.patch('swift.ring_manager.builder.fsync') as mock_fsync, \
                mock.patch(
                    'swift.ring_manager.builder.fsync_dir') as mock_dir:
            self.app.builder_manager.save_builder(builder, builder_path)

        self.assertEqual(1, mock_fsync.call_count)
        mock_dir.assert_called_once_with(os.path.dirname(builder_path))
        self.assertEqual([], self._builder_temp_files(builder_path))
        loaded = RingBuilder.load(builder_path)
        self.assertEqual(4, loaded.part_power)
        self.assertEqual(3, loaded.replicas)

    def test_builder_save_failure_preserves_existing_builder(self):
        with open(self.builder_path, 'rb') as fp:
            before_body = fp.read()
        builder = RingBuilder.load(self.builder_path)
        builder.set_replicas(2.5)

        with mock.patch(
                'swift.ring_manager.builder.fsync',
                side_effect=OSError('builder fsync failed')):
            self.assertRaises(
                OSError, self.app.builder_manager.save_builder,
                builder, self.builder_path)

        with open(self.builder_path, 'rb') as fp:
            self.assertEqual(before_body, fp.read())
        self.assertEqual([], self._builder_temp_files(self.builder_path))

    def test_builder_save_preserves_existing_file_mode(self):
        os.chmod(self.builder_path, 0o640)
        builder = RingBuilder.load(self.builder_path)
        builder.set_replicas(2.5)

        self.app.builder_manager.save_builder(builder, self.builder_path)

        mode = os.stat(self.builder_path).st_mode
        self.assertEqual(0o640, mode & 0o777)
        self.assertEqual([], self._builder_temp_files(self.builder_path))

    def test_ring_membership_device_is_not_implemented(self):
        resp, body = self.get_json(
            '/api/v1/rings/membership/device/7/')
        self.assertEqual(501, resp.status_int)
        self.assertIn('not implemented', body['error'])

    def test_none_ring_id_never_matches_first_ring(self):
        self.assertRaises(RingNotFound, self.app.store.get_ring, None)


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


class TestRingManagerAuthMiddleware(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.app = lambda env, start_response: Response(
            body=b'OK')(env, start_response)

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def _write_secret(self, name, value):
        path = os.path.join(self.testdir, name)
        with open(path, 'wb') as fp:
            fp.write(value)
        os.chmod(path, 0o600)
        return path

    def test_allow_unauthenticated(self):
        app = RingManagerAuthMiddleware(
            self.app, {'allow_unauthenticated': 'true'},
            logger=debug_logger())
        req = Request.blank('/api/v1/rings/')
        self.assertEqual(200, req.get_response(app).status_int)

    def test_missing_keys_fail_closed(self):
        app = RingManagerAuthMiddleware(
            self.app, {}, logger=debug_logger())
        self.assertEqual(
            503, Request.blank('/api/v1/rings/').get_response(app).status_int)
        self.assertEqual(
            503, Request.blank(
                '/api/v1/rings/', method='POST').get_response(app).status_int)

    def test_admin_key_allows_reads_and_writes(self):
        app = RingManagerAuthMiddleware(
            self.app, {'admin_key': 'secret'}, logger=debug_logger())
        self.assertEqual(
            401, Request.blank('/api/v1/rings/').get_response(app).status_int)

        headers = {'X-Ring-Manager-Admin-Key': 'secret'}
        self.assertEqual(200, Request.blank(
            '/api/v1/rings/', headers=headers).get_response(app).status_int)
        self.assertEqual(200, Request.blank(
            '/api/v1/rings/', method='POST', headers=headers,
            body=b'{}').get_response(app).status_int)

    def test_key_files(self):
        app = RingManagerAuthMiddleware(
            self.app, {
                'admin_key_file': self._write_secret(
                    'admin.key', b'admin\n'),
                'read_key_file': self._write_secret(
                    'read.key', b'reader\n'),
            }, logger=debug_logger())

        self.assertEqual(200, Request.blank(
            '/api/v1/rings/',
            headers={'X-Ring-Manager-Read-Key': 'reader'}
        ).get_response(app).status_int)
        self.assertEqual(200, Request.blank(
            '/api/v1/rings/', method='POST',
            headers={'X-Ring-Manager-Admin-Key': 'admin'},
            body=b'{}').get_response(app).status_int)

    def test_key_file_conflict_fails_closed(self):
        with self.assertRaises(ValueError) as cm:
            RingManagerAuthMiddleware(
                self.app, {
                    'admin_key': 'inline',
                    'admin_key_file': self._write_secret(
                        'admin.key', b'admin\n'),
                }, logger=debug_logger())
        self.assertIn('mutually exclusive', str(cm.exception))

    def test_read_key_allows_reads_not_writes_or_builders(self):
        app = RingManagerAuthMiddleware(
            self.app, {'admin_key': 'admin', 'read_key': 'reader'},
            logger=debug_logger())
        read_headers = {'X-Ring-Manager-Read-Key': 'reader'}

        self.assertEqual(200, Request.blank(
            '/api/v1/rings/', headers=read_headers
        ).get_response(app).status_int)
        self.assertEqual(401, Request.blank(
            '/api/v1/rings/', method='POST', headers=read_headers,
            body=b'{}').get_response(app).status_int)
        self.assertEqual(401, Request.blank(
            '/api/v1/rings/1/builder/', headers=read_headers
        ).get_response(app).status_int)
        self.assertEqual(200, Request.blank(
            '/api/v1/rings/1/builder/',
            headers={'X-Ring-Manager-Admin-Key': 'admin'}
        ).get_response(app).status_int)

    def test_read_key_without_admin_keeps_admin_requests_unavailable(self):
        app = RingManagerAuthMiddleware(
            self.app, {'read_key': 'reader'}, logger=debug_logger())
        headers = {'X-Ring-Manager-Read-Key': 'reader'}

        self.assertEqual(200, Request.blank(
            '/api/v1/rings/', headers=headers).get_response(app).status_int)
        self.assertEqual(503, Request.blank(
            '/api/v1/rings/', method='POST', headers=headers,
            body=b'{}').get_response(app).status_int)
        self.assertEqual(503, Request.blank(
            '/api/v1/rings/1/builder/', headers=headers
        ).get_response(app).status_int)

    def test_options_and_healthcheck_bypass_auth(self):
        app = RingManagerAuthMiddleware(
            self.app, {}, logger=debug_logger())
        self.assertEqual(200, Request.blank(
            '/api/v1/rings/', method='OPTIONS').get_response(app).status_int)
        self.assertEqual(200, Request.blank(
            '/healthcheck').get_response(app).status_int)


if __name__ == '__main__':
    unittest.main()
