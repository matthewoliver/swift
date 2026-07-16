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

import hashlib
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from urllib.parse import quote

from swift.common.ring.builder import RingBuilder
from swift.common.swob import Request, Response
from swift.common.utils import md5
from swift.ring_manager import routing
from swift.ring_manager.builder import DEFAULT_MAX_EXPLICIT_DEVICE_ID
from swift.ring_manager.builder_daemon import RingBuildWorker, \
    RingManagerBuilder
from swift.ring_manager.common import DEFAULT_BUILDER_LOCK_TIMEOUT, \
    DEFAULT_RING_ARTIFACT_DIR, DEFAULT_RING_BUILD_EXECUTOR, \
    DEFAULT_RING_BUILDER_DIR, DEFAULT_RING_MANAGER_STATE_DIR, \
    NormalTimestamp
from swift.ring_manager.server import app_factory, \
    DEFAULT_MAX_PARTITIONS_AT_RISK_SELECTORS, RingManagerApplication
from swift.ring_manager.middleware.auth import RingManagerAuthMiddleware
from swift.ring_manager.publisher import RingBuilderPublisherDeferred
from swift.ring_manager.store import RingManagerStore, RingNotFound
from test.debug_logger import debug_logger


class TestRingManagerApplication(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.state_dir = os.path.join(self.testdir, 'state')
        self.builder_path = os.path.join(self.testdir, 'object.builder')
        self._make_builder(self.builder_path)
        self.artifact_dir = os.path.join(self.testdir, 'artifacts')
        os.makedirs(self.artifact_dir)
        self.latest_version = '2026-05-22T10:31:00Z-abc123'
        self.artifact_name = 'object.ring.gz'
        self.artifact_body = b'new ring bytes'
        self.artifact_path = os.path.join(
            self.artifact_dir, self.artifact_name)
        with open(self.artifact_path, 'wb') as fp:
            fp.write(self.artifact_body)
        self.artifact_md5 = md5(
            self.artifact_body, usedforsecurity=False).hexdigest()
        self.artifact_sha256 = hashlib.sha256(
            self.artifact_body).hexdigest()
        self.last_rebalance_time = NormalTimestamp(
            float(NormalTimestamp.now()) - 1800).internal
        self._write_json('index.json', {
            'latest_ring_version': self.latest_version,
        })
        self._write_json('rings/1.json', {
            'id': 1,
            'name': 'Account & Container',
            'cluster_id': 7,
            'cluster': '/api/v1/clusters/7/',
            'storage_policy_index': None,
            'policy_type': 'replication',
            'ever_pushed': True,
            'last_rebalance_time': self.last_rebalance_time,
            'builder_files': [self.builder_path],
        })
        self._write_json('rings/2.json', {
            'id': 2,
            'name': 'Policy-1',
            'cluster': '/api/v1/clusters/8/',
            'storage_policy_index': 1,
            'policy_type': 'erasure_coding',
        })
        self._write_json('releases/older-version/manifest.json', {
            'version': 'older-version',
            'cluster_id': 7,
            'state': 'superseded',
            'created_at': '2026-05-22T10:00:00Z',
            'files': [],
        })
        self._write_latest_release()
        self.logger = debug_logger()
        self.app = RingManagerApplication(
            {
                'ring_manager_state_dir': self.state_dir,
                'ring_artifact_dir': self.artifact_dir,
                'ring_builder_dir': self.testdir,
            }, logger=self.logger)

    def _write_latest_release(self, files=None):
        if files is None:
            files = [{
                'name': self.artifact_name,
                'path': self.artifact_name,
                'md5': self.artifact_md5,
                'sha256': self.artifact_sha256,
                'bytes': len(self.artifact_body),
                'storage_policy_index': 0,
            }]
        self._write_json(
            'releases/%s/manifest.json' % self.latest_version, {
                'version': self.latest_version,
                'cluster_id': 7,
                'state': 'approved',
                'created_at': '2026-05-22T10:31:00Z',
                'files': files,
            })

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

    def _make_large_device_id_builder(self, path, device_id=70000):
        builder = RingBuilder(4, 3, 1)
        for index, dev_id in enumerate((device_id, 1, 2)):
            builder.add_dev({
                'id': dev_id,
                'region': 1,
                'zone': index,
                'ip': '10.0.70.%d' % index,
                'port': 6000,
                'device': 'sd%d' % index,
                'replication_ip': '10.0.70.%d' % index,
                'replication_port': 6003,
                'weight': 100,
            })
        builder.rebalance(seed=1)
        builder.save(path)

    def _make_count_parts_edge_ring(self, ring_id, mutate_builder):
        builder_path = os.path.join(self.testdir, '%s.builder' % ring_id)
        self._make_builder(builder_path)
        builder = RingBuilder.load(builder_path)
        mutate_builder(builder)
        builder.save(builder_path)
        self._write_json('rings/%s.json' % ring_id, {
            'id': ring_id,
            'name': 'Count Parts Edge %s' % ring_id,
            'storage_policy_index': 0,
            'policy_type': 'replication',
            'builder_files': [builder_path],
        })

    def _make_object_ring(self, ring_id='object-0'):
        builder_path = os.path.join(self.testdir, '%s.builder' % ring_id)
        self._make_builder(builder_path)
        self._write_json('rings/%s.json' % ring_id, {
            'id': ring_id,
            'name': 'Policy %s' % ring_id,
            'ring_type': 'object',
            'storage_policy_index': 0,
            'policy_type': 'replication',
            'builder_files': [builder_path],
        })
        return builder_path

    def _make_publishable_object_ring(self, ring_id, policy_index):
        builder_path = os.path.join(self.testdir, '%s.builder' % ring_id)
        self._make_builder(builder_path)
        self._write_json('rings/%s.json' % ring_id, {
            'id': ring_id,
            'name': 'Policy %s' % policy_index,
            'ring_type': 'object',
            'storage_policy_index': policy_index,
            'policy_type': 'replication',
            'builder_files': [builder_path],
        })
        return builder_path

    def _disable_initial_rings(self):
        self.app.store.update_ring('1', {'disabled': True})
        self.app.store.update_ring('2', {'disabled': True})

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

    def _builder_snapshot_files(self):
        return [
            name for name in os.listdir(self.state_dir)
            if name.startswith('.ring-manager-builder-snapshot-')]

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

    def process_next_build(self):
        worker = RingBuildWorker(
            self.app.store, self.app.publisher, builder_id='test-builder',
            logger=debug_logger(), lease_refresh_interval=3600)
        return worker.process_job()

    def test_app_factory(self):
        app = app_factory({}, ring_manager_state_dir=self.state_dir)
        self.assertIsInstance(app, RingManagerApplication)
        self.assertEqual(self.state_dir, app.store.state_dir)

    def test_app_factory_uses_sample_config_defaults(self):
        app = app_factory({})
        self.assertEqual(DEFAULT_RING_MANAGER_STATE_DIR, app.store.state_dir)
        self.assertEqual(
            DEFAULT_RING_ARTIFACT_DIR, app.store.ring_artifact_dir)
        self.assertEqual(DEFAULT_RING_BUILDER_DIR, app.ring_builder_dir)
        self.assertEqual(DEFAULT_MAX_EXPLICIT_DEVICE_ID,
                         app.max_explicit_device_id)
        self.assertEqual(DEFAULT_MAX_PARTITIONS_AT_RISK_SELECTORS,
                         app.max_partitions_at_risk_selectors)
        self.assertEqual(DEFAULT_BUILDER_LOCK_TIMEOUT,
                         app.builder_lock_timeout)
        self.assertEqual(DEFAULT_RING_BUILD_EXECUTOR,
                         app.ring_build_executor)

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
        self.assertEqual('/api/v1/rings/builds/',
                         body['links']['ring_builds'])
        self.assertEqual('/api/v1/rings/releases/',
                         body['links']['ring_versions'])
        self.assertEqual('/api/v1/rings/releases/latest/',
                         body['links']['latest_ring_version'])

        resp, body = self.get_json('/api/v1/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('v1', body['api_version'])

    def test_status(self):
        resp, body = self.get_json('/api/v1/ring_manager/status/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('ok', body['status'])
        self.assertEqual('external', body['ring_build_executor'])
        self.assertEqual(0, body['ring_builds']['total'])

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
            ('^/api/v1/rings/builds/?$', ('GET',), 'ring_builds'),
            ('^/api/v1/rings/builds/(?P<build_id>[^/]+)/cancel/?$',
             ('POST',), 'ring_build_cancel'),
            ('^/api/v1/rings/builds/(?P<build_id>[^/]+)/retry/?$',
             ('POST',), 'ring_build_retry'),
            ('^/api/v1/rings/builds/(?P<build_id>[^/]+)/?$', ('GET',),
             'ring_build_detail'),
            ('^/api/v1/rings/releases/?$', ('GET', 'POST'),
             'ring_versions'),
            ('^/api/v1/rings/releases/latest/?$',
             ('GET',), 'latest_ring_version'),
            ('^/api/v1/rings/releases/latest/manifest/?$',
             ('GET',), 'latest_ring_version_manifest'),
            ('^/api/v1/rings/releases/latest/files/'
             '(?P<file_name>[^/]+)/?$',
             ('GET',), 'latest_ring_version_file'),
            ('^/api/v1/rings/releases/(?P<version>[^/]+)/?$',
             ('GET',), 'ring_version_detail'),
            ('^/api/v1/rings/releases/(?P<version>[^/]+)/manifest/?$',
             ('GET',), 'ring_version_manifest'),
            ('^/api/v1/rings/releases/(?P<version>[^/]+)/files/'
             '(?P<file_name>[^/]+)/?$',
             ('GET',), 'ring_version_file'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/versions/?$',
             ('GET', 'POST'), 'ring_artifact_versions'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/versions/latest/?$',
             ('GET',), 'latest_ring_artifact_version'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/versions/latest/'
             'files/(?P<file_name>[^/]+)/?$', ('GET',),
             'latest_ring_artifact_version_file'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/versions/'
             '(?P<version>[^/]+)/?$', ('GET',),
             'ring_artifact_version_detail'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/versions/'
             '(?P<version>[^/]+)/files/(?P<file_name>[^/]+)/?$',
             ('GET',), 'ring_artifact_version_file'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/?$',
             ('GET', 'PUT', 'PATCH', 'DELETE'), 'ring_detail'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/devices/?$',
             ('GET', 'PUT'), 'ring_devices'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/devices/add/?$',
             ('POST',), 'ring_devices_add'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/devices/remove/?$',
             ('POST',), 'ring_devices_remove'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/'
             'partition_power_increase/'
             '(?P<action>prepare|increase|cancel|finish)/?$',
             ('POST',), 'ring_partition_power_increase'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/builder/?$',
             ('GET',), 'ring_builder'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/builder/file/?$',
             ('GET',), 'ring_builder_file'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/parts/?$',
             ('GET',), 'ring_parts'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/rebalance/?$',
             ('GET',), 'ring_rebalance'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/dispersion/?$',
             ('GET',), 'ring_dispersion'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/at_risk/?$',
             ('GET',), 'ring_at_risk'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/count_parts/?$',
             ('GET',), 'ring_count_parts'),
            ('^/api/v1/rings/(?P<ring_id>[^/]+)/partitions_at_risk/?$',
             ('GET', 'POST'), 'ring_partitions_at_risk'),
        ], [
            (route.regex.pattern, route.methods, route.handler.__name__)
            for route in self.app.routes
        ])

    def test_partitions_at_risk_post_is_read_only(self):
        route = next(
            route for route in self.app.routes
            if route.handler.__name__ == 'ring_partitions_at_risk')
        self.assertEqual(('GET', 'HEAD', 'POST'), route.read_only_methods)

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

    def test_ring_versions(self):
        resp, body = self.get_json('/api/v1/rings/releases/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(2, body['meta']['total_count'])
        latest = next(
            item for item in body['objects']
            if item['version'] == self.latest_version)
        self.assertTrue(latest['latest'])
        self.assertEqual(
            '/api/v1/rings/releases/%s/' % quote(
                self.latest_version, safe=''),
            latest['resource_uri'])
        self.assertNotIn('path', latest['files'][0])
        self.assertNotIn(self.artifact_dir, json.dumps(latest))
        self.assertEqual(
            '/api/v1/rings/releases/%s/files/%s' % (
                quote(self.latest_version, safe=''), self.artifact_name),
            latest['files'][0]['url'])

    def test_publish_release_builds_ring_artifact(self):
        self._disable_initial_rings()
        builder_path = self._make_publishable_object_ring('object-0', 0)

        resp, body = self.json_request('/api/v1/rings/releases/', 'POST', {
            'version': 'release-demo',
            'rings': ['object-0'],
            'seed': '1',
        })
        self.assertEqual(202, resp.status_int)
        self.assertEqual('queued', body['state'])
        self.assertTrue(resp.headers['Location'].endswith(
            body['resource_uri']))
        build = self.process_next_build()
        self.assertEqual('completed', build['state'])
        body = build['result']
        self.assertEqual('release-demo', body['version'])
        self.assertTrue(body['latest'])
        self.assertEqual([{
            'ring_id': 'object-0',
            'swift_ring_version': body['rings'][0]['swift_ring_version'],
        }], body['rings'])
        self.assertTrue(os.path.exists(os.path.join(
            self.artifact_dir, 'release-demo', 'object.ring.gz')))
        self.assertTrue(os.path.exists(builder_path))

        resp, artifact = self.get_json(
            '/api/v1/rings/object-0/versions/latest/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('object-0', artifact['ring_id'])
        self.assertTrue(artifact['latest'])
        self.assertEqual('object.ring.gz', artifact['files'][0]['name'])

        resp, body = self.json_request('/api/v1/rings/releases/', 'POST', {
            'version': 'release-demo',
            'rings': ['object-0'],
            'force': True,
        })
        self.assertEqual(400, resp.status_int)
        self.assertIn('already exists', body['error'])

    def test_publish_release_carries_enabled_rings_forward(self):
        self._disable_initial_rings()
        self._make_publishable_object_ring('object-0', 0)
        self._make_publishable_object_ring('object-1', 1)

        resp, body = self.json_request('/api/v1/rings/releases/', 'POST', {
            'version': 'release-one',
            'rings': ['object-0', 'object-1'],
            'seed': '1',
        })
        self.assertEqual(202, resp.status_int)
        first = self.process_next_build()
        self.assertEqual('completed', first['state'])
        body = first['result']
        first_versions = dict(
            (ring['ring_id'], ring['swift_ring_version'])
            for ring in body['rings'])

        resp, body = self.json_request('/api/v1/rings/releases/', 'POST', {
            'version': 'release-two',
            'rings': ['object-0'],
            'seed': '2',
        })
        self.assertEqual(202, resp.status_int)
        second = self.process_next_build()
        self.assertEqual('completed', second['state'])
        body = second['result']
        self.assertEqual(['object-0', 'object-1'], [
            ring['ring_id'] for ring in body['rings']])
        second_versions = dict(
            (ring['ring_id'], ring['swift_ring_version'])
            for ring in body['rings'])
        self.assertEqual(first_versions['object-1'],
                         second_versions['object-1'])

    def test_publish_rejects_disabled_and_missing_rings_before_building(self):
        self._disable_initial_rings()
        builder_path = self._make_publishable_object_ring('object-0', 0)
        before = RingBuilder.load(builder_path).version
        self.app.store.update_ring('object-0', {'disabled': True})

        resp, body = self.json_request('/api/v1/rings/releases/', 'POST', {
            'version': 'release-disabled',
            'rings': ['object-0'],
        })
        self.assertEqual(400, resp.status_int)
        self.assertIn('object-0 is disabled', body['error'])

        resp, body = self.json_request('/api/v1/rings/releases/', 'POST', {
            'version': 'release-missing',
            'rings': ['missing-ring'],
        })
        self.assertEqual(404, resp.status_int)
        self.assertIsNone(body)
        self.assertEqual(before, RingBuilder.load(builder_path).version)
        self.assertFalse(self.app.store.ring_version_exists('release-missing'))

    def test_artifact_only_build_allows_disabled_ring(self):
        self._disable_initial_rings()
        self._make_publishable_object_ring('object-0', 0)
        self.app.store.update_ring('object-0', {'disabled': True})

        resp, body = self.json_request(
            '/api/v1/rings/object-0/versions/', 'POST', {'seed': '1'})
        self.assertEqual(202, resp.status_int)
        build = self.process_next_build()
        self.assertEqual('completed', build['state'])
        body = build['result']
        self.assertEqual('object-0', body['ring_id'])
        self.assertTrue(body['latest'])
        self.assertEqual('object.ring.gz', body['files'][0]['name'])
        self.assertEqual(self.latest_version,
                         self.app.store.get_latest_ring_version_id())

    def test_ring_builds_are_fifo_and_exclude_duplicate_versions(self):
        self._disable_initial_rings()
        self._make_publishable_object_ring('object-0', 0)

        resp, first = self.json_request('/api/v1/rings/releases/', 'POST', {
            'version': 'release-queued',
            'rings': ['object-0'],
        })
        self.assertEqual(202, resp.status_int)
        self.assertEqual(1, first['sequence'])
        self.assertTrue(first['id'].startswith('0000000000000001-'))

        resp, body = self.json_request('/api/v1/rings/releases/', 'POST', {
            'version': 'release-queued',
            'rings': ['object-0'],
        })
        self.assertEqual(409, resp.status_int)
        self.assertIn('active ring build', body['error'])

        resp, body = self.get_json('/api/v1/rings/builds/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(
            [first['id']], [item['id'] for item in body['objects']])

    def test_ring_build_cancel_is_narrow_and_idempotent(self):
        first = self.app.store.create_ring_build({
            'version': 'release-queued',
            'rings': ['1'],
        }, NormalTimestamp(1000).internal)
        second = self.app.store.create_ring_build({
            'version': 'release-next',
            'rings': ['1'],
        }, NormalTimestamp(1001).internal)

        resp, body = self.json_request(
            '/api/v1/rings/builds/%s/cancel/' % first['id'], 'POST', {
                'reason': 'superseded',
            })
        self.assertEqual(200, resp.status_int)
        self.assertEqual('cancelled', body['state'])
        self.assertEqual('queued', body['cancelled_from'])
        self.assertEqual('superseded', body['cancel_reason'])

        resp, body = self.json_request(
            '/api/v1/rings/builds/%s/cancel/' % first['id'], 'POST', {
                'reason': 'ignored retry',
            })
        self.assertEqual(200, resp.status_int)
        self.assertEqual('superseded', body['cancel_reason'])

        claimed = self.app.store.claim_ring_build(
            'builder-a', NormalTimestamp(1002).internal)
        self.assertEqual(second['id'], claimed['id'])
        resp, body = self.json_request(
            '/api/v1/rings/builds/%s/cancel/' % second['id'], 'POST', {})
        self.assertEqual(409, resp.status_int)
        self.assertIn('cannot be cancelled', body['error'])

    def test_ring_build_cancel_deferred_job_unblocks_queue(self):
        first = self.app.store.create_ring_build({
            'version': 'release-deferred',
            'rings': ['1'],
        }, NormalTimestamp(1000).internal)
        second = self.app.store.create_ring_build({
            'version': 'release-next',
            'rings': ['1'],
        }, NormalTimestamp(1001).internal)
        self.app.store._update_ring_build(first['id'], {
            'state': 'deferred',
            'deferred_until': NormalTimestamp(2000).internal,
        }, NormalTimestamp(1002).internal)

        resp, body = self.json_request(
            '/api/v1/rings/builds/%s/cancel/' % first['id'], 'POST', {})
        self.assertEqual(200, resp.status_int)
        self.assertEqual('deferred', body['cancelled_from'])
        claimed = self.app.store.claim_ring_build(
            'builder-a', NormalTimestamp(1003).internal)
        self.assertEqual(second['id'], claimed['id'])

    def test_ring_build_retry_appends_immutable_lineage(self):
        source = self.app.store.create_ring_build({
            'version': 'release-retry',
            'rings': ['1'],
        }, NormalTimestamp(1000).internal)
        self.app.store._update_ring_build(source['id'], {
            'state': 'failed',
            'completed_at': NormalTimestamp(1001).internal,
            'error': 'temporary builder failure',
        }, NormalTimestamp(1001).internal)

        resp, retry = self.json_request(
            '/api/v1/rings/builds/%s/retry/' % source['id'], 'POST', {
                'reason': 'operator retry',
            })
        self.assertEqual(202, resp.status_int)
        self.assertNotEqual(source['id'], retry['id'])
        self.assertEqual(2, retry['sequence'])
        self.assertEqual('queued', retry['state'])
        self.assertEqual(source['id'], retry['retry_of'])
        self.assertEqual(source['id'], retry['retry_root'])
        self.assertEqual(1, retry['retry_count'])
        self.assertEqual('failed', retry['retry_source_state'])
        self.assertEqual('temporary builder failure',
                         retry['retry_source_error'])
        self.assertEqual('operator retry', retry['retry_reason'])
        self.assertTrue(resp.headers['Location'].endswith(
            retry['resource_uri']))
        self.assertEqual('failed', self.app.store.get_ring_build(
            source['id'])['state'])

        self.app.store._update_ring_build(retry['id'], {
            'state': 'cancelled',
        }, NormalTimestamp(1002).internal)
        resp, child = self.json_request(
            '/api/v1/rings/builds/%s/retry/' % retry['id'], 'POST', {})
        self.assertEqual(202, resp.status_int)
        self.assertEqual(retry['id'], child['retry_of'])
        self.assertEqual(source['id'], child['retry_root'])
        self.assertEqual(2, child['retry_count'])

    def test_ring_build_retry_rejects_invalid_source_state(self):
        job = self.app.store.create_ring_build({
            'version': 'release-queued',
            'rings': ['1'],
        }, NormalTimestamp(1000).internal)
        resp, body = self.json_request(
            '/api/v1/rings/builds/%s/retry/' % job['id'], 'POST', {})
        self.assertEqual(409, resp.status_int)
        self.assertIn('cannot be retried', body['error'])

    def test_ring_build_retry_reuses_enqueue_admission(self):
        source = self.app.store.create_ring_build({
            'version': 'release-retry',
            'rings': ['1'],
        }, NormalTimestamp(1000).internal)
        self.app.store._update_ring_build(source['id'], {
            'state': 'failed',
        }, NormalTimestamp(1001).internal)
        self.app.store.create_ring_build({
            'version': 'release-retry',
            'rings': ['1'],
        }, NormalTimestamp(1002).internal)

        resp, body = self.json_request(
            '/api/v1/rings/builds/%s/retry/' % source['id'], 'POST', {})
        self.assertEqual(409, resp.status_int)
        self.assertIn('active ring build', body['error'])

    def test_ring_build_actions_wake_manager_executor(self):
        app = RingManagerApplication({
            'ring_manager_state_dir': self.state_dir,
            'ring_artifact_dir': self.artifact_dir,
            'ring_builder_dir': self.testdir,
            'ring_build_executor': 'manager',
        }, logger=debug_logger())
        queued = app.store.create_ring_build({
            'version': 'release-queued',
            'rings': ['1'],
        }, NormalTimestamp(1000).internal)
        with mock.patch.object(app.build_pool, 'spawn_n') as spawn:
            resp, body = self.json_request(
                '/api/v1/rings/builds/%s/cancel/' % queued['id'], 'POST',
                {}, app=app)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('cancelled', body['state'])
        spawn.assert_called_once_with(app.build_worker.process_jobs)

        app.store._update_ring_build(queued['id'], {
            'state': 'failed',
        }, NormalTimestamp(1001).internal)
        with mock.patch.object(app.build_pool, 'spawn_n') as spawn:
            resp, body = self.json_request(
                '/api/v1/rings/builds/%s/retry/' % queued['id'], 'POST',
                {}, app=app)
        self.assertEqual(202, resp.status_int)
        self.assertEqual('queued', body['state'])
        spawn.assert_called_once_with(app.build_worker.process_jobs)

    def test_ring_builds_filter_retry_lineage(self):
        self._write_json('ring_builds/source.json', {
            'id': 'source',
            'sequence': 1,
            'state': 'failed',
        })
        self._write_json('ring_builds/direct-child.json', {
            'id': 'direct-child',
            'sequence': 2,
            'state': 'failed',
            'retry_of': 'source',
            'retry_root': 'source',
        })
        self._write_json('ring_builds/grandchild.json', {
            'id': 'grandchild',
            'sequence': 3,
            'state': 'queued',
            'retry_of': 'direct-child',
            'retry_root': 'source',
        })

        resp, body = self.get_json(
            '/api/v1/rings/builds/?retry_of=source')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(['direct-child'], [
            build['id'] for build in body['objects']])
        resp, body = self.get_json(
            '/api/v1/rings/builds/?retry_root=source')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(['direct-child', 'grandchild'], [
            build['id'] for build in body['objects']])
        resp, body = self.get_json('/api/v1/rings/builds/?retry_of=')
        self.assertEqual(400, resp.status_int)
        self.assertIn('retry_of must not be empty', body['error'])

    def test_deferred_job_allows_disjoint_ring_claim(self):
        first = self.app.store.create_ring_build({
            'rings': ['object-0'],
        }, NormalTimestamp(1000).internal)
        second = self.app.store.create_ring_build({
            'rings': ['object-1'],
        }, NormalTimestamp(1001).internal)
        self.app.store._update_ring_build(first['id'], {
            'state': 'deferred',
            'deferred_until': NormalTimestamp(2000).internal,
        }, NormalTimestamp(1002).internal)

        claimed = self.app.store.claim_ring_build(
            'builder-b', NormalTimestamp(1003).internal)
        self.assertEqual(second['id'], claimed['id'])

    def test_claim_leases_require_current_owner(self):
        job = self.app.store.create_ring_build({
            'rings': ['object-0'],
        }, NormalTimestamp(1000).internal)
        claimed = self.app.store.claim_ring_build(
            'builder-a', NormalTimestamp(1000).internal, lease_timeout=5)
        self.assertEqual(job['id'], claimed['id'])
        self.assertIsNone(self.app.store.refresh_ring_build_lease(
            job['id'], 'builder-a', 'wrong-claim',
            NormalTimestamp(1001).internal, lease_timeout=5))
        self.assertIsNone(self.app.store.update_claimed_ring_build(
            job['id'], 'builder-a', 'wrong-claim', {'state': 'completed'},
            NormalTimestamp(1001).internal))
        refreshed = self.app.store.refresh_ring_build_lease(
            job['id'], 'builder-a', claimed['claimed_at'],
            NormalTimestamp(1001).internal, lease_timeout=5)
        self.assertEqual(NormalTimestamp(1006).internal,
                         refreshed['lease_expires_at'])

        recovered = self.app.store.claim_ring_build(
            'builder-b', NormalTimestamp(1006).internal, lease_timeout=5)
        self.assertEqual(job['id'], recovered['id'])
        self.assertEqual(2, recovered['attempts'])

    def test_old_persisted_build_fails_closed(self):
        self._write_json('ring_builds/old-build.json', {
            'id': 'old-build',
            'state': 'queued',
        })
        self.assertIsNone(self.app.store.claim_ring_build(
            'builder-a', NormalTimestamp(1000).internal))
        build = self.app.store.get_ring_build('old-build')
        self.assertEqual('failed', build['state'])
        self.assertIn('lacks a durable request', build['error'])

    def test_worker_defers_and_preserves_fifo_scope(self):
        job = self.app.store.create_ring_build({
            'rings': ['object-0'],
        }, NormalTimestamp(1000).internal)
        publisher = mock.Mock()
        publisher.publish.side_effect = RingBuilderPublisherDeferred(
            'wait for min_part_hours', reason='min_part_hours',
            retry_after=60, ring_id='object-0')
        worker = RingBuildWorker(
            self.app.store, publisher, builder_id='test-builder',
            time_func=lambda: NormalTimestamp(1000),
            lease_refresh_interval=3600)

        deferred = worker.process_job()
        self.assertEqual('deferred', deferred['state'])
        self.assertEqual('min_part_hours', deferred['defer_reason'])
        self.assertEqual('object-0', deferred['deferred_ring_id'])
        self.assertIsNone(self.app.store.claim_ring_build(
            'other-builder', NormalTimestamp(1001).internal))
        self.assertEqual(job['id'], deferred['id'])

    def test_builder_daemon_marks_bad_job_failed(self):
        job = self.app.store.create_ring_build({
            'version': 'release-missing',
            'rings': ['missing'],
        }, NormalTimestamp(1000).internal)
        builder = RingManagerBuilder({
            'ring_manager_state_dir': self.state_dir,
            'ring_artifact_dir': self.artifact_dir,
            'ring_builder_dir': self.testdir,
        })
        self.assertEqual(1, builder.run_once())

        build = self.app.store.get_ring_build(job['id'])
        self.assertEqual('failed', build['state'])
        self.assertIn('missing', build['error'])

    def test_release_endpoint_rejects_artifact_only_request(self):
        resp, body = self.json_request('/api/v1/rings/releases/', 'POST', {
            'artifact_only': True,
            'ring_id': '1',
        })
        self.assertEqual(400, resp.status_int)
        self.assertIn('/api/v1/rings/<ring_id>/versions/', body['error'])

    def test_ring_versions_cluster_filter(self):
        resp, body = self.get_json(
            '/api/v1/rings/releases/?cluster_id=8')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(0, body['meta']['total_count'])

    def test_ring_versions_validate_manifest_identity(self):
        self._write_json(
            'releases/%s/manifest.json' % self.latest_version, {
                'version': 'latest',
                'files': [],
            })
        resp, body = self.get_json('/api/v1/rings/releases/')
        self.assertEqual(400, resp.status_int)
        self.assertIn('reserved', body['error'])

    def test_ring_versions_reject_state_path_mismatch(self):
        self._write_json(
            'releases/%s/manifest.json' % self.latest_version, {
                'version': 'different-version',
                'files': [],
            })
        resp, body = self.get_json('/api/v1/rings/releases/')
        self.assertEqual(400, resp.status_int)
        self.assertIn('does not match state path', body['error'])

    def test_ring_versions_validate_file_identity(self):
        self._write_latest_release(files=[{
            'name': '../object.ring.gz',
            'path': self.artifact_name,
        }])
        resp, body = self.get_json('/api/v1/rings/releases/')
        self.assertEqual(400, resp.status_int)
        self.assertIn('path separators', body['error'])

    def test_ring_version_detail_and_manifest(self):
        version_path = '/api/v1/rings/releases/%s/' % quote(
            self.latest_version, safe='')
        resp, body = self.get_json(version_path)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(self.latest_version, body['version'])
        self.assertTrue(body['latest'])
        self.assertNotIn('path', body['files'][0])

        resp, body = self.get_json(version_path + 'manifest/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(self.latest_version, body['version'])
        self.assertTrue(body['latest'])
        self.assertEqual('approved', body['state'])
        self.assertEqual(7, body['cluster_id'])
        self.assertEqual(self.artifact_md5, body['files'][0]['md5'])
        self.assertEqual(self.artifact_sha256, body['files'][0]['sha256'])
        self.assertNotIn('path', body['files'][0])

    def test_latest_ring_version_resources(self):
        resp, body = self.get_json('/api/v1/rings/releases/latest/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(self.latest_version, body['version'])
        self.assertTrue(body['latest'])

        resp, body = self.get_json(
            '/api/v1/rings/releases/latest/manifest/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(self.latest_version, body['version'])
        self.assertTrue(body['latest'])

        req = Request.blank(
            '/api/v1/rings/releases/latest/files/%s' % self.artifact_name)
        resp = req.get_response(self.app)
        self.assertEqual(307, resp.status_int)
        self.assertTrue(resp.headers['Location'].endswith(
            '/api/v1/rings/releases/%s/files/%s' % (
                quote(self.latest_version, safe=''), self.artifact_name)))

    def test_ring_version_file_streams_with_validators(self):
        path = '/api/v1/rings/releases/%s/files/%s' % (
            quote(self.latest_version, safe=''), self.artifact_name)
        resp = Request.blank(path).get_response(self.app)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(self.artifact_body, resp.body)
        self.assertEqual(self.artifact_md5, resp.headers['Etag'])
        self.assertEqual(
            self.artifact_sha256, resp.headers['X-Checksum-Sha256'])
        self.assertEqual(len(self.artifact_body), resp.content_length)

        resp = Request.blank(path, method='HEAD').get_response(self.app)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(b'', resp.body)
        self.assertEqual(len(self.artifact_body), resp.content_length)

        resp = Request.blank(
            path, headers={'If-None-Match': self.artifact_md5}
        ).get_response(self.app)
        self.assertEqual(304, resp.status_int)
        self.assertEqual(b'', resp.body)

        resp = Request.blank(
            path, headers={'Range': 'bytes=4-7'}
        ).get_response(self.app)
        self.assertEqual(206, resp.status_int)
        self.assertEqual(self.artifact_body[4:8], resp.body)
        self.assertEqual(
            'bytes 4-7/%d' % len(self.artifact_body),
            resp.headers['Content-Range'])

        resp = Request.blank(
            path, headers={'Range': 'bytes=0-2,5-7'}
        ).get_response(self.app)
        self.assertEqual(206, resp.status_int)
        self.assertIn(self.artifact_body[0:3], resp.body)
        self.assertIn(self.artifact_body[5:8], resp.body)

    def test_ring_version_file_uses_streaming_iterable(self):
        calls = []
        artifact_body = self.artifact_body

        class FakeFileIterable(object):
            def __init__(self, path, **kwargs):
                calls.append((path, kwargs))

            def __iter__(self):
                yield artifact_body

            def close(self):
                pass

        original_factory = self.app.ring_controller._file_iterable_factory
        self.app.ring_controller._file_iterable_factory = FakeFileIterable
        try:
            req = Request.blank(
                '/api/v1/rings/releases/%s/files/%s' % (
                    quote(self.latest_version, safe=''), self.artifact_name))
            resp = req.get_response(self.app)
            self.assertEqual(self.artifact_body, resp.body)
        finally:
            self.app.ring_controller._file_iterable_factory = \
                original_factory
        self.assertEqual([(self.artifact_path, {})], calls)

    def test_ring_version_file_without_md5_omits_etag(self):
        self._write_latest_release(files=[{
            'name': self.artifact_name,
            'path': self.artifact_name,
            'sha256': self.artifact_sha256,
            'bytes': len(self.artifact_body),
        }])
        req = Request.blank(
            '/api/v1/rings/releases/%s/files/%s' % (
                quote(self.latest_version, safe=''), self.artifact_name),
            headers={'If-None-Match': self.artifact_sha256})
        resp = req.get_response(self.app)
        self.assertEqual(200, resp.status_int)
        self.assertNotIn('Etag', resp.headers)
        self.assertEqual(
            self.artifact_sha256, resp.headers['X-Checksum-Sha256'])

    def test_ring_version_file_rejects_artifact_path_escape(self):
        outside_path = os.path.join(self.testdir, 'outside.ring.gz')
        with open(outside_path, 'wb') as fp:
            fp.write(self.artifact_body)
        self._write_latest_release(files=[{
            'name': self.artifact_name,
            'path': '../outside.ring.gz',
            'md5': self.artifact_md5,
            'sha256': self.artifact_sha256,
            'bytes': len(self.artifact_body),
        }])
        req = Request.blank(
            '/api/v1/rings/releases/%s/files/%s' % (
                quote(self.latest_version, safe=''), self.artifact_name))
        self.assertEqual(404, req.get_response(self.app).status_int)

    def test_ring_version_file_rejects_symlink_escape(self):
        outside_path = os.path.join(self.testdir, 'outside.ring.gz')
        with open(outside_path, 'wb') as fp:
            fp.write(self.artifact_body)
        symlink_path = os.path.join(self.artifact_dir, 'linked.ring.gz')
        os.symlink(outside_path, symlink_path)
        self._write_latest_release(files=[{
            'name': self.artifact_name,
            'path': 'linked.ring.gz',
            'md5': self.artifact_md5,
            'sha256': self.artifact_sha256,
            'bytes': len(self.artifact_body),
        }])
        req = Request.blank(
            '/api/v1/rings/releases/%s/files/%s' % (
                quote(self.latest_version, safe=''), self.artifact_name))
        self.assertEqual(404, req.get_response(self.app).status_int)

    def test_ring_version_file_missing_resources(self):
        resp, body = self.get_json('/api/v1/rings/releases/nope/')
        self.assertEqual(404, resp.status_int)
        self.assertIsNone(body)
        resp, body = self.get_json(
            '/api/v1/rings/releases/nope/manifest/')
        self.assertEqual(404, resp.status_int)
        self.assertIsNone(body)

        path = '/api/v1/rings/releases/%s/files/%s' % (
            quote(self.latest_version, safe=''), self.artifact_name)
        os.unlink(self.artifact_path)
        self.assertEqual(
            404, Request.blank(path).get_response(self.app).status_int)
        self.assertEqual(
            404, Request.blank(
                path, method='HEAD').get_response(self.app).status_int)
        self.assertEqual(
            404, Request.blank(
                path, headers={'If-None-Match': self.artifact_md5}
            ).get_response(self.app).status_int)

    def test_ring_builder_metadata_and_file(self):
        with open(self.builder_path, 'rb') as fp:
            builder_body = fp.read()
        builder_md5 = md5(
            builder_body, usedforsecurity=False).hexdigest()
        builder_sha256 = hashlib.sha256(builder_body).hexdigest()
        builder_version = RingBuilder.load(self.builder_path).version

        resp, body = self.get_json('/api/v1/rings/1/builder/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('1', str(body['ring_id']))
        self.assertEqual(builder_version, body['builder_version'])
        self.assertEqual({
            'bytes': len(builder_body),
            'md5': builder_md5,
            'sha256': builder_sha256,
            'url': '/api/v1/rings/1/builder/file/',
        }, body['file'])
        self.assertNotIn(self.testdir, json.dumps(body))

        path = '/api/v1/rings/1/builder/file/'
        resp = Request.blank(path).get_response(self.app)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(builder_body, resp.body)
        self.assertEqual(builder_md5, resp.headers['Etag'])
        self.assertEqual(builder_sha256,
                         resp.headers['X-Checksum-Sha256'])
        self.assertEqual(str(builder_version),
                         resp.headers['X-Ring-Builder-Version'])
        self.assertEqual([], self._builder_snapshot_files())

        resp = Request.blank(path, method='HEAD').get_response(self.app)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(b'', resp.body)
        self.assertEqual(len(builder_body), resp.content_length)
        self.assertEqual([], self._builder_snapshot_files())

        resp = Request.blank(
            path, headers={'If-None-Match': builder_md5}
        ).get_response(self.app)
        self.assertEqual(304, resp.status_int)
        self.assertEqual(b'', resp.body)
        self.assertEqual([], self._builder_snapshot_files())

        resp = Request.blank(
            path, headers={'Range': 'bytes=0-9'}
        ).get_response(self.app)
        self.assertEqual(206, resp.status_int)
        self.assertEqual(builder_body[:10], resp.body)
        self.assertEqual([], self._builder_snapshot_files())

    def test_ring_builder_download_uses_stable_snapshot(self):
        with open(self.builder_path, 'rb') as fp:
            builder_body = fp.read()
        ring = self.app.store.get_ring('1')
        record = self.app.ring_controller._builder_file_snapshot_record(ring)
        snapshot_path = record['path']
        try:
            with open(self.builder_path, 'wb') as fp:
                fp.write(b'changed builder bytes')
            req = Request.blank('/api/v1/rings/1/builder/file/')
            resp = self.app.ring_controller._builder_file_response(
                req, record)
            self.assertEqual(builder_body, resp.body)
            self.assertFalse(os.path.exists(snapshot_path))
        finally:
            if os.path.exists(snapshot_path):
                os.unlink(snapshot_path)
            with open(self.builder_path, 'wb') as fp:
                fp.write(builder_body)

    def test_ring_builder_rejects_path_outside_builder_dir(self):
        app = RingManagerApplication({
            'ring_manager_state_dir': self.state_dir,
            'ring_artifact_dir': self.artifact_dir,
            'ring_builder_dir': os.path.join(self.testdir, 'builders'),
        }, logger=debug_logger())
        with mock.patch(
                'swift.ring_manager.controllers.ring.RingBuilder.load') \
                as load:
            resp, body = self.get_json(
                '/api/v1/rings/1/builder/', app=app)
        self.assertEqual(400, resp.status_int)
        self.assertIn('unavailable', body['error'])
        self.assertNotIn(self.builder_path, body['error'])
        load.assert_not_called()

    def test_ring_schema(self):
        resp, body = self.get_json('/api/v1/rings/schema/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(['get', 'put', 'patch', 'delete'],
                         body['allowed_detail_http_methods'])
        self.assertEqual(['get', 'post'], body['allowed_list_http_methods'])
        for field in ('id', 'name', 'cluster', 'policy_type',
                      'storage_policy_index', 'resource_uri',
                      'next_part_power',
                      'partition_power_increase_state',
                      'allowed_partition_power_actions'):
            self.assertIn(field, body['fields'])
        self.assertEqual(
            True,
            body['fields']['partition_power_increase_state']['readonly'])

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
        self.assertIsNone(body['next_part_power'])
        self.assertEqual('idle', body['partition_power_increase_state'])
        self.assertEqual([], body['allowed_partition_power_actions'])
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

    def test_ring_create_rejects_partition_power_state(self):
        resp, body = self.json_request('/api/v1/rings/', 'POST', {
            'name': 'Policy-2',
            'storage_policy_index': 2,
            'next_part_power': 5,
        })
        self.assertEqual(400, resp.status_int)
        self.assertIn('Read-only ring fields', body['error'])
        self.assertFalse(os.path.exists(os.path.join(
            self.state_dir, 'rings', 'object-2.json')))

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

    def test_ring_update_rejects_partition_power_state_updates(self):
        cases = (
            {'next_part_power': 5},
            {'partition_power_increase_state': 'prepared'},
            {'allowed_partition_power_actions': ['finish']},
        )
        for method in ('PATCH', 'PUT'):
            for payload in cases:
                before = RingBuilder.load(self.builder_path)
                before_values = (
                    before.version, before.part_power,
                    before.next_part_power)

                resp, body = self.json_request(
                    '/api/v1/rings/1/', method, payload)
                self.assertEqual(400, resp.status_int)
                self.assertIn('Read-only ring fields', body['error'])

                after = RingBuilder.load(self.builder_path)
                self.assertEqual(before_values, (
                    after.version, after.part_power,
                    after.next_part_power))

    def test_ring_detail_reports_partition_power_increase_state(self):
        self._make_object_ring()

        resp, body = self.get_json('/api/v1/rings/object-0/')
        self.assertEqual(200, resp.status_int)
        self.assertIsNone(body['next_part_power'])
        self.assertEqual('idle', body['partition_power_increase_state'])
        self.assertEqual(
            ['prepare'], body['allowed_partition_power_actions'])

    def test_ring_partition_power_increase_lifecycle(self):
        builder_path = self._make_object_ring()
        before = RingBuilder.load(builder_path)

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/prepare/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        self.assertEqual('prepare', body['action'])
        self.assertEqual(True, body['requires_publish'])
        self.assertEqual(4, body['part_power'])
        self.assertEqual(5, body['next_part_power'])
        self.assertEqual(
            'prepared', body['partition_power_increase_state'])
        self.assertEqual(
            ['increase', 'cancel'],
            body['allowed_partition_power_actions'])
        prepared = RingBuilder.load(builder_path)
        self.assertEqual(before.version + 1, prepared.version)
        self.assertEqual(4, prepared.part_power)
        self.assertEqual(5, prepared.next_part_power)

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/increase/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        self.assertEqual('increase', body['action'])
        self.assertEqual(5, body['part_power'])
        self.assertEqual(5, body['next_part_power'])
        self.assertEqual(
            'cleanup_pending', body['partition_power_increase_state'])
        self.assertEqual(
            ['finish'], body['allowed_partition_power_actions'])
        increased = RingBuilder.load(builder_path)
        self.assertEqual(prepared.version + 1, increased.version)
        self.assertEqual(5, increased.part_power)
        self.assertEqual(5, increased.next_part_power)

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/finish/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        self.assertEqual('finish', body['action'])
        self.assertEqual(5, body['part_power'])
        self.assertIsNone(body['next_part_power'])
        self.assertEqual('idle', body['partition_power_increase_state'])
        self.assertEqual(
            ['prepare'], body['allowed_partition_power_actions'])
        finished = RingBuilder.load(builder_path)
        self.assertEqual(increased.version + 1, finished.version)
        self.assertIsNone(finished.next_part_power)

    def test_ring_partition_power_increase_cancel_lifecycle(self):
        builder_path = self._make_object_ring()

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/prepare/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        prepared = RingBuilder.load(builder_path)

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/cancel/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        self.assertEqual('cancel', body['action'])
        self.assertEqual(4, body['part_power'])
        self.assertEqual(4, body['next_part_power'])
        self.assertEqual(
            'cleanup_pending', body['partition_power_increase_state'])
        self.assertEqual(
            ['finish'], body['allowed_partition_power_actions'])
        cancelled = RingBuilder.load(builder_path)
        self.assertEqual(prepared.version + 1, cancelled.version)
        self.assertEqual(4, cancelled.part_power)
        self.assertEqual(4, cancelled.next_part_power)

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/finish/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        self.assertEqual('idle', body['partition_power_increase_state'])
        self.assertIsNone(body['next_part_power'])

    def test_ring_partition_power_rejects_invalid_transition(self):
        builder_path = self._make_object_ring()
        before = RingBuilder.load(builder_path)

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/increase/',
            'POST', {})
        self.assertEqual(409, resp.status_int)
        self.assertIn('cannot increase from state idle', body['error'])

        after = RingBuilder.load(builder_path)
        self.assertEqual(before.version, after.version)
        self.assertIsNone(after.next_part_power)

    def test_ring_partition_power_rejects_non_object_ring(self):
        resp, body = self.json_request(
            '/api/v1/rings/1/partition_power_increase/prepare/',
            'POST', {})
        self.assertEqual(400, resp.status_int)
        self.assertIn('only supported for object rings', body['error'])

    def test_ring_partition_power_rejects_active_build(self):
        builder_path = self._make_object_ring()
        before = RingBuilder.load(builder_path)
        self._write_json('ring_builds/0001-build.json', {
            'id': 'build-1',
            'sequence': 1,
            'state': 'queued',
            'artifact_only': True,
            'ring_id': 'object-0',
        })

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/prepare/',
            'POST', {})
        self.assertEqual(409, resp.status_int)
        self.assertIn('active ring build build-1', body['error'])

        after = RingBuilder.load(builder_path)
        self.assertEqual(before.version, after.version)
        self.assertIsNone(after.next_part_power)

    def test_ring_partition_power_rejects_active_cluster_build(self):
        self._make_object_ring()
        self._write_json('ring_builds/0001-build.json', {
            'id': 'build-1',
            'sequence': 1,
            'state': 'deferred',
            'rings': None,
        })

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/prepare/',
            'POST', {})
        self.assertEqual(409, resp.status_int)
        self.assertIn('active ring build build-1', body['error'])

    def test_ring_partition_power_ignores_inactive_builds(self):
        self._make_object_ring()
        self._write_json('ring_builds/0001-build.json', {
            'id': 'build-1',
            'sequence': 1,
            'state': 'completed',
            'artifact_only': True,
            'ring_id': 'object-0',
        })
        self._write_json('ring_builds/0002-build.json', {
            'id': 'build-2',
            'sequence': 2,
            'state': 'queued',
            'artifact_only': True,
            'ring_id': 'object-1',
        })

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/prepare/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        self.assertEqual('prepared',
                         body['partition_power_increase_state'])

    def test_ring_partition_power_recovery_allows_active_build(self):
        builder_path = self._make_object_ring()
        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/prepare/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        self._write_json('ring_builds/0001-build.json', {
            'id': 'build-1',
            'sequence': 1,
            'state': 'building',
            'artifact_only': True,
            'ring_id': 'object-0',
        })

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/increase/',
            'POST', {})
        self.assertEqual(409, resp.status_int)
        self.assertIn('active ring build', body['error'])

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/cancel/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        self.assertEqual(
            'cleanup_pending', body['partition_power_increase_state'])
        cancelled = RingBuilder.load(builder_path)
        self.assertEqual(4, cancelled.next_part_power)

        resp, body = self.json_request(
            '/api/v1/rings/object-0/partition_power_increase/finish/',
            'POST', {})
        self.assertEqual(200, resp.status_int)
        self.assertEqual('idle', body['partition_power_increase_state'])
        finished = RingBuilder.load(builder_path)
        self.assertIsNone(finished.next_part_power)

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

    def test_ring_parts(self):
        resp, body = self.get_json('/api/v1/rings/1/parts/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(48, body['total_parts'])
        self.assertEqual({'sd0': 16, 'sd1': 16, 'sd2': 16}, body['parts'])

    def test_ring_parts_checks_ring(self):
        resp, body = self.get_json('/api/v1/rings/404/parts/')
        self.assertEqual(404, resp.status_int)
        self.assertIsNone(body)

    def test_ring_rebalance(self):
        resp, body = self.get_json('/api/v1/rings/1/rebalance/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(0, body['dispersion'])
        self.assertEqual(True, body['requires_rebalance'])
        self.assertGreaterEqual(body['time_since_last_rebalance'], 1800)
        self.assertGreater(body['minimum_time_until_rebalance'], 0)
        self.assertLessEqual(body['minimum_time_until_rebalance'], 1800)

    def test_ring_dispersion(self):
        resp, body = self.get_json('/api/v1/rings/1/dispersion/?level=zone')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('zone', body['level'])
        self.assertEqual(3, len(body['dispersion']))
        for record in body['dispersion']:
            self.assertEqual(1, record['region'])
            self.assertEqual(1, record['max_replicas'])
            self.assertEqual(16, sum(record['num_of_parts'][1:]))

    def test_ring_dispersion_defaults_invalid_level_to_zone(self):
        resp, body = self.get_json('/api/v1/rings/1/dispersion/?level=bogus')
        self.assertEqual(200, resp.status_int)
        self.assertEqual('bogus', body['level'])
        self.assertEqual(3, len(body['dispersion']))
        for record in body['dispersion']:
            self.assertIn('zone', record)

    def test_ring_at_risk(self):
        resp, body = self.get_json('/api/v1/rings/1/at_risk/')
        self.assertEqual(200, resp.status_int)
        self.assertEqual({
            '10.0.0.0': 16,
            '10.0.0.1': 16,
            '10.0.0.2': 16,
        }, body['total'])
        self.assertEqual(body['total'], body['dispersed'])
        self.assertEqual({
            '10.0.0.0': 0,
            '10.0.0.1': 0,
            '10.0.0.2': 0,
        }, body['at_risk'])

    def test_ring_count_parts(self):
        resp, body = self.get_json(
            '/api/v1/rings/1/count_parts/?replication_ip=10.0.0.0&'
            'replication_ip=10.0.0.1')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(2, body['risk_count'])
        self.assertEqual(list(range(16)), body['count_parts']['2'])

    def test_ring_count_parts_normalizes_risk_count(self):
        resp, body = self.get_json(
            '/api/v1/rings/1/count_parts/?replication_ip=10.0.0.0&'
            'risk_count=1')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(2, body['risk_count'])
        self.assertEqual({}, body['count_parts'])

    def test_ring_count_parts_skips_unplaced_assignments(self):
        def set_none_dev_id(builder):
            builder._replica2part2dev[0][0] = builder.none_dev_id

        def set_out_of_range_dev_id(builder):
            builder._replica2part2dev[0][0] = len(builder.devs)

        def set_removed_dev_id(builder):
            builder.devs[2] = None
            builder._replica2part2dev[0][0] = 2

        cases = (
            ('none-dev-id', set_none_dev_id),
            ('out-of-range-dev-id', set_out_of_range_dev_id),
            ('removed-dev-id', set_removed_dev_id),
        )
        query = ('replication_ip=10.0.0.0&replication_ip=10.0.0.1&'
                 'replication_ip=10.0.0.2&risk_count=3')
        for ring_id, mutate_builder in cases:
            with self.subTest(ring_id=ring_id):
                self._make_count_parts_edge_ring(ring_id, mutate_builder)
                resp, body = self.get_json(
                    '/api/v1/rings/%s/count_parts/?%s' % (ring_id, query))
                self.assertEqual(200, resp.status_int)
                self.assertEqual(3, body['risk_count'])
                self.assertIn('count_parts', body)

    def test_ring_partitions_at_risk_by_node_ip(self):
        resp, body = self.get_json(
            '/api/v1/rings/1/partitions_at_risk/?'
            'node_ip=10.0.0.0&node_ip=10.0.0.1&details=true')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(2, body['risk_count'])
        self.assertEqual(['10.0.0.0', '10.0.0.1'],
                         body['selectors']['node_ips'])
        self.assertEqual(2, body['summary']['matched_devices'])
        self.assertEqual(16, body['summary']['affected_partitions'])
        self.assertEqual(16, body['summary']['at_risk_partitions'])
        self.assertEqual(2, body['summary']['max_down_replicas'])
        self.assertNotIn('partitions_by_down_replica_count', body)
        self.assertEqual(1, len(body['builder_summaries']))
        self.assertEqual('object.builder',
                         body['builder_summaries'][0]['builder'])
        self.assertEqual(list(range(16)),
                         body['builder_summaries'][0]
                         ['partitions_by_down_replica_count']['2'])

    def test_ring_partitions_at_risk_post_body(self):
        resp, body = self.json_request(
            '/api/v1/rings/1/partitions_at_risk/', 'POST', {
                'node_ips': ['10.0.0.0', '10.0.0.1'],
                'device_ids': [],
                'risk_count': 2,
                'details': True,
            })
        self.assertEqual(200, resp.status_int)
        self.assertEqual(['10.0.0.0', '10.0.0.1'],
                         body['selectors']['node_ips'])
        self.assertEqual(2, body['risk_count'])
        self.assertEqual(2, body['summary']['matched_devices'])
        self.assertEqual(16, body['summary']['at_risk_partitions'])
        self.assertEqual(list(range(16)),
                         body['builder_summaries'][0]
                         ['partitions_by_down_replica_count']['2'])

    def test_ring_partitions_at_risk_rejects_oversize_body(self):
        app = RingManagerApplication(
            {
                'ring_manager_state_dir': self.state_dir,
                'ring_artifact_dir': self.testdir,
                'max_json_request_body_size': '24',
            },
            logger=debug_logger())
        resp, body = self.json_request(
            '/api/v1/rings/1/partitions_at_risk/', 'POST', {
                'node_ips': ['10.0.0.0'],
                'padding': 'x' * 64,
            }, app=app)
        self.assertEqual(413, resp.status_int)
        self.assertEqual(
            'Request body must be no larger than 24 bytes', body['error'])

    def test_ring_partitions_at_risk_rejects_streamed_oversize_body(self):
        app = RingManagerApplication(
            {
                'ring_manager_state_dir': self.state_dir,
                'ring_artifact_dir': self.testdir,
                'max_json_request_body_size': '24',
            },
            logger=debug_logger())
        req = Request.blank(
            '/api/v1/rings/1/partitions_at_risk/', method='POST',
            body=json.dumps({
                'node_ips': ['10.0.0.0'],
                'padding': 'x' * 64,
            }).encode('ascii'),
            headers={'Content-Type': 'application/json'})
        req.environ.pop('CONTENT_LENGTH', None)
        resp = req.get_response(app)
        body = json.loads(resp.body.decode('ascii'))
        self.assertEqual(413, resp.status_int)
        self.assertEqual(
            'Request body must be no larger than 24 bytes', body['error'])

    def test_ring_partitions_at_risk_by_ip_alias_and_device_id(self):
        resp, body = self.get_json(
            '/api/v1/rings/1/partitions_at_risk/?ip=10.0.0.0&'
            'device_id=1&details=false')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(['10.0.0.0'], body['selectors']['node_ips'])
        self.assertEqual([1], body['selectors']['device_ids'])
        self.assertEqual(2, body['summary']['matched_devices'])
        self.assertEqual(16, body['summary']['affected_partitions'])
        self.assertEqual(16, body['summary']['at_risk_partitions'])
        self.assertNotIn('partitions_by_down_replica_count', body)
        self.assertNotIn('partitions_by_down_replica_count',
                         body['builder_summaries'][0])

    def test_ring_partitions_at_risk_no_matching_devices(self):
        resp, body = self.get_json(
            '/api/v1/rings/1/partitions_at_risk/?node_ip=10.9.9.9')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(0, body['summary']['matched_devices'])
        self.assertEqual(0, body['summary']['affected_partitions'])
        self.assertEqual(0, body['summary']['at_risk_partitions'])
        self.assertNotIn('partitions_by_down_replica_count', body)
        self.assertEqual(0, body['builder_summaries'][0]['matched_devices'])

    def test_ring_partitions_at_risk_rejects_invalid_device_id(self):
        resp, body = self.get_json(
            '/api/v1/rings/1/partitions_at_risk/?device_id=nope')
        self.assertEqual(400, resp.status_int)
        self.assertIn('device_id must be a non-negative integer',
                      body['error'])

    def test_ring_partitions_at_risk_rejects_no_selectors(self):
        resp, body = self.get_json(
            '/api/v1/rings/1/partitions_at_risk/')
        self.assertEqual(400, resp.status_int)
        self.assertIn('requires at least one down selector', body['error'])

    def test_ring_partitions_at_risk_rejects_blank_selectors(self):
        resp, body = self.get_json(
            '/api/v1/rings/1/partitions_at_risk/?node_ip=&'
            'replication_ip=')
        self.assertEqual(400, resp.status_int)
        self.assertIn('requires at least one down selector', body['error'])

    def test_ring_partitions_at_risk_rejects_too_many_selectors(self):
        missing_builder = os.path.join(self.testdir, 'missing.builder')
        self._write_json('rings/selector-limit.json', {
            'id': 'selector-limit',
            'name': 'Selector Limit',
            'storage_policy_index': 0,
            'policy_type': 'replication',
            'num_replicas': 3,
            'builder_files': [missing_builder],
        })
        app = RingManagerApplication(
            {
                'ring_manager_state_dir': self.state_dir,
                'ring_artifact_dir': self.testdir,
                'max_partitions_at_risk_selectors': '1',
            },
            logger=debug_logger())
        resp, body = self.get_json(
            '/api/v1/rings/selector-limit/partitions_at_risk/?'
            'node_ip=10.0.0.0&node_ip=10.0.0.1', app=app)
        self.assertEqual(400, resp.status_int)
        self.assertEqual(
            'partitions_at_risk accepts at most 1 down selectors; got 2',
            body['error'])

    def test_ring_partitions_at_risk_post_rejects_too_many_selectors(self):
        missing_builder = os.path.join(self.testdir, 'missing.builder')
        self._write_json('rings/selector-limit.json', {
            'id': 'selector-limit',
            'name': 'Selector Limit',
            'storage_policy_index': 0,
            'policy_type': 'replication',
            'num_replicas': 3,
            'builder_files': [missing_builder],
        })
        app = RingManagerApplication(
            {
                'ring_manager_state_dir': self.state_dir,
                'ring_artifact_dir': self.testdir,
                'max_partitions_at_risk_selectors': '2',
            },
            logger=debug_logger())
        resp, body = self.json_request(
            '/api/v1/rings/selector-limit/partitions_at_risk/', 'POST', {
                'node_ips': ['10.0.0.0'],
                'replication_ips': ['10.1.0.0'],
                'device_ids': [2],
            }, app=app)
        self.assertEqual(400, resp.status_int)
        self.assertEqual(
            'partitions_at_risk accepts at most 2 down selectors; got 3',
            body['error'])

    def test_ring_partitions_at_risk_post_rejects_invalid_body(self):
        resp, body = self.json_request(
            '/api/v1/rings/1/partitions_at_risk/', 'POST',
            {'node_ips': [10]})
        self.assertEqual(400, resp.status_int)
        self.assertIn('node_ip entries must be strings', body['error'])

    def test_ring_partitions_at_risk_post_rejects_invalid_details(self):
        resp, body = self.json_request(
            '/api/v1/rings/1/partitions_at_risk/', 'POST',
            {'node_ips': ['10.0.0.0'], 'details': ['true']})
        self.assertEqual(400, resp.status_int)
        self.assertIn('details must be a boolean value', body['error'])

    def test_ring_partitions_at_risk_reports_missing_builder(self):
        missing_builder = os.path.join(self.testdir, 'missing.builder')
        self._write_json('rings/missing.json', {
            'id': 'missing',
            'name': 'Missing Builder',
            'storage_policy_index': 99,
            'policy_type': 'replication',
            'num_replicas': 3,
            'builder_files': [missing_builder],
        })
        resp, body = self.get_json(
            '/api/v1/rings/missing/partitions_at_risk/?node_ip=10.0.0.1')
        self.assertEqual(409, resp.status_int)
        self.assertIn('builder files unavailable', body['error'])
        self.assertNotIn(self.testdir, body['error'])

    def test_ring_partitions_at_risk_namespaces_multiple_builders(self):
        second_builder_path = os.path.join(self.testdir, 'container.builder')
        self._make_builder(second_builder_path)
        self._write_json('rings/multi.json', {
            'id': 'multi',
            'name': 'Account and Container',
            'storage_policy_index': None,
            'policy_type': 'replication',
            'num_replicas': 3,
            'builder_files': [self.builder_path, second_builder_path],
        })
        resp, body = self.get_json(
            '/api/v1/rings/multi/partitions_at_risk/?'
            'node_ip=10.0.0.0&node_ip=10.0.0.1&details=true')
        self.assertEqual(200, resp.status_int)
        self.assertEqual(4, body['summary']['matched_devices'])
        self.assertEqual(32, body['summary']['affected_partitions'])
        self.assertEqual(32, body['summary']['at_risk_partitions'])
        self.assertEqual(2, len(body['builder_summaries']))
        for builder_summary in body['builder_summaries']:
            self.assertEqual(2, builder_summary['matched_devices'])
            self.assertEqual(16, builder_summary['affected_partitions'])
            self.assertEqual(16, builder_summary['at_risk_partitions'])
            self.assertEqual(
                list(range(16)),
                builder_summary['partitions_by_down_replica_count']['2'])

    def test_ring_partitions_at_risk_matches_large_device_id(self):
        builder_path = os.path.join(self.testdir, 'large-id.builder')
        self._make_large_device_id_builder(builder_path, device_id=70000)
        self._write_json('rings/large-id.json', {
            'id': 'large-id',
            'name': 'Large Device IDs',
            'storage_policy_index': 0,
            'policy_type': 'replication',
            'num_replicas': 3,
            'builder_files': [builder_path],
        })
        resp, body = self.get_json(
            '/api/v1/rings/large-id/partitions_at_risk/?'
            'device_id=70000&node_ip=10.0.70.1&details=true')
        self.assertEqual(200, resp.status_int)
        self.assertEqual([70000], body['selectors']['device_ids'])
        self.assertEqual(2, body['summary']['matched_devices'])

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
        self.assertEqual(200, Request.blank(
            '/api/v1/rings/1/partitions_at_risk/', method='POST',
            headers=read_headers, body=b'{}').get_response(app).status_int)
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
        self.assertEqual(200, Request.blank(
            '/api/v1/rings/1/partitions_at_risk/', method='POST',
            headers=headers, body=b'{}').get_response(app).status_int)
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
