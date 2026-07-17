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

from urllib.parse import urlparse

import swift.ring_manager.agent as agent_mod
from swift.common.concurrency import urllib_request
from swift.common.recon import RECON_RING_MANAGER_AGENT_FILE
from swift.common.utils import md5
from swift.ring_manager.agent import INSTALL_JOURNAL, RingManagerAgent, \
    RingManagerAgentError
from test.debug_logger import debug_logger


class FakeResponse(object):
    def __init__(self, body=b'', status=200, headers=None):
        self.body = body
        self.status = status
        self.code = status
        self.headers = headers or {}

    def read(self):
        return self.body

    def info(self):
        return self.headers

    def getcode(self):
        return self.status


class FakeOpener(object):
    def __init__(self, routes):
        self.routes = routes
        self.requests = []

    def __call__(self, req, timeout=None):
        parsed = urlparse(req.get_full_url())
        headers = dict((key.lower(), value)
                       for key, value in req.header_items())
        request = {
            'method': req.get_method(),
            'host': parsed.netloc,
            'path': parsed.path,
            'headers': headers,
            'timeout': timeout,
        }
        self.requests.append(request)
        route = self.routes[(request['method'], request['host'],
                             request['path'])]
        if callable(route):
            return route(request)
        return route


def json_response(value):
    return FakeResponse(json.dumps(value).encode('ascii'))


class TestRingManagerAgent(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.swift_dir = os.path.join(self.testdir, 'swift')
        self.recon_cache_path = os.path.join(self.testdir, 'recon')
        self.state_file = os.path.join(self.recon_cache_path, 'state.json')
        self.ring_body = b'object ring bytes'
        self.ring_md5 = md5(
            self.ring_body, usedforsecurity=False).hexdigest()
        self.ring_sha256 = hashlib.sha256(self.ring_body).hexdigest()
        self.created_at = agent_mod.NormalTimestamp(1).internal
        self.manifest = {
            'version': 'release-1',
            'state': 'published',
            'created_at': self.created_at,
            'latest': True,
            'resource_uri': '/api/v1/rings/releases/release-1/',
            'files': [
                {
                    'name': 'object.ring.gz',
                    'url': '/api/v1/rings/releases/release-1/files/'
                    'object.ring.gz',
                    'bytes': len(self.ring_body),
                    'md5': self.ring_md5,
                    'sha256': self.ring_sha256,
                },
            ],
        }

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def _agent(self, opener, urls='https://ring.example.com:6205',
               logger=None, **conf):
        agent_conf = {
            'ring_manager_urls': urls,
            'swift_dir': self.swift_dir,
            'recon_cache_path': self.recon_cache_path,
            'state_file': self.state_file,
            'request_timeout': 11,
            'jitter': 0,
        }
        agent_conf.update(conf)
        return RingManagerAgent(
            agent_conf, opener=opener, logger=logger or debug_logger())

    def _write_secret(self, name, value):
        path = os.path.join(self.testdir, name)
        with open(path, 'wb') as fp:
            fp.write(value)
        os.chmod(path, 0o600)
        return path

    def _recon_stats(self):
        path = os.path.join(self.recon_cache_path,
                            RECON_RING_MANAGER_AGENT_FILE)
        with open(path, 'r') as fp:
            return json.load(fp)['ring_manager_agent']

    def _manifest_for_version(self, version, body=None, created_at=None):
        body = self.ring_body if body is None else body
        sha256 = hashlib.sha256(body).hexdigest()
        return {
            'version': version,
            'state': 'published',
            'created_at': created_at or self.created_at,
            'latest': True,
            'resource_uri': '/api/v1/rings/releases/%s/' % version,
            'files': [
                {
                    'name': 'object.ring.gz',
                    'url': '/api/v1/rings/releases/%s/files/'
                    'object.ring.gz' % version,
                    'bytes': len(body),
                    'sha256': sha256,
                },
            ],
        }

    def _routes_for_manifest(self, manifest, body=None,
                             host='ring.example.com:6205'):
        body = self.ring_body if body is None else body
        routes = {}
        routes[('GET', host, '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(manifest)
        routes[('GET', host, manifest['files'][0]['url'])] = \
            FakeResponse(body)
        return routes

    def _write_agent_state(self, version, created_at, manifest=None):
        os.makedirs(os.path.dirname(self.state_file))
        state = {
            'source': 'https://previous.example.com:6205',
            'latest_ring_version': version,
            'latest_ring_created_at': created_at,
            'synced_at': created_at,
            'swift_dir': self.swift_dir,
            'files': [],
            'manifest': manifest or {
                'version': version,
                'created_at': created_at,
            },
        }
        with open(self.state_file, 'w') as fp:
            json.dump(state, fp)

    def _routes(self):
        manifest_path = '/api/v1/rings/releases/latest/manifest/'
        file_path = '/api/v1/rings/releases/release-1/files/object.ring.gz'
        routes = {}
        routes[('GET', 'ring.example.com:6205', manifest_path)] = \
            json_response(self.manifest)
        routes[('GET', 'ring.example.com:6205', file_path)] = \
            FakeResponse(self.ring_body)
        return routes

    def _multi_file_manifest_routes(self, files):
        manifest = dict(self.manifest)
        manifest['files'] = []
        routes = {}
        for name, body, sha256 in files:
            path = '/api/v1/rings/releases/release-1/files/%s' % name
            manifest['files'].append({
                'name': name,
                'url': path,
                'bytes': len(body),
                'sha256': sha256,
            })
            routes[('GET', 'ring.example.com:6205', path)] = \
                FakeResponse(body)
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(manifest)
        return manifest, routes

    def test_sync_once_downloads_rings_state_and_recon(self):
        opener = FakeOpener(self._routes())
        logger = debug_logger()
        result = self._agent(opener, logger=logger).sync_once()

        self.assertEqual({
            'latest_ring_version': 'release-1',
            'files_installed': 1,
            'files_downloaded': 1,
            'files_unchanged': 0,
        }, result)
        ring_path = os.path.join(self.swift_dir, 'object.ring.gz')
        with open(ring_path, 'rb') as fp:
            self.assertEqual(self.ring_body, fp.read())

        with open(self.state_file, 'r') as fp:
            state = json.load(fp)
        self.assertEqual('release-1', state['latest_ring_version'])
        self.assertEqual('https://ring.example.com:6205', state['source'])
        self.assertEqual(ring_path, state['files'][0]['path'])

        stats = self._recon_stats()
        self.assertTrue(stats['success'])
        self.assertEqual('release-1', stats['latest_ring_version'])
        self.assertEqual(1, stats['files_downloaded'])
        self.assertEqual(0, stats['files_unchanged'])
        self.assertEqual(self.swift_dir, stats['swift_dir'])
        self.assertFalse(stats['operator_attention']['needed'])
        self.assertEqual(11, opener.requests[0]['timeout'])

        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.sync.attempts'])
        self.assertEqual(1, counts['agent.sync.successes'])
        self.assertEqual(1, counts['agent.files.downloaded'])
        self.assertEqual(0, counts['agent.files.unchanged'])
        self.assertEqual(1, counts['agent.files.installed'])
        self.assertEqual(len(self.ring_body),
                         counts['agent.bytes_downloaded'])
        self.assertIn(
            'agent.sync.timing',
            [call[0][0] for call in
             logger.statsd_client.calls['timing']])

    def test_sync_once_rejects_older_manifest_than_state(self):
        os.makedirs(self.swift_dir)
        ring_path = os.path.join(self.swift_dir, 'object.ring.gz')
        with open(ring_path, 'wb') as fp:
            fp.write(b'already installed ring')
        installed_created_at = agent_mod.NormalTimestamp(2).internal
        self._write_agent_state('release-2', installed_created_at)
        opener = FakeOpener(self._routes())

        with self.assertRaises(RingManagerAgentError) as cm:
            self._agent(opener).sync_once()

        self.assertIn('older than installed version release-2',
                      str(cm.exception))
        self.assertEqual(1, len(opener.requests))
        with open(ring_path, 'rb') as fp:
            self.assertEqual(b'already installed ring', fp.read())
        with open(self.state_file, 'r') as fp:
            state = json.load(fp)
        self.assertEqual('release-2', state['latest_ring_version'])
        self.assertEqual(installed_created_at,
                         state['latest_ring_created_at'])
        stats = self._recon_stats()
        self.assertFalse(stats['success'])
        self.assertIn('older than installed version release-2',
                      stats['source_errors'][0]['error'])

    def test_sync_once_allows_older_manifest_with_rollback_override(self):
        os.makedirs(self.swift_dir)
        ring_path = os.path.join(self.swift_dir, 'object.ring.gz')
        with open(ring_path, 'wb') as fp:
            fp.write(b'newer installed ring')
        installed_created_at = agent_mod.NormalTimestamp(2).internal
        self._write_agent_state('release-2', installed_created_at)
        opener = FakeOpener(self._routes())

        result = self._agent(
            opener, allow_ring_version_rollback='true').sync_once()

        self.assertEqual({
            'latest_ring_version': 'release-1',
            'files_installed': 1,
            'files_downloaded': 1,
            'files_unchanged': 0,
        }, result)
        with open(ring_path, 'rb') as fp:
            self.assertEqual(self.ring_body, fp.read())
        with open(self.state_file, 'r') as fp:
            state = json.load(fp)
        self.assertEqual('release-1', state['latest_ring_version'])
        self.assertEqual(self.created_at, state['latest_ring_created_at'])

    def test_sync_once_allows_equal_manifest_timestamp(self):
        self._write_agent_state('release-1', self.created_at)
        opener = FakeOpener(self._routes())

        result = self._agent(opener).sync_once()

        self.assertEqual('release-1', result['latest_ring_version'])
        with open(self.state_file, 'r') as fp:
            state = json.load(fp)
        self.assertEqual('release-1', state['latest_ring_version'])
        self.assertEqual(self.created_at, state['latest_ring_created_at'])
        self.assertEqual(2, len(opener.requests))

    def test_sync_once_allows_newer_manifest_than_state(self):
        self._write_agent_state('release-1', self.created_at)
        newer_created_at = agent_mod.NormalTimestamp(2).internal
        newer_body = b'newer object ring bytes'
        manifest = self._manifest_for_version(
            'release-2', body=newer_body, created_at=newer_created_at)
        opener = FakeOpener(self._routes_for_manifest(manifest, newer_body))

        result = self._agent(opener).sync_once()

        self.assertEqual({
            'latest_ring_version': 'release-2',
            'files_installed': 1,
            'files_downloaded': 1,
            'files_unchanged': 0,
        }, result)
        with open(self.state_file, 'r') as fp:
            state = json.load(fp)
        self.assertEqual('release-2', state['latest_ring_version'])
        self.assertEqual(newer_created_at, state['latest_ring_created_at'])
        with open(os.path.join(self.swift_dir, 'object.ring.gz'), 'rb') as fp:
            self.assertEqual(newer_body, fp.read())

    def test_sync_once_corrupt_state_aborts_before_install(self):
        os.makedirs(os.path.dirname(self.state_file))
        with open(self.state_file, 'w') as fp:
            fp.write('{bad json')
        routes = self._routes()
        routes[('GET', 'backup.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(self.manifest)
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerAgentError) as cm:
            self._agent(
                opener,
                urls='https://ring.example.com:6205, '
                'https://backup.example.com:6205').sync_once()

        self.assertIn('state read failed', str(cm.exception))
        self.assertEqual(['ring.example.com:6205'],
                         [request['host'] for request in opener.requests])
        self.assertFalse(os.path.exists(
            os.path.join(self.swift_dir, 'object.ring.gz')))
        stats = self._recon_stats()
        self.assertFalse(stats['success'])
        self.assertIn('local_failure',
                      stats['operator_attention']['reasons'])

    def test_sync_once_non_object_state_aborts_before_install(self):
        os.makedirs(os.path.dirname(self.state_file))
        with open(self.state_file, 'w') as fp:
            json.dump([], fp)
        routes = self._routes()
        routes[('GET', 'backup.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(self.manifest)
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerAgentError) as cm:
            self._agent(
                opener,
                urls='https://ring.example.com:6205, '
                'https://backup.example.com:6205').sync_once()

        self.assertIn('state must be an object', str(cm.exception))
        self.assertEqual(['ring.example.com:6205'],
                         [request['host'] for request in opener.requests])
        self.assertFalse(os.path.exists(
            os.path.join(self.swift_dir, 'object.ring.gz')))
        stats = self._recon_stats()
        self.assertFalse(stats['success'])
        self.assertIn('local_failure',
                      stats['operator_attention']['reasons'])

    def test_sync_once_uses_read_key_when_configured(self):
        opener = FakeOpener(self._routes())
        self._agent(
            opener, read_key='reader', admin_key='admin').sync_once()

        for request in opener.requests:
            self.assertEqual(
                'reader', request['headers']['x-ring-manager-read-key'])
            self.assertNotIn('x-ring-manager-admin-key',
                             request['headers'])

    def test_sync_once_uses_read_key_file_when_configured(self):
        opener = FakeOpener(self._routes())
        self._agent(
            opener,
            read_key_file=self._write_secret('read.key', b'reader\n'),
            admin_key_file=self._write_secret('admin.key', b'admin\n')
        ).sync_once()

        for request in opener.requests:
            self.assertEqual(
                'reader', request['headers']['x-ring-manager-read-key'])
            self.assertNotIn('x-ring-manager-admin-key',
                             request['headers'])

    def test_key_file_conflict_fails_closed(self):
        opener = FakeOpener(self._routes())
        with self.assertRaises(RingManagerAgentError) as cm:
            self._agent(
                opener, read_key='reader',
                read_key_file=self._write_secret('read.key', b'reader\n'))
        self.assertIn('mutually exclusive', str(cm.exception))

    def test_sync_once_uses_read_auth_token_when_configured(self):
        opener = FakeOpener(self._routes())
        self._agent(
            opener, read_auth_token='reader-token',
            admin_key='admin', auth_token='admin-token').sync_once()

        for request in opener.requests:
            self.assertEqual('reader-token',
                             request['headers']['x-auth-token'])
            self.assertNotIn('x-ring-manager-admin-key',
                             request['headers'])

    def test_sync_once_uses_conditional_get_for_existing_ring(self):
        os.makedirs(self.swift_dir)
        ring_path = os.path.join(self.swift_dir, 'object.ring.gz')
        with open(ring_path, 'wb') as fp:
            fp.write(self.ring_body)
        routes = self._routes()
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/release-1/files/object.ring.gz')] = \
            FakeResponse(status=304)
        opener = FakeOpener(routes)

        result = self._agent(opener).sync_once()

        self.assertEqual(0, result['files_downloaded'])
        self.assertEqual(1, result['files_unchanged'])
        file_req = opener.requests[1]
        self.assertEqual(self.ring_md5,
                         file_req['headers']['if-none-match'])
        with open(ring_path, 'rb') as fp:
            self.assertEqual(self.ring_body, fp.read())

    def test_sync_once_rejects_304_without_conditional_get(self):
        routes = self._routes()
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/release-1/files/object.ring.gz')] = \
            FakeResponse(status=304)
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerAgentError) as cm:
            self._agent(opener).sync_once()

        self.assertIn('without a conditional request', str(cm.exception))
        self.assertFalse(os.path.exists(self.state_file))
        self.assertEqual(2, len(opener.requests))

    def test_sync_once_holds_install_lock(self):
        locked = []

        class FakeLock(object):
            def __enter__(self):
                locked.append(True)

            def __exit__(self, _exc_type, _exc_value, _traceback):
                locked.append(False)

        def fake_lock_path(directory, timeout=None, name=None):
            self.assertEqual(self.swift_dir, directory)
            self.assertEqual(7, timeout)
            self.assertEqual('ring-manager-agent', name)
            return FakeLock()

        def manifest_route(_request):
            self.assertTrue(locked[-1])
            return json_response(self.manifest)

        def file_route(_request):
            self.assertTrue(locked[-1])
            return FakeResponse(self.ring_body)

        routes = self._routes()
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = manifest_route
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/release-1/files/object.ring.gz')] = \
            file_route

        with mock.patch('swift.ring_manager.agent.lock_path',
                        side_effect=fake_lock_path):
            self._agent(FakeOpener(routes), lock_timeout=7).sync_once()

        self.assertEqual([True, False], locked)

    def test_sync_once_falls_back_to_next_source(self):
        def fail_primary(_request):
            raise urllib_request.URLError('down')

        manifest_path = '/api/v1/rings/releases/latest/manifest/'
        file_path = '/api/v1/rings/releases/release-1/files/object.ring.gz'
        routes = {}
        routes[('GET', 'primary.example.com:6205', manifest_path)] = \
            fail_primary
        routes[('GET', 'secondary.example.com:6205', manifest_path)] = \
            json_response(self.manifest)
        routes[('GET', 'secondary.example.com:6205', file_path)] = \
            FakeResponse(self.ring_body)
        opener = FakeOpener(routes)
        logger = debug_logger()
        agent = self._agent(
            opener,
            urls='https://primary.example.com:6205, '
            'https://secondary.example.com:6205',
            logger=logger)

        result = agent.sync_once()

        self.assertEqual('release-1', result['latest_ring_version'])
        self.assertEqual('primary.example.com:6205',
                         opener.requests[0]['host'])
        self.assertEqual('secondary.example.com:6205',
                         opener.requests[1]['host'])
        self.assertEqual('https://secondary.example.com:6205',
                         self._recon_stats()['source'])
        stats = self._recon_stats()
        self.assertEqual(1, len(stats['source_errors']))
        self.assertEqual('https://primary.example.com:6205',
                         stats['source_errors'][0]['source'])
        self.assertIn('down', stats['source_errors'][0]['error'])
        self.assertEqual({
            'needed': True,
            'reasons': ['source_errors'],
            'source_errors': 1,
            'install_journal_path': None,
            'backup_files_count': 0,
            'backup_files': [],
        }, stats['operator_attention'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.operator_attention'])
        self.assertEqual(1, counts['agent.operator_attention.source_errors'])

    def test_sync_once_rejects_cross_origin_artifact_url(self):
        manifest = dict(self.manifest)
        manifest['files'] = [dict(
            self.manifest['files'][0],
            url='https://evil.example.com/steal')]
        routes = self._routes()
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(manifest)
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerAgentError) as cm:
            self._agent(opener, read_key='reader').sync_once()
        self.assertIn('same-origin relative URL', str(cm.exception))
        self.assertEqual(1, len(opener.requests))
        self.assertEqual(
            'reader',
            opener.requests[0]['headers']['x-ring-manager-read-key'])

    def test_sync_once_rejects_artifact_url_dot_segments(self):
        manifest = dict(self.manifest)
        manifest['files'] = [dict(
            self.manifest['files'][0],
            url='/api/v1/../../healthcheck')]
        routes = self._routes()
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(manifest)
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerAgentError) as cm:
            self._agent(opener, read_key='reader').sync_once()
        self.assertIn('dot segments', str(cm.exception))
        self.assertEqual(1, len(opener.requests))

    def test_source_urls_can_be_shuffled(self):
        values = iter([0.9, 0.1])
        agent = RingManagerAgent({
            'ring_manager_urls': 'https://first, https://second',
            'swift_dir': self.swift_dir,
            'recon_cache_path': self.recon_cache_path,
            'shuffle_ring_manager_urls': 'true',
        }, opener=FakeOpener({}), logger=debug_logger(),
            random_func=lambda: next(values))

        self.assertEqual(['https://second', 'https://first'],
                         agent._source_urls())

    def test_bad_checksum_does_not_install_ring(self):
        bad_body = b'x' * len(self.ring_body)
        routes = self._routes()
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/release-1/files/object.ring.gz')] = \
            FakeResponse(bad_body)
        opener = FakeOpener(routes)
        logger = debug_logger()

        with self.assertRaises(RingManagerAgentError):
            self._agent(opener, logger=logger).sync_once()

        self.assertFalse(os.path.exists(
            os.path.join(self.swift_dir, 'object.ring.gz')))
        self.assertFalse(os.path.exists(self.state_file))
        stats = self._recon_stats()
        self.assertFalse(stats['success'])
        self.assertEqual(1, len(stats['source_errors']))
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.sync.attempts'])
        self.assertEqual(1, counts['agent.source.failures'])
        self.assertEqual(1, counts['agent.sync.failures'])
        self.assertEqual(1, counts['agent.checksum_failures'])

    def test_success_recon_clears_stale_source_errors(self):
        ring_path = '/api/v1/rings/releases/release-1/files/object.ring.gz'
        routes = self._routes()
        routes[('GET', 'ring.example.com:6205', ring_path)] = FakeResponse(
            b'x' * len(self.ring_body))
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerAgentError):
            self._agent(opener).sync_once()

        stats = self._recon_stats()
        self.assertFalse(stats['success'])
        self.assertIn('source_errors', stats)
        self.assertIn('sources', stats)

        routes[('GET', 'ring.example.com:6205', ring_path)] = FakeResponse(
            self.ring_body)
        result = self._agent(opener).sync_once()

        self.assertEqual('release-1', result['latest_ring_version'])
        stats = self._recon_stats()
        self.assertTrue(stats['success'])
        self.assertNotIn('source_errors', stats)
        self.assertNotIn('sources', stats)
        self.assertFalse(stats['operator_attention']['needed'])

    def test_bad_second_checksum_does_not_install_first_ring(self):
        os.makedirs(self.swift_dir)
        old_object = b'old object ring'
        old_container = b'old container ring'
        object_path = os.path.join(self.swift_dir, 'object.ring.gz')
        container_path = os.path.join(self.swift_dir, 'container.ring.gz')
        with open(object_path, 'wb') as fp:
            fp.write(old_object)
        with open(container_path, 'wb') as fp:
            fp.write(old_container)

        object_body = b'new object ring'
        container_body = b'bad container bytes'
        _manifest, routes = self._multi_file_manifest_routes([
            ('object.ring.gz', object_body,
             hashlib.sha256(object_body).hexdigest()),
            ('container.ring.gz', container_body,
             hashlib.sha256(b'expected container ring').hexdigest()),
        ])
        logger = debug_logger()

        with self.assertRaises(RingManagerAgentError):
            self._agent(FakeOpener(routes), logger=logger).sync_once()

        with open(object_path, 'rb') as fp:
            self.assertEqual(old_object, fp.read())
        with open(container_path, 'rb') as fp:
            self.assertEqual(old_container, fp.read())
        self.assertFalse(os.path.exists(self.state_file))
        stats = self._recon_stats()
        self.assertFalse(stats['success'])
        self.assertFalse(stats['operator_attention']['needed'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.checksum_failures'])

    def test_cleanup_fsync_failure_does_not_fall_back_to_next_source(self):
        os.makedirs(self.swift_dir)
        object_body = b'new object ring'
        container_body = b'bad container bytes'
        _manifest, routes = self._multi_file_manifest_routes([
            ('object.ring.gz', object_body,
             hashlib.sha256(object_body).hexdigest()),
            ('container.ring.gz', container_body,
             hashlib.sha256(b'expected container ring').hexdigest()),
        ])
        routes[('GET', 'backup.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(self.manifest)
        opener = FakeOpener(routes)

        real_open = agent_mod.os.open

        def fail_swift_dir_open(path, flags, *args):
            if path == self.swift_dir:
                raise OSError('injected cleanup directory fsync failure')
            return real_open(path, flags, *args)

        with mock.patch('swift.ring_manager.agent.os.open',
                        side_effect=fail_swift_dir_open):
            with self.assertRaises(RingManagerAgentError) as cm:
                self._agent(
                    opener,
                    urls='https://ring.example.com:6205, '
                    'https://backup.example.com:6205').sync_once()

        self.assertIn('local cleanup after ring sync failure failed',
                      str(cm.exception))
        self.assertNotIn(
            'backup.example.com:6205',
            [request['host'] for request in opener.requests])
        self.assertFalse(os.path.exists(self.state_file))

    def test_staging_fsync_failure_does_not_fall_back_to_next_source(self):
        os.makedirs(self.swift_dir)
        routes = self._routes()
        routes[('GET', 'backup.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(self.manifest)
        opener = FakeOpener(routes)
        real_fsync = agent_mod.fsync
        fsync_calls = []

        def fail_first_fsync(fd):
            fsync_calls.append(fd)
            if len(fsync_calls) == 1:
                raise OSError('injected staging fsync failure')
            return real_fsync(fd)

        with mock.patch('swift.ring_manager.agent.fsync',
                        side_effect=fail_first_fsync):
            with self.assertRaises(RingManagerAgentError) as cm:
                self._agent(
                    opener,
                    urls='https://ring.example.com:6205, '
                    'https://backup.example.com:6205').sync_once()

        self.assertIn('local staging failed', str(cm.exception))
        self.assertNotIn(
            'backup.example.com:6205',
            [request['host'] for request in opener.requests])
        self.assertFalse(os.path.exists(self.state_file))

    def test_install_failure_rolls_back_and_does_not_record_success(self):
        os.makedirs(self.swift_dir)
        old_object = b'old object ring'
        old_container = b'old container ring'
        object_path = os.path.join(self.swift_dir, 'object.ring.gz')
        container_path = os.path.join(self.swift_dir, 'container.ring.gz')
        with open(object_path, 'wb') as fp:
            fp.write(old_object)
        with open(container_path, 'wb') as fp:
            fp.write(old_container)

        object_body = b'new object ring'
        container_body = b'new container ring'
        _manifest, routes = self._multi_file_manifest_routes([
            ('object.ring.gz', object_body,
             hashlib.sha256(object_body).hexdigest()),
            ('container.ring.gz', container_body,
             hashlib.sha256(container_body).hexdigest()),
        ])
        real_rename = os.rename
        install_renames = []

        def fail_second_install_rename(src, dst):
            if (dst in (object_path, container_path) and
                    '.tmp-' in os.path.basename(src)):
                install_renames.append(dst)
                if len(install_renames) == 2:
                    raise OSError('injected install failure')
            return real_rename(src, dst)

        logger = debug_logger()
        with mock.patch('swift.ring_manager.agent.os.rename',
                        side_effect=fail_second_install_rename):
            with self.assertRaises(RingManagerAgentError):
                self._agent(FakeOpener(routes), logger=logger).sync_once()

        with open(object_path, 'rb') as fp:
            self.assertEqual(old_object, fp.read())
        with open(container_path, 'rb') as fp:
            self.assertEqual(old_container, fp.read())
        self.assertFalse(os.path.exists(self.state_file))
        self.assertEqual([], [
            name for name in os.listdir(self.swift_dir)
            if '.tmp-' in name or '.ring-manager-backup-' in name])
        stats = self._recon_stats()
        self.assertFalse(stats['success'])
        self.assertTrue(stats['operator_attention']['needed'])
        self.assertIn('local_failure',
                      stats['operator_attention']['reasons'])
        self.assertEqual(0, stats['operator_attention']['backup_files_count'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.install_failures'])
        self.assertEqual(1, counts['agent.sync.failures'])
        self.assertEqual(1, counts['agent.operator_attention'])
        self.assertEqual(1, counts['agent.operator_attention.local_failures'])
        self.assertNotIn('agent.sync.successes', counts)

    def test_successful_multi_file_update_removes_backups(self):
        os.makedirs(self.swift_dir)
        object_path = os.path.join(self.swift_dir, 'object.ring.gz')
        container_path = os.path.join(self.swift_dir, 'container.ring.gz')
        with open(object_path, 'wb') as fp:
            fp.write(b'old object ring')
        with open(container_path, 'wb') as fp:
            fp.write(b'old container ring')

        object_body = b'new object ring'
        container_body = b'new container ring'
        _manifest, routes = self._multi_file_manifest_routes([
            ('object.ring.gz', object_body,
             hashlib.sha256(object_body).hexdigest()),
            ('container.ring.gz', container_body,
             hashlib.sha256(container_body).hexdigest()),
        ])

        result = self._agent(FakeOpener(routes)).sync_once()

        self.assertEqual(2, result['files_downloaded'])
        with open(object_path, 'rb') as fp:
            self.assertEqual(object_body, fp.read())
        with open(container_path, 'rb') as fp:
            self.assertEqual(container_body, fp.read())
        self.assertEqual([], [
            name for name in os.listdir(self.swift_dir)
            if '.tmp-' in name or '.ring-manager-backup-' in name])
        stats = self._recon_stats()
        self.assertTrue(stats['success'])
        self.assertFalse(stats['operator_attention']['needed'])

    def test_incomplete_rollback_preserves_backup_for_operator(self):
        os.makedirs(self.swift_dir)
        old_object = b'old object ring'
        old_container = b'old container ring'
        object_path = os.path.join(self.swift_dir, 'object.ring.gz')
        container_path = os.path.join(self.swift_dir, 'container.ring.gz')
        with open(object_path, 'wb') as fp:
            fp.write(old_object)
        with open(container_path, 'wb') as fp:
            fp.write(old_container)

        object_body = b'new object ring'
        container_body = b'new container ring'
        _manifest, routes = self._multi_file_manifest_routes([
            ('object.ring.gz', object_body,
             hashlib.sha256(object_body).hexdigest()),
            ('container.ring.gz', container_body,
             hashlib.sha256(container_body).hexdigest()),
        ])
        real_rename = os.rename
        install_renames = []

        def fail_install_then_rollback(src, dst):
            if (dst in (object_path, container_path) and
                    '.tmp-' in os.path.basename(src)):
                install_renames.append(dst)
                if len(install_renames) == 2:
                    raise OSError('injected install failure')
            if (dst == object_path and
                    '.ring-manager-backup-' in os.path.basename(src)):
                raise OSError('injected rollback failure')
            return real_rename(src, dst)

        logger = debug_logger()
        with mock.patch('swift.ring_manager.agent.os.rename',
                        side_effect=fail_install_then_rollback):
            with self.assertRaises(RingManagerAgentError):
                self._agent(FakeOpener(routes), logger=logger).sync_once()

        with open(object_path, 'rb') as fp:
            self.assertEqual(object_body, fp.read())
        with open(container_path, 'rb') as fp:
            self.assertEqual(old_container, fp.read())
        backups = [
            name for name in os.listdir(self.swift_dir)
            if '.ring-manager-backup-' in name]
        self.assertEqual(1, len(backups))
        with open(os.path.join(self.swift_dir, backups[0]), 'rb') as fp:
            self.assertEqual(old_object, fp.read())
        self.assertFalse(os.path.exists(self.state_file))
        stats = self._recon_stats()
        self.assertFalse(stats['success'])
        attention = stats['operator_attention']
        self.assertTrue(attention['needed'])
        self.assertEqual(1, attention['backup_files_count'])
        self.assertEqual(os.path.join(self.swift_dir, backups[0]),
                         attention['backup_files'][0])
        self.assertEqual(os.path.join(self.swift_dir, INSTALL_JOURNAL),
                         attention['install_journal_path'])
        self.assertIn('install_journal_present', attention['reasons'])
        self.assertIn('rollback_backups_present', attention['reasons'])
        self.assertIn('local_failure', attention['reasons'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.install_failures'])
        self.assertEqual(1, counts['agent.operator_attention'])
        self.assertEqual(1, counts['agent.operator_attention.journal'])
        self.assertEqual(1, counts['agent.operator_attention.backup_files'])
        self.assertEqual(1, counts['agent.operator_attention.local_failures'])
        self.assertNotIn('agent.sync.successes', counts)

    def test_install_failure_does_not_fall_back_to_next_source(self):
        os.makedirs(self.swift_dir)
        old_object = b'old object ring'
        old_container = b'old container ring'
        object_path = os.path.join(self.swift_dir, 'object.ring.gz')
        container_path = os.path.join(self.swift_dir, 'container.ring.gz')
        with open(object_path, 'wb') as fp:
            fp.write(old_object)
        with open(container_path, 'wb') as fp:
            fp.write(old_container)

        object_body = b'new object ring'
        container_body = b'new container ring'
        _manifest, routes = self._multi_file_manifest_routes([
            ('object.ring.gz', object_body,
             hashlib.sha256(object_body).hexdigest()),
            ('container.ring.gz', container_body,
             hashlib.sha256(container_body).hexdigest()),
        ])
        routes[('GET', 'backup.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(self.manifest)
        opener = FakeOpener(routes)
        real_rename = os.rename
        install_renames = []

        def fail_install_then_rollback(src, dst):
            if (dst in (object_path, container_path) and
                    '.tmp-' in os.path.basename(src)):
                install_renames.append(dst)
                if len(install_renames) == 2:
                    raise OSError('injected install failure')
            if (dst == object_path and
                    '.ring-manager-backup-' in os.path.basename(src)):
                raise OSError('injected rollback failure')
            return real_rename(src, dst)

        with mock.patch('swift.ring_manager.agent.os.rename',
                        side_effect=fail_install_then_rollback):
            with self.assertRaises(RingManagerAgentError):
                self._agent(
                    opener,
                    urls='https://ring.example.com:6205, '
                    'https://backup.example.com:6205').sync_once()

        self.assertNotIn(
            'backup.example.com:6205',
            [request['host'] for request in opener.requests])
        self.assertTrue(os.path.exists(
            os.path.join(self.swift_dir, INSTALL_JOURNAL)))
        self.assertFalse(os.path.exists(self.state_file))
        self.assertEqual(1, len([
            name for name in os.listdir(self.swift_dir)
            if '.ring-manager-backup-' in name]))

    def test_backup_cleanup_failure_does_not_fall_back_to_next_source(self):
        os.makedirs(self.swift_dir)
        object_path = os.path.join(self.swift_dir, 'object.ring.gz')
        with open(object_path, 'wb') as fp:
            fp.write(b'old object ring')
        routes = self._routes()
        routes[('GET', 'backup.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(self.manifest)
        opener = FakeOpener(routes)
        real_unlink = os.unlink

        def fail_backup_unlink(path):
            if '.ring-manager-backup-' in os.path.basename(path):
                raise OSError('injected backup cleanup failure')
            return real_unlink(path)

        logger = debug_logger()
        with mock.patch('swift.ring_manager.agent.os.unlink',
                        side_effect=fail_backup_unlink):
            with self.assertRaises(RingManagerAgentError):
                self._agent(
                    opener, logger=logger,
                    urls='https://ring.example.com:6205, '
                    'https://backup.example.com:6205').sync_once()

        self.assertNotIn(
            'backup.example.com:6205',
            [request['host'] for request in opener.requests])
        self.assertFalse(os.path.exists(self.state_file))
        self.assertEqual(1, len([
            name for name in os.listdir(self.swift_dir)
            if '.ring-manager-backup-' in name]))
        stats = self._recon_stats()
        attention = stats['operator_attention']
        self.assertTrue(attention['needed'])
        self.assertIn('rollback_backups_present', attention['reasons'])
        self.assertIn('local_failure', attention['reasons'])
        self.assertEqual(1, attention['backup_files_count'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.operator_attention'])
        self.assertEqual(1, counts['agent.operator_attention.backup_files'])
        self.assertEqual(1, counts['agent.operator_attention.local_failures'])

    def test_success_recon_reports_leftover_backups_attention(self):
        os.makedirs(self.swift_dir)
        object_path = os.path.join(self.swift_dir, 'object.ring.gz')
        backup_path = '%s.ring-manager-backup-leftover' % object_path
        with open(object_path, 'wb') as fp:
            fp.write(self.ring_body)
        with open(backup_path, 'wb') as fp:
            fp.write(b'old object ring')

        routes = self._routes()
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/release-1/files/object.ring.gz')] = \
            FakeResponse(status=304)
        logger = debug_logger()

        result = self._agent(FakeOpener(routes), logger=logger).sync_once()

        self.assertEqual(0, result['files_downloaded'])
        stats = self._recon_stats()
        self.assertTrue(stats['success'])
        attention = stats['operator_attention']
        self.assertTrue(attention['needed'])
        self.assertEqual(1, attention['backup_files_count'])
        self.assertEqual([backup_path], attention['backup_files'])
        self.assertIn('rollback_backups_present', attention['reasons'])
        self.assertNotIn('local_failure', attention['reasons'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.operator_attention'])
        self.assertEqual(1, counts['agent.operator_attention.backup_files'])
        self.assertNotIn('agent.operator_attention.local_failures', counts)

    def test_install_journal_recovery_restores_old_files_before_sync(self):
        os.makedirs(self.swift_dir)
        old_object = b'old object ring'
        old_container = b'old container ring'
        new_object = b'new object ring'
        new_container = b'new container ring'
        object_path = os.path.join(self.swift_dir, 'object.ring.gz')
        container_path = os.path.join(self.swift_dir, 'container.ring.gz')
        object_backup = '%s.ring-manager-backup-recover' % object_path
        container_backup = '%s.ring-manager-backup-recover' % container_path
        container_temp = '%s.tmp-recover' % container_path

        with open(object_path, 'wb') as fp:
            fp.write(new_object)
        with open(object_backup, 'wb') as fp:
            fp.write(old_object)
        with open(container_path, 'wb') as fp:
            fp.write(old_container)
        with open(container_backup, 'wb') as fp:
            fp.write(old_container)
        with open(container_temp, 'wb') as fp:
            fp.write(new_container)

        journal = {
            'version': 'release-1',
            'swift_dir': self.swift_dir,
            'created_at': '1.00000',
            'files': [
                {
                    'name': 'object.ring.gz',
                    'local_path': object_path,
                    'temp_path': None,
                    'backup_path': object_backup,
                    'had_existing': True,
                    'file_info': {
                        'name': 'object.ring.gz',
                        'bytes': len(new_object),
                        'sha256': hashlib.sha256(new_object).hexdigest(),
                    },
                },
                {
                    'name': 'container.ring.gz',
                    'local_path': container_path,
                    'temp_path': container_temp,
                    'backup_path': container_backup,
                    'had_existing': True,
                    'file_info': {
                        'name': 'container.ring.gz',
                        'bytes': len(new_container),
                        'sha256': hashlib.sha256(new_container).hexdigest(),
                    },
                },
            ],
        }
        with open(os.path.join(self.swift_dir, INSTALL_JOURNAL), 'w') as fp:
            json.dump(journal, fp)

        def fail_manifest(_request):
            raise urllib_request.URLError('down')

        routes = {
            ('GET', 'ring.example.com:6205',
             '/api/v1/rings/releases/latest/manifest/'): fail_manifest,
        }
        logger = debug_logger()

        with self.assertRaises(RingManagerAgentError):
            self._agent(FakeOpener(routes), logger=logger).sync_once()

        with open(object_path, 'rb') as fp:
            self.assertEqual(old_object, fp.read())
        with open(container_path, 'rb') as fp:
            self.assertEqual(old_container, fp.read())
        self.assertFalse(os.path.exists(object_backup))
        self.assertFalse(os.path.exists(container_backup))
        self.assertFalse(os.path.exists(container_temp))
        self.assertFalse(os.path.exists(
            os.path.join(self.swift_dir, INSTALL_JOURNAL)))
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.install_recoveries'])
        stats = self._recon_stats()
        self.assertFalse(stats['operator_attention']['needed'])

    def test_install_journal_read_failure_needs_operator_attention(self):
        os.makedirs(self.swift_dir)
        journal_path = os.path.join(self.swift_dir, INSTALL_JOURNAL)
        with open(journal_path, 'w') as fp:
            fp.write('{bad json')
        opener = FakeOpener(self._routes())
        logger = debug_logger()

        with self.assertRaises(RingManagerAgentError):
            self._agent(opener, logger=logger).sync_once()

        self.assertEqual([], opener.requests)
        stats = self._recon_stats()
        self.assertFalse(stats['success'])
        attention = stats['operator_attention']
        self.assertTrue(attention['needed'])
        self.assertEqual(journal_path, attention['install_journal_path'])
        self.assertIn('install_journal_present', attention['reasons'])
        self.assertIn('local_failure', attention['reasons'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.operator_attention'])
        self.assertEqual(1, counts['agent.operator_attention.journal'])
        self.assertEqual(1, counts['agent.operator_attention.local_failures'])

    def test_falsey_install_journal_needs_operator_attention(self):
        os.makedirs(self.swift_dir)
        journal_path = os.path.join(self.swift_dir, INSTALL_JOURNAL)
        for body in ('{}', 'null'):
            with self.subTest(body=body):
                with open(journal_path, 'w') as fp:
                    fp.write(body)
                opener = FakeOpener(self._routes())
                logger = debug_logger()

                with self.assertRaises(RingManagerAgentError):
                    self._agent(opener, logger=logger).sync_once()

                self.assertEqual([], opener.requests)
                stats = self._recon_stats()
                attention = stats['operator_attention']
                self.assertTrue(attention['needed'])
                self.assertEqual(
                    journal_path, attention['install_journal_path'])
                self.assertIn('install_journal_present', attention['reasons'])
                self.assertIn('local_failure', attention['reasons'])
                counts = logger.statsd_client.get_stats_counts()
                self.assertEqual(1, counts['agent.operator_attention'])
                self.assertEqual(1, counts['agent.operator_attention.journal'])
                self.assertEqual(
                    1, counts['agent.operator_attention.local_failures'])
                os.unlink(journal_path)

    def test_journal_clear_fsync_failure_needs_operator_attention(self):
        os.makedirs(self.swift_dir)
        object_path = os.path.join(self.swift_dir, 'object.ring.gz')
        object_backup = '%s.ring-manager-backup-recover' % object_path
        new_object = b'new object ring'
        old_object = b'old object ring'
        with open(object_path, 'wb') as fp:
            fp.write(new_object)
        with open(object_backup, 'wb') as fp:
            fp.write(old_object)
        journal_path = os.path.join(self.swift_dir, INSTALL_JOURNAL)
        journal = {
            'version': 'release-1',
            'swift_dir': self.swift_dir,
            'created_at': '1.00000',
            'files': [{
                'name': 'object.ring.gz',
                'local_path': object_path,
                'temp_path': None,
                'backup_path': object_backup,
                'had_existing': True,
                'file_info': {
                    'name': 'object.ring.gz',
                    'bytes': len(new_object),
                    'sha256': hashlib.sha256(new_object).hexdigest(),
                },
            }],
        }
        with open(journal_path, 'w') as fp:
            json.dump(journal, fp)
        opener = FakeOpener(self._routes())
        logger = debug_logger()
        real_fsync = agent_mod.fsync
        calls = []

        def fail_second_fsync(fd):
            calls.append(fd)
            if len(calls) == 2:
                raise OSError('injected journal clear fsync failure')
            return real_fsync(fd)

        with mock.patch('swift.ring_manager.agent.fsync',
                        side_effect=fail_second_fsync):
            with self.assertRaises(RingManagerAgentError):
                self._agent(opener, logger=logger).sync_once()

        self.assertEqual([], opener.requests)
        self.assertFalse(os.path.exists(journal_path))
        self.assertFalse(os.path.exists(object_backup))
        with open(object_path, 'rb') as fp:
            self.assertEqual(old_object, fp.read())
        stats = self._recon_stats()
        attention = stats['operator_attention']
        self.assertTrue(attention['needed'])
        self.assertEqual(['local_failure'], attention['reasons'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['agent.operator_attention'])
        self.assertEqual(1, counts['agent.operator_attention.local_failures'])

    def test_unsafe_file_name_is_rejected(self):
        manifest = dict(self.manifest)
        manifest['files'] = [dict(self.manifest['files'][0],
                                  name='../object.ring.gz')]
        routes = self._routes()
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(manifest)
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerAgentError):
            self._agent(opener).sync_once()

        self.assertFalse(os.path.exists(
            os.path.join(self.testdir, 'object.ring.gz')))

    def test_reserved_agent_file_names_are_rejected(self):
        for name in (INSTALL_JOURNAL, '.lock-ring-manager-agent'):
            manifest = dict(self.manifest)
            manifest['files'] = [dict(self.manifest['files'][0], name=name)]
            routes = self._routes()
            routes[('GET', 'ring.example.com:6205',
                    '/api/v1/rings/releases/latest/manifest/')] = \
                json_response(manifest)
            opener = FakeOpener(routes)

            with self.subTest(name=name):
                with self.assertRaises(RingManagerAgentError) as cm:
                    self._agent(opener).sync_once()

                self.assertIn('reserved for ring-manager-agent',
                              str(cm.exception))
                self.assertEqual(1, len(opener.requests))
                self.assertFalse(os.path.exists(self.state_file))

    def test_duplicate_manifest_file_names_are_rejected_before_download(self):
        manifest = dict(self.manifest)
        manifest['files'] = [
            dict(self.manifest['files'][0]),
            dict(self.manifest['files'][0]),
        ]
        routes = self._routes()
        routes[('GET', 'ring.example.com:6205',
                '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(manifest)
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerAgentError) as cm:
            self._agent(opener).sync_once()

        self.assertIn('duplicate file name object.ring.gz', str(cm.exception))
        self.assertEqual(1, len(opener.requests))
        self.assertFalse(os.path.exists(self.state_file))


if __name__ == '__main__':
    unittest.main()
