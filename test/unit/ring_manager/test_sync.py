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
import io
import json
import os
import shutil
import tempfile
import unittest

from urllib.parse import urlparse
from unittest import mock

from swift.common.ring.builder import RingBuilder
from swift.common.recon import RECON_RING_MANAGER_FILE
from swift.common.swob import Request
from swift.common.utils import md5
from swift.ring_manager.common import NormalTimestamp, \
    RING_MANAGER_SYNC_JOURNAL
from swift.ring_manager.server import RingManagerApplication
from swift.ring_manager import sync
from swift.ring_manager.sync import RingManagerSync, RingManagerSyncError, \
    RingManagerSyncLocalError
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
        route = self.routes.get((
            request['method'], parsed.netloc, request['path']))
        if route is None:
            route = self.routes[(request['method'], request['path'])]
        if callable(route):
            return route(request)
        return route


def json_response(value):
    return FakeResponse(json.dumps(value).encode('ascii'))


class TestRingManagerSync(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.state_dir = os.path.join(self.testdir, 'state')
        self.artifact_dir = os.path.join(self.testdir, 'artifacts')
        self.recon_cache_path = os.path.join(self.testdir, 'recon')
        self.artifact_body = b'account ring bytes'
        self.artifact_md5 = md5(
            self.artifact_body, usedforsecurity=False).hexdigest()
        self.artifact_sha256 = hashlib.sha256(
            self.artifact_body).hexdigest()
        self.builder_path = os.path.join(self.testdir, 'account.builder')
        self._make_builder(self.builder_path)
        with open(self.builder_path, 'rb') as fp:
            self.builder_body = fp.read()
        self.builder_md5 = md5(
            self.builder_body, usedforsecurity=False).hexdigest()
        self.builder_sha256 = hashlib.sha256(
            self.builder_body).hexdigest()
        self.builder_version = RingBuilder.load(self.builder_path).version
        self.manifest = {
            'version': 'release-1',
            'state': 'published',
            'latest': True,
            'resource_uri': '/api/v1/rings/releases/release-1/',
            'rings': [
                {'ring_id': 'account', 'swift_ring_version': 12},
            ],
            'files': [
                {
                    'name': 'account.ring.gz',
                    'url': '/api/v1/rings/releases/release-1/files/'
                    'account.ring.gz',
                    'bytes': len(self.artifact_body),
                    'md5': self.artifact_md5,
                    'sha256': self.artifact_sha256,
                },
            ],
        }
        self.rings = {
            'meta': {
                'limit': 1,
                'next': None,
                'offset': 0,
                'previous': None,
                'total_count': 1,
            },
            'objects': [
                {
                    'id': 'account',
                    'name': 'Account',
                    'ring_type': 'account',
                    'resource_uri': '/api/v1/rings/account/',
                },
            ],
        }
        self.ring_version = {
            'ring_id': 'account',
            'version': '12',
            'swift_ring_version': 12,
            'state': 'published',
            'latest': True,
            'resource_uri': '/api/v1/rings/account/versions/12/',
            'files': [
                {
                    'name': 'account.ring.gz',
                    'url': '/api/v1/rings/account/versions/12/files/'
                    'account.ring.gz',
                    'bytes': len(self.artifact_body),
                    'md5': self.artifact_md5,
                    'sha256': self.artifact_sha256,
                },
            ],
        }
        self.primary_status = {
            'mode': 'primary',
            'latest_ring_version': 'release-1',
            'ring_manager_sync': {
                'applicable': False,
            },
        }

    def tearDown(self):
        shutil.rmtree(self.testdir)

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

    def _routes(self):
        def artifact(request):
            if request['headers'].get('If-none-match'.lower()) == \
                    self.artifact_md5:
                return FakeResponse(status=304)
            return FakeResponse(self.artifact_body)

        def builder_file(request):
            if request['headers'].get('If-none-match'.lower()) == \
                    self.builder_md5:
                return FakeResponse(status=304)
            return FakeResponse(self.builder_body, headers={
                'X-Checksum-Sha256': self.builder_sha256,
                'X-Ring-Builder-Version': str(self.builder_version),
            })

        return {
            ('GET', '/api/v1/ring_manager/status/'):
                json_response(self.primary_status),
            ('GET', '/api/v1/rings/releases/latest/manifest/'):
                json_response(self.manifest),
            ('GET', '/api/v1/rings/releases/release-1/files/'
             'account.ring.gz'): artifact,
            ('GET', '/api/v1/rings/'): json_response(self.rings),
            ('GET', '/api/v1/rings/account/versions/12/'):
                json_response(self.ring_version),
            ('GET', '/api/v1/rings/account/versions/12/files/'
             'account.ring.gz'): artifact,
            ('GET', '/api/v1/rings/account/builder/'):
                json_response({
                    'ring_id': 'account',
                    'disabled': False,
                    'builder_version': self.builder_version,
                    'latest_swift_ring_version': self.builder_version,
                    'file': {
                        'bytes': len(self.builder_body),
                        'md5': self.builder_md5,
                        'sha256': self.builder_sha256,
                        'url': '/api/v1/rings/account/builder/file/',
                    },
                }),
            ('GET', '/api/v1/rings/account/builder/file/'): builder_file,
        }

    def _syncer(self, opener, time_func=NormalTimestamp.now, logger=None,
                source_url='http://primary.example.com:6205', **kwargs):
        sync_kwargs = {
            'admin_key': 'secret',
            'timeout': 12,
            'opener': opener,
            'recon_cache_path': self.recon_cache_path,
            'logger': logger or debug_logger(),
            'time_func': time_func,
        }
        sync_kwargs.update(kwargs)
        return RingManagerSync(
            source_url,
            self.state_dir,
            self.artifact_dir,
            **sync_kwargs)

    def _replica_status(self, mode='readonly',
                        can_serve_published_reads=True, reasons=None,
                        latest_ring_version='release-1',
                        last_synced_at='1700000000.00000',
                        latest_matches_local=True,
                        synced=True, fresh=True, stale=False):
        return {
            'mode': mode,
            'ring_manager_sync': {
                'can_serve_published_reads': can_serve_published_reads,
                'latest_ring_version': latest_ring_version,
                'last_synced_at': last_synced_at,
                'latest_matches_local': latest_matches_local,
                'synced': synced,
                'fresh': fresh,
                'stale': stale,
                'reasons': reasons or [],
            },
        }

    def _write_secret(self, name, value):
        path = os.path.join(self.testdir, name)
        with open(path, 'wb') as fp:
            fp.write(value)
        os.chmod(path, 0o600)
        return path

    def _write_config(self, body):
        path = os.path.join(self.testdir, 'ring-manager-server.conf')
        with open(path, 'w') as fp:
            fp.write(body)
        return path

    def _make_state_hook(self):
        hook_path = os.path.join(self.testdir, 'state-hook')
        log_path = os.path.join(self.testdir, 'state-hook.log')
        with open(hook_path, 'w') as fp:
            fp.write('#!/bin/sh\n')
            fp.write('printf "%s|%s\\n" "$RING_MANAGER_STATE_ACTION" '
                     '"$RING_MANAGER_STATE_RELPATH" >> "$1"\n')
        os.chmod(hook_path, 0o755)
        return hook_path, log_path

    def _read_recon(self):
        with open(os.path.join(
                self.recon_cache_path, RECON_RING_MANAGER_FILE)) as fp:
            return json.load(fp)

    def test_sync_latest_manifest_artifacts_and_ring_metadata(self):
        opener = FakeOpener(self._routes())
        logger = debug_logger()
        result = self._syncer(
            opener, time_func=lambda: NormalTimestamp(1700000000),
            logger=logger).sync()
        self.assertEqual({
            'latest_ring_version': 'release-1',
            'manifest_files_downloaded': 1,
            'manifest_files_unchanged': 0,
            'rings_synced': 1,
            'ring_versions_synced': 1,
            'ring_version_files_downloaded': 0,
            'ring_version_files_unchanged': 1,
            'builder_files_synced': 0,
            'builder_files_downloaded': 0,
            'builder_files_unchanged': 0,
            'builder_files_skipped_disabled': 0,
        }, result)

        artifact_path = os.path.join(
            self.artifact_dir, 'release-1', 'account.ring.gz')
        with open(artifact_path, 'rb') as fp:
            self.assertEqual(self.artifact_body, fp.read())

        with open(os.path.join(self.state_dir, 'index.json')) as fp:
            index = json.load(fp)
        self.assertEqual('release-1', index['latest_ring_version'])
        self.assertEqual('http://primary.example.com:6205',
                         index['ring_manager_sync']['source'])
        self.assertEqual('release-1',
                         index['ring_manager_sync']['latest_ring_version'])
        self.assertEqual('1700000000.00000',
                         index['ring_manager_sync']['synced_at'])

        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertTrue(recon_stats['success'])
        self.assertEqual('http://primary.example.com:6205',
                         recon_stats['source'])
        self.assertEqual('release-1',
                         recon_stats['latest_ring_version'])
        self.assertEqual({
            'action': 'none',
            'committed': False,
            'entries': 0,
            'pending': False,
            'recovered': False,
            'staged_builders': 0,
        }, recon_stats['sync_transaction'])
        self.assertEqual(1, recon_stats['manifest_files_downloaded'])
        self.assertEqual(0, recon_stats['builder_files_synced'])
        self.assertEqual(1, recon_stats['rings_synced'])
        self.assertEqual(0.0, recon_stats['sync_time'])
        self.assertEqual('1700000000.00000',
                         recon_stats['last_attempt'])
        self.assertEqual('1700000000.00000',
                         recon_stats['last_attempted_at'])
        self.assertEqual('1700000000.00000',
                         recon_stats['last_success'])
        self.assertEqual('1700000000.00000',
                         recon_stats['last_synced_at'])
        self.assertEqual({
            'needed': False,
            'reasons': [],
            'source_errors': 0,
            'sync_transaction': {
                'pending': False,
                'recovery_failed': False,
            },
        }, recon_stats['operator_attention'])
        self.assertNotIn('error', recon_stats)

        with open(os.path.join(
                self.state_dir, 'releases', 'release-1',
                'manifest.json')) as fp:
            manifest = json.load(fp)
        self.assertNotIn('latest', manifest)
        self.assertNotIn('resource_uri', manifest)
        self.assertNotIn('url', manifest['files'][0])
        self.assertEqual('release-1/account.ring.gz',
                         manifest['files'][0]['path'])

        with open(os.path.join(
                self.state_dir, 'rings', 'account.json')) as fp:
            ring = json.load(fp)
        self.assertEqual('account', ring['id'])
        self.assertNotIn('resource_uri', ring)

        with open(os.path.join(
                self.state_dir, 'ring-versions', 'account',
                '12.json')) as fp:
            ring_version = json.load(fp)
        self.assertNotIn('latest', ring_version)
        self.assertNotIn('resource_uri', ring_version)
        self.assertEqual('release-1/account.ring.gz',
                         ring_version['files'][0]['path'])

        app = RingManagerApplication({
            'ring_manager_state_dir': self.state_dir,
            'ring_artifact_dir': self.artifact_dir,
            'ring_manager_mode': 'readonly',
        }, logger=debug_logger())
        req = Request.blank('/api/v1/rings/releases/latest/manifest/')
        resp = req.get_response(app)
        self.assertEqual(200, resp.status_int)
        body = json.loads(resp.body.decode('ascii'))
        self.assertEqual('release-1', body['version'])
        self.assertEqual(True, body['latest'])
        self.assertNotIn('path', body['files'][0])

        req = Request.blank(
            '/api/v1/rings/releases/release-1/files/account.ring.gz')
        resp = req.get_response(app)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(self.artifact_body, resp.body)

        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['sync.attempts'])
        self.assertEqual(1, counts['sync.successes'])
        self.assertEqual(1, counts['sync.manifest_files.downloaded'])
        self.assertEqual(0, counts['sync.manifest_files.unchanged'])
        self.assertEqual(1, counts['sync.rings_synced'])
        self.assertEqual(1, counts['sync.ring_versions_synced'])
        self.assertEqual(0, counts['sync.ring_version_files.downloaded'])
        self.assertEqual(1, counts['sync.ring_version_files.unchanged'])
        self.assertEqual(0, counts['sync.builder_files.synced'])
        self.assertEqual(0, counts['sync.builder_files.downloaded'])
        self.assertEqual(0, counts['sync.builder_files.unchanged'])
        self.assertEqual(0, counts['sync.builder_files.skipped_disabled'])
        self.assertEqual(len(self.artifact_body),
                         counts['sync.bytes_downloaded'])
        self.assertIn(
            'sync.timing',
            [call[0][0] for call in logger.statsd_client.calls['timing']])

    def test_sync_builder_files_when_enabled(self):
        self.rings['objects'][0]['builder_files'] = [
            '/srv/primary/account.builder']
        builder_dir = os.path.join(self.testdir, 'builders')
        opener = FakeOpener(self._routes())
        logger = debug_logger()

        result = self._syncer(
            opener, builder_dir=builder_dir,
            sync_builder_files=True, logger=logger).sync()

        self.assertEqual(1, result['builder_files_synced'])
        self.assertEqual(1, result['builder_files_downloaded'])
        self.assertEqual(0, result['builder_files_unchanged'])
        local_builder = os.path.join(builder_dir, 'account.builder')
        self.assertTrue(os.path.exists(local_builder))
        self.assertEqual(self.builder_version,
                         RingBuilder.load(local_builder).version)
        with open(os.path.join(self.state_dir, 'rings', 'account.json')) as fp:
            ring = json.load(fp)
        self.assertEqual(['account.builder'], ring['builder_files'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['sync.builder_files.synced'])
        self.assertEqual(1, counts['sync.builder_files.downloaded'])
        self.assertEqual(0, counts['sync.builder_files.unchanged'])
        self.assertEqual(0, counts['sync.builder_files.skipped_disabled'])

        result = self._syncer(
            opener, builder_dir=builder_dir,
            sync_builder_files=True).sync()
        self.assertEqual(1, result['builder_files_synced'])
        self.assertEqual(0, result['builder_files_downloaded'])
        self.assertEqual(1, result['builder_files_unchanged'])

        builder_requests = [
            req for req in opener.requests
            if req['path'].startswith('/api/v1/rings/account/builder')]
        self.assertEqual([
            '/api/v1/rings/account/builder/',
            '/api/v1/rings/account/builder/file/',
            '/api/v1/rings/account/builder/',
            '/api/v1/rings/account/builder/file/',
        ], [req['path'] for req in builder_requests])
        for request in builder_requests:
            self.assertEqual(
                'secret', request['headers']['x-ring-manager-admin-key'])
        self.assertEqual(
            self.builder_md5,
            builder_requests[-1]['headers']['if-none-match'])

    def test_sync_builder_files_requires_builder_dir(self):
        with self.assertRaises(RingManagerSyncError) as cm:
            RingManagerSync(
                'http://primary.example.com:6205', self.state_dir,
                self.artifact_dir, admin_key='secret',
                sync_builder_files=True)
        self.assertIn('ring_builder_dir is required', str(cm.exception))

    def test_sync_builder_files_requires_admin_credentials(self):
        with self.assertRaises(RingManagerSyncError) as cm:
            RingManagerSync(
                'http://primary.example.com:6205', self.state_dir,
                self.artifact_dir, read_key='reader',
                builder_dir=os.path.join(self.testdir, 'builders'),
                sync_builder_files=True)
        self.assertIn('admin credentials are required', str(cm.exception))

    def test_sync_builder_files_skip_disabled_rings(self):
        self.rings['objects'][0]['disabled'] = True
        builder_dir = os.path.join(self.testdir, 'builders')
        opener = FakeOpener(self._routes())

        result = self._syncer(
            opener, builder_dir=builder_dir,
            sync_builder_files=True).sync()

        self.assertEqual(0, result['builder_files_synced'])
        self.assertEqual(1, result['builder_files_skipped_disabled'])
        self.assertFalse(os.path.exists(
            os.path.join(builder_dir, 'account.builder')))
        with open(os.path.join(self.state_dir, 'rings', 'account.json')) as fp:
            ring = json.load(fp)
        self.assertEqual(['account.builder'], ring['builder_files'])
        self.assertNotIn('builder_path', ring)
        self.assertFalse([
            req for req in opener.requests
            if req['path'].startswith('/api/v1/rings/account/builder')])

    def test_sync_builder_checksum_mismatch_does_not_update_latest(self):
        routes = self._routes()
        routes[('GET', '/api/v1/rings/account/builder/')] = json_response({
            'ring_id': 'account',
            'disabled': False,
            'builder_version': self.builder_version,
            'file': {
                'bytes': len(self.builder_body),
                'sha256': '0' * 64,
                'url': '/api/v1/rings/account/builder/file/',
            },
        })
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerSyncError) as cm:
            self._syncer(
                opener, builder_dir=os.path.join(self.testdir, 'builders'),
                sync_builder_files=True).sync()

        self.assertIn('sha256 mismatch', str(cm.exception))
        self.assertFalse(os.path.exists(
            os.path.join(self.state_dir, 'index.json')))
        self.assertFalse(os.path.exists(
            os.path.join(self.state_dir, 'rings', 'account.json')))

    def test_sync_builder_missing_integrity_metadata_fails_closed(self):
        routes = self._routes()
        routes[('GET', '/api/v1/rings/account/builder/')] = json_response({
            'ring_id': 'account',
            'disabled': False,
            'builder_version': self.builder_version,
            'file': {
                'bytes': len(self.builder_body),
                'url': '/api/v1/rings/account/builder/file/',
            },
        })
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerSyncError) as cm:
            self._syncer(
                opener, builder_dir=os.path.join(self.testdir, 'builders'),
                sync_builder_files=True).sync()

        self.assertIn('missing required field(s): sha256', str(cm.exception))
        self.assertFalse(os.path.exists(
            os.path.join(self.state_dir, 'index.json')))
        self.assertFalse(os.path.exists(
            os.path.join(self.state_dir, 'rings', 'account.json')))

    def test_sync_builder_local_failure_does_not_fall_back(self):
        routes = self._routes()
        routes[('GET', 'primary.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self.primary_status)
        routes[('GET', 'ring-ro.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self._replica_status())
        opener = FakeOpener(routes)

        with mock.patch.object(
                RingManagerSync, '_stage_builder_file',
                side_effect=RingManagerSyncLocalError(
                    'local builder write failed')):
            with self.assertRaises(RingManagerSyncLocalError):
                self._syncer(
                    opener,
                    builder_dir=os.path.join(self.testdir, 'builders'),
                    sync_builder_files=True,
                    source_url=[
                        'http://primary.example.com:6205',
                        'http://ring-ro.example.com:6205',
                    ]).sync()

        self.assertEqual(
            ['primary.example.com:6205'],
            sorted(set(request['host'] for request in opener.requests)))

    def test_sync_builder_duplicate_local_names_fail_closed(self):
        self.rings['objects'] = [
            {
                'id': 'account',
                'name': 'Account',
                'ring_type': 'account',
                'builder_files': ['/srv/primary/account.builder'],
            },
            {
                'id': 'account-copy',
                'name': 'Account Copy',
                'ring_type': 'account',
                'builder_files': ['/srv/other/account.builder'],
            },
        ]
        opener = FakeOpener(self._routes())

        with self.assertRaises(RingManagerSyncError) as cm:
            self._syncer(
                opener, builder_dir=os.path.join(self.testdir, 'builders'),
                sync_builder_files=True).sync()

        self.assertIn('map to the same local builder file account.builder',
                      str(cm.exception))
        self.assertFalse(os.path.exists(
            os.path.join(self.state_dir, 'index.json')))
        self.assertFalse(os.path.exists(
            os.path.join(self.state_dir, 'rings', 'account.json')))
        self.assertFalse([
            req for req in opener.requests
            if req['path'].startswith('/api/v1/rings/account/builder')])

    def test_sync_accepts_fresh_readonly_source(self):
        routes = self._routes()
        routes[('GET', '/api/v1/ring_manager/status/')] = json_response(
            self._replica_status())
        opener = FakeOpener(routes)
        self._syncer(
            opener, source_url='http://ring-ro.example.com:6205').sync()

        with open(os.path.join(self.state_dir, 'index.json')) as fp:
            index = json.load(fp)
        self.assertEqual('http://ring-ro.example.com:6205',
                         index['ring_manager_sync']['source'])
        self.assertEqual('1700000000.00000',
                         index['ring_manager_sync']['synced_at'])

    def test_sync_skips_stale_replica_and_falls_back_to_primary(self):
        routes = self._routes()
        routes[('GET', 'ring-ro.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self._replica_status(
                        can_serve_published_reads=False,
                        fresh=False, stale=True,
                        reasons=['freshness_threshold_exceeded']))
        routes[('GET', 'primary.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self.primary_status)
        opener = FakeOpener(routes)
        logger = debug_logger()
        result = self._syncer(
            opener, logger=logger,
            source_url=[
                'http://ring-ro.example.com:6205',
                'http://primary.example.com:6205',
            ]).sync()
        self.assertEqual('release-1', result['latest_ring_version'])

        with open(os.path.join(self.state_dir, 'index.json')) as fp:
            index = json.load(fp)
        self.assertEqual('http://primary.example.com:6205',
                         index['ring_manager_sync']['source'])

        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertTrue(recon_stats['success'])
        self.assertEqual('http://primary.example.com:6205',
                         recon_stats['source'])
        self.assertEqual([
            'http://ring-ro.example.com:6205',
            'http://primary.example.com:6205',
        ], recon_stats['sources'])
        self.assertEqual(1, len(recon_stats['source_errors']))
        self.assertEqual('http://ring-ro.example.com:6205',
                         recon_stats['source_errors'][0]['source'])
        self.assertIn('freshness_threshold_exceeded',
                      recon_stats['source_errors'][0]['error'])
        self.assertEqual({
            'needed': True,
            'reasons': ['source_errors'],
            'source_errors': 1,
            'sync_transaction': {
                'pending': False,
                'recovery_failed': False,
            },
        }, recon_stats['operator_attention'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['sync.source.failures'])
        self.assertEqual(1, counts['sync.operator_attention'])
        self.assertEqual(1, counts['sync.operator_attention.source_errors'])

    def test_sync_rejects_replica_missing_required_status_fields(self):
        routes = self._routes()
        routes[('GET', '/api/v1/ring_manager/status/')] = json_response({
            'mode': 'readonly',
            'ring_manager_sync': {
                'can_serve_published_reads': True,
            },
        })
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerSyncError) as caught:
            self._syncer(
                opener, source_url='http://ring-ro.example.com:6205').sync()
        self.assertIn('not_synced', str(caught.exception))
        self.assertIn('missing_latest_ring_version', str(caught.exception))
        self.assertIn('missing_last_synced_at', str(caught.exception))
        self.assertFalse(os.path.exists(os.path.join(
            self.state_dir, 'index.json')))

    def test_sync_rejects_replica_manifest_status_mismatch(self):
        routes = self._routes()
        routes[('GET', '/api/v1/ring_manager/status/')] = json_response(
            self._replica_status(latest_ring_version='release-other'))
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerSyncError) as caught:
            self._syncer(
                opener, source_url='http://ring-ro.example.com:6205').sync()
        self.assertIn('status latest version release-other',
                      str(caught.exception))
        self.assertFalse(os.path.exists(os.path.join(
            self.state_dir, 'index.json')))

    def test_sync_rejects_stale_replica_without_refreshing_state(self):
        routes = self._routes()
        routes[('GET', '/api/v1/ring_manager/status/')] = json_response(
            self._replica_status(
                can_serve_published_reads=False,
                fresh=False, stale=True,
                reasons=['freshness_threshold_exceeded']))
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerSyncError) as caught:
            self._syncer(
                opener, source_url='http://ring-ro.example.com:6205').sync()
        self.assertIn('cannot serve fresh published reads',
                      str(caught.exception))
        self.assertFalse(os.path.exists(os.path.join(
            self.state_dir, 'index.json')))
        self.assertEqual(
            ['/api/v1/ring_manager/status/'],
            [request['path'] for request in opener.requests])
        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertFalse(recon_stats['success'])
        self.assertIn('freshness_threshold_exceeded',
                      recon_stats['error'])

    def test_sync_aborts_on_local_failure_without_fallback(self):
        routes = self._routes()
        routes[('GET', 'primary.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self.primary_status)
        routes[('GET', 'ring-ro.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self._replica_status())
        opener = FakeOpener(routes)

        with mock.patch.object(
                RingManagerSync, '_commit_path_body',
                side_effect=RingManagerSyncLocalError('local write failed')):
            with self.assertRaises(RingManagerSyncLocalError):
                self._syncer(
                    opener,
                    source_url=[
                        'http://primary.example.com:6205',
                        'http://ring-ro.example.com:6205',
                    ]).sync()

        self.assertEqual(
            ['primary.example.com:6205'],
            sorted(set(request['host'] for request in opener.requests)))
        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertFalse(recon_stats['success'])
        self.assertNotIn('source_errors', recon_stats)
        self.assertIn('local write failed', recon_stats['error'])
        self.assertEqual(['sync_failed'],
                         recon_stats['operator_attention']['reasons'])
        self.assertTrue(recon_stats['operator_attention']['needed'])

    def test_sync_rolls_back_transaction_on_latest_write_failure(self):
        os.makedirs(os.path.join(self.state_dir, 'rings'))
        os.makedirs(os.path.join(self.state_dir, 'ring-versions', 'account'))
        os.makedirs(os.path.join(self.state_dir, 'releases', 'old-release'))
        old_ring = {
            'id': 'account',
            'name': 'Old Account',
            'ring_type': 'account',
            'builder_files': ['account.builder'],
        }
        with open(os.path.join(
                self.state_dir, 'rings', 'account.json'), 'w') as fp:
            json.dump(old_ring, fp)
        with open(os.path.join(self.state_dir, 'index.json'), 'w') as fp:
            json.dump({'latest_ring_version': 'old-release'}, fp)
        builder_dir = os.path.join(self.testdir, 'builders')
        os.makedirs(builder_dir)
        local_builder = os.path.join(builder_dir, 'account.builder')
        old_builder = RingBuilder(4, 3, 1)
        old_builder.save(local_builder)
        with open(local_builder, 'rb') as fp:
            old_builder_body = fp.read()
        hook_path, log_path = self._make_state_hook()

        opener = FakeOpener(self._routes())
        real_write = RingManagerSync._write_file_atomic

        def fail_latest(syncer, path, body):
            if path.endswith('index.json'):
                raise RingManagerSyncLocalError('latest write failed')
            return real_write(syncer, path, body)

        with mock.patch.object(
                RingManagerSync, '_write_file_atomic', fail_latest):
            with self.assertRaises(RingManagerSyncLocalError):
                self._syncer(
                    opener, builder_dir=builder_dir,
                    sync_builder_files=True,
                    state_change_hook='%s %s' % (hook_path, log_path),
                    state_change_hook_timeout=5).sync()

        with open(os.path.join(
                self.state_dir, 'rings', 'account.json')) as fp:
            self.assertEqual(old_ring, json.load(fp))
        with open(os.path.join(self.state_dir, 'index.json')) as fp:
            self.assertEqual(
                {'latest_ring_version': 'old-release'}, json.load(fp))
        self.assertFalse(os.path.exists(os.path.join(
            self.state_dir, 'ring-versions', 'account', '12.json')))
        self.assertFalse(os.path.exists(os.path.join(
            self.state_dir, 'releases', 'release-1', 'manifest.json')))
        with open(local_builder, 'rb') as fp:
            self.assertEqual(old_builder_body, fp.read())
        self.assertFalse([
            name for name in os.listdir(builder_dir)
            if '.sync-' in name or '.sync-backup-' in name])
        self.assertFalse(os.path.exists(log_path))
        self.assertFalse(os.path.exists(os.path.join(
            self.state_dir, RING_MANAGER_SYNC_JOURNAL)))

    def test_sync_recovers_partial_transaction_before_fetching_source(self):
        os.makedirs(os.path.join(self.state_dir, 'rings'))
        old_ring = {
            'id': 'account',
            'name': 'Old Account',
            'ring_type': 'account',
            'builder_files': ['account.builder'],
        }
        new_ring = dict(old_ring)
        new_ring['name'] = 'New Account'
        ring_path = os.path.join(self.state_dir, 'rings', 'account.json')
        ring_backup = '%s.sync-backup-test' % ring_path
        with open(ring_path, 'w') as fp:
            json.dump(new_ring, fp)
        with open(ring_backup, 'w') as fp:
            json.dump(old_ring, fp)

        builder_dir = os.path.join(self.testdir, 'builders')
        os.makedirs(builder_dir)
        builder_path = os.path.join(builder_dir, 'account.builder')
        old_builder = RingBuilder(4, 3, 1)
        old_builder.save('%s.sync-backup-test' % builder_path)
        with open('%s.sync-backup-test' % builder_path, 'rb') as fp:
            old_builder_body = fp.read()
        with open(builder_path, 'wb') as fp:
            fp.write(self.builder_body)
        staged_builder = '%s.sync-staged' % builder_path
        with open(staged_builder, 'wb') as fp:
            fp.write(self.builder_body)

        journal_path = os.path.join(
            self.state_dir, RING_MANAGER_SYNC_JOURNAL)
        with open(journal_path, 'w') as fp:
            json.dump({
                'committed': False,
                'entries': [
                    {
                        'kind': 'state',
                        'path': ring_path,
                        'backup_path': ring_backup,
                    },
                    {
                        'kind': 'builder',
                        'path': builder_path,
                        'backup_path': '%s.sync-backup-test' % builder_path,
                    },
                ],
                'staged_builders': [staged_builder],
            }, fp)

        opener = FakeOpener({
            ('GET', '/api/v1/ring_manager/status/'): json_response({}),
        })
        logger = debug_logger()
        with self.assertRaises(RingManagerSyncError):
            self._syncer(
                opener, logger=logger, builder_dir=builder_dir,
                sync_builder_files=True).sync()

        self.assertEqual(
            ['/api/v1/ring_manager/status/'],
            [request['path'] for request in opener.requests])
        with open(ring_path) as fp:
            self.assertEqual(old_ring, json.load(fp))
        self.assertFalse(os.path.exists(ring_backup))
        with open(builder_path, 'rb') as fp:
            self.assertEqual(old_builder_body, fp.read())
        self.assertFalse(os.path.exists('%s.sync-backup-test' % builder_path))
        self.assertFalse(os.path.exists(staged_builder))
        self.assertFalse(os.path.exists(journal_path))
        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertFalse(recon_stats['success'])
        self.assertEqual({
            'action': 'rollback',
            'committed': False,
            'entries': 2,
            'pending': True,
            'recovered': True,
            'staged_builders': 1,
        }, recon_stats['sync_transaction'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['sync.transaction.pending'])
        self.assertEqual(1, counts['sync.transaction.recoveries'])
        self.assertEqual(1, counts['sync.transaction.rollbacks'])

    def test_sync_recovers_committed_transaction_before_fetching_source(self):
        os.makedirs(os.path.join(self.state_dir, 'rings'))
        live_ring = {
            'id': 'account',
            'name': 'Committed Account',
            'ring_type': 'account',
            'builder_files': ['account.builder'],
        }
        old_ring = dict(live_ring, name='Old Account')
        ring_path = os.path.join(self.state_dir, 'rings', 'account.json')
        ring_backup = '%s.sync-backup-test' % ring_path
        with open(ring_path, 'w') as fp:
            json.dump(live_ring, fp)
        with open(ring_backup, 'w') as fp:
            json.dump(old_ring, fp)

        journal_path = os.path.join(
            self.state_dir, RING_MANAGER_SYNC_JOURNAL)
        with open(journal_path, 'w') as fp:
            json.dump({
                'committed': True,
                'entries': [{
                    'kind': 'state',
                    'path': ring_path,
                    'backup_path': ring_backup,
                }],
                'staged_builders': [],
            }, fp)

        opener = FakeOpener({
            ('GET', '/api/v1/ring_manager/status/'): json_response({}),
        })
        logger = debug_logger()
        with self.assertRaises(RingManagerSyncError):
            self._syncer(opener, logger=logger).sync()

        with open(ring_path) as fp:
            self.assertEqual(live_ring, json.load(fp))
        self.assertFalse(os.path.exists(ring_backup))
        self.assertFalse(os.path.exists(journal_path))
        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertFalse(recon_stats['success'])
        self.assertEqual({
            'action': 'cleanup',
            'committed': True,
            'entries': 1,
            'pending': True,
            'recovered': True,
            'staged_builders': 0,
        }, recon_stats['sync_transaction'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['sync.transaction.pending'])
        self.assertEqual(1, counts['sync.transaction.recoveries'])
        self.assertEqual(1, counts['sync.transaction.committed_cleanups'])

    def test_sync_records_recon_when_journal_recovery_fails(self):
        os.makedirs(self.state_dir)
        with open(os.path.join(self.state_dir, RING_MANAGER_SYNC_JOURNAL),
                  'w') as fp:
            json.dump([], fp)
        opener = FakeOpener(self._routes())
        logger = debug_logger()

        with self.assertRaises(RingManagerSyncLocalError) as cm:
            self._syncer(opener, logger=logger).sync()

        self.assertIn('sync journal must be an object', str(cm.exception))
        self.assertEqual([], opener.requests)
        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertFalse(recon_stats['success'])
        self.assertIn('sync journal must be an object',
                      recon_stats['error'])
        self.assertEqual({
            'action': 'failed',
            'committed': False,
            'entries': 0,
            'pending': True,
            'recovered': False,
            'recovery_failed': True,
            'staged_builders': 0,
        }, recon_stats['sync_transaction'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['sync.failures'])
        self.assertEqual(1, counts['sync.transaction.pending'])
        self.assertEqual(1, counts['sync.transaction.recovery_failures'])
        self.assertEqual([
            'sync_failed',
            'sync_transaction_recovery_failed',
        ], recon_stats['operator_attention']['reasons'])
        self.assertEqual(1, counts['sync.operator_attention'])
        self.assertEqual(
            1,
            counts['sync.operator_attention.'
                   'sync_transaction_recovery_failed'])

    def test_sync_aborts_on_invalid_local_index_without_fallback(self):
        os.makedirs(self.state_dir)
        with open(os.path.join(self.state_dir, 'index.json'), 'w') as fp:
            fp.write('[]')
        routes = self._routes()
        routes[('GET', 'primary.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self.primary_status)
        routes[('GET', 'ring-ro.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self._replica_status())
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerSyncLocalError) as caught:
            self._syncer(
                opener,
                source_url=[
                    'http://primary.example.com:6205',
                    'http://ring-ro.example.com:6205',
                ]).sync()
        self.assertIn('local index.json must be an object',
                      str(caught.exception))
        self.assertEqual(
            ['primary.example.com:6205'],
            sorted(set(request['host'] for request in opener.requests)))

    def test_sync_aborts_on_artifact_write_failure_without_fallback(self):
        routes = self._routes()
        routes[('GET', 'primary.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self.primary_status)
        routes[('GET', 'ring-ro.example.com:6205',
                '/api/v1/ring_manager/status/')] = json_response(
                    self._replica_status())
        opener = FakeOpener(routes)
        logger = debug_logger()

        with mock.patch.object(
                RingManagerSync, '_write_file_atomic',
                side_effect=RingManagerSyncLocalError(
                    'artifact write failed')):
            with self.assertRaises(RingManagerSyncLocalError):
                self._syncer(
                    opener, logger=logger,
                    source_url=[
                        'http://primary.example.com:6205',
                        'http://ring-ro.example.com:6205',
                    ]).sync()

        self.assertEqual(
            ['primary.example.com:6205'],
            sorted(set(request['host'] for request in opener.requests)))
        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertFalse(recon_stats['success'])
        self.assertNotIn('source_errors', recon_stats)
        self.assertIn('artifact write failed', recon_stats['error'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['sync.failures'])
        self.assertNotIn('sync.source.failures', counts)

    def test_sync_uses_conditional_request_for_existing_artifact(self):
        local_path = os.path.join(
            self.artifact_dir, 'release-1', 'account.ring.gz')
        os.makedirs(os.path.dirname(local_path))
        with open(local_path, 'wb') as fp:
            fp.write(self.artifact_body)

        opener = FakeOpener(self._routes())
        result = self._syncer(opener).sync()
        self.assertEqual(0, result['manifest_files_downloaded'])
        self.assertEqual(1, result['manifest_files_unchanged'])

        file_requests = [
            req for req in opener.requests
            if req['path'].endswith('/files/account.ring.gz')]
        self.assertEqual(2, len(file_requests))
        for req in file_requests:
            self.assertEqual(self.artifact_md5,
                             req['headers']['if-none-match'])
            self.assertEqual('secret',
                             req['headers']['x-ring-manager-admin-key'])
            self.assertEqual(12, req['timeout'])

    def test_sync_uses_read_key_when_configured(self):
        opener = FakeOpener(self._routes())

        self._syncer(opener, read_key='reader').sync()

        for request in opener.requests:
            self.assertEqual(
                'reader', request['headers']['x-ring-manager-read-key'])
            self.assertNotIn('x-ring-manager-admin-key',
                             request['headers'])

    def test_sync_uses_read_key_file_when_configured(self):
        opener = FakeOpener(self._routes())

        self._syncer(
            opener, read_key_file=self._write_secret(
                'read.key', b'reader\n'),
            admin_key_file=self._write_secret(
                'admin.key', b'admin\n'),
            admin_key=None).sync()

        for request in opener.requests:
            self.assertEqual(
                'reader', request['headers']['x-ring-manager-read-key'])
            self.assertNotIn('x-ring-manager-admin-key',
                             request['headers'])

    def test_sync_rejects_inline_and_file_key_configuration(self):
        opener = FakeOpener(self._routes())

        with self.assertRaises(RingManagerSyncError) as cm:
            self._syncer(
                opener, read_key='reader',
                read_key_file=self._write_secret('read.key', b'reader\n'))

        self.assertIn('mutually exclusive', str(cm.exception))

    def test_sync_uses_read_auth_token_when_configured(self):
        opener = FakeOpener(self._routes())

        self._syncer(
            opener, auth_token='admin-token',
            read_auth_token='reader-token').sync()

        for request in opener.requests:
            self.assertEqual('reader-token',
                             request['headers']['x-auth-token'])
            self.assertNotIn('x-ring-manager-admin-key',
                             request['headers'])

    def test_sync_rejects_source_url_without_scheme(self):
        with self.assertRaises(RingManagerSyncError) as caught:
            RingManagerSync(
                'primary.example.com:6205', self.state_dir,
                self.artifact_dir)
        self.assertIn('source_url must be an http(s) URL',
                      str(caught.exception))

    def test_main_reads_sync_config_section(self):
        conf_path = self._write_config('''
[DEFAULT]
log_statsd_host = 127.0.0.1

[ring-manager-sync]
source_urls = http://primary.example.com:6205
ring_manager_state_dir = %s
ring_artifact_dir = %s
recon_cache_path = %s
request_timeout = 9
read_key = reader
''' % (self.state_dir, self.artifact_dir, self.recon_cache_path))
        opener = FakeOpener(self._routes())
        logger_confs = []

        def fake_get_logger(conf, *args, **kwargs):
            logger_confs.append(conf)
            return debug_logger()

        with mock.patch('swift.ring_manager.sync.urllib_request.urlopen',
                        opener), \
                mock.patch('swift.ring_manager.sync.get_logger',
                           fake_get_logger), \
                mock.patch('sys.stdout', io.StringIO()):
            status = sync.main([conf_path])

        self.assertEqual(0, status)
        self.assertEqual('127.0.0.1', logger_confs[0]['log_statsd_host'])
        self.assertEqual(9.0, opener.requests[0]['timeout'])
        for request in opener.requests:
            self.assertEqual(
                'reader', request['headers']['x-ring-manager-read-key'])
        with open(os.path.join(self.state_dir, 'index.json')) as fp:
            index = json.load(fp)
        self.assertEqual('release-1', index['latest_ring_version'])

    def test_main_reads_builder_sync_config_section(self):
        builder_dir = os.path.join(self.testdir, 'builders')
        conf_path = self._write_config('''
[ring-manager-sync]
source_urls = http://primary.example.com:6205
ring_manager_state_dir = %s
ring_artifact_dir = %s
ring_builder_dir = %s
sync_builder_files = true
admin_key = secret
''' % (self.state_dir, self.artifact_dir, builder_dir))
        opener = FakeOpener(self._routes())

        with mock.patch('swift.ring_manager.sync.urllib_request.urlopen',
                        opener), \
                mock.patch('sys.stdout', io.StringIO()):
            status = sync.main([conf_path])

        self.assertEqual(0, status)
        self.assertTrue(os.path.exists(
            os.path.join(builder_dir, 'account.builder')))
        builder_requests = [
            request for request in opener.requests
            if request['path'].startswith('/api/v1/rings/account/builder')]
        self.assertEqual(2, len(builder_requests))
        for request in builder_requests:
            self.assertEqual(
                'secret', request['headers']['x-ring-manager-admin-key'])

    def test_main_cli_disables_configured_builder_sync(self):
        builder_dir = os.path.join(self.testdir, 'builders')
        conf_path = self._write_config('''
[ring-manager-sync]
source_urls = http://primary.example.com:6205
ring_manager_state_dir = %s
ring_artifact_dir = %s
ring_builder_dir = %s
sync_builder_files = true
admin_key = secret
''' % (self.state_dir, self.artifact_dir, builder_dir))
        opener = FakeOpener(self._routes())

        with mock.patch('swift.ring_manager.sync.urllib_request.urlopen',
                        opener), \
                mock.patch('sys.stdout', io.StringIO()):
            status = sync.main([
                '--config', conf_path,
                '--no-sync-builder-files',
            ])

        self.assertEqual(0, status)
        self.assertFalse(os.path.exists(
            os.path.join(builder_dir, 'account.builder')))
        self.assertFalse([
            request for request in opener.requests
            if request['path'].startswith('/api/v1/rings/account/builder')])

    def test_main_cli_options_override_sync_config_section(self):
        conf_path = self._write_config('''
[ring-manager-sync]
source_urls = http://unused.example.com:6205
ring_manager_state_dir = /does/not/matter
ring_artifact_dir = /does/not/matter
recon_cache_path = /does/not/matter
request_timeout = 1
read_key = from-config
''')
        opener = FakeOpener(self._routes())

        with mock.patch('swift.ring_manager.sync.urllib_request.urlopen',
                        opener), \
                mock.patch('sys.stdout', io.StringIO()):
            status = sync.main([
                '--config', conf_path,
                '--ring-manager-state-dir', self.state_dir,
                '--ring-artifact-dir', self.artifact_dir,
                '--recon-cache-path', self.recon_cache_path,
                '--timeout', '12',
                '--read-key', 'from-cli',
                'http://primary.example.com:6205',
            ])

        self.assertEqual(0, status)
        self.assertEqual(
            ['primary.example.com:6205'],
            sorted(set(request['host'] for request in opener.requests)))
        self.assertEqual(12.0, opener.requests[0]['timeout'])
        for request in opener.requests:
            self.assertEqual(
                'from-cli', request['headers']['x-ring-manager-read-key'])

    def test_main_cli_key_options_override_config_key_files(self):
        config_values = {
            'state_dir': self.state_dir,
            'artifact_dir': self.artifact_dir,
            'recon_cache_path': self.recon_cache_path,
            'read_key_file': self._write_secret(
                'config-read.key', b'from-config\n'),
        }
        conf_path = self._write_config('''
[ring-manager-sync]
source_urls = http://primary.example.com:6205
ring_manager_state_dir = %(state_dir)s
ring_artifact_dir = %(artifact_dir)s
recon_cache_path = %(recon_cache_path)s
read_key_file = %(read_key_file)s
''' % config_values)
        opener = FakeOpener(self._routes())

        with mock.patch('swift.ring_manager.sync.urllib_request.urlopen',
                        opener), \
                mock.patch('sys.stdout', io.StringIO()):
            status = sync.main([
                '--config', conf_path,
                '--read-key', 'from-cli',
            ])

        self.assertEqual(0, status)
        for request in opener.requests:
            self.assertEqual(
                'from-cli', request['headers']['x-ring-manager-read-key'])

    def test_main_cli_auth_token_overrides_config_read_auth_token(self):
        conf_path = self._write_config('''
[ring-manager-sync]
source_urls = http://primary.example.com:6205
ring_manager_state_dir = %s
ring_artifact_dir = %s
recon_cache_path = %s
read_auth_token = from-config-read
''' % (self.state_dir, self.artifact_dir, self.recon_cache_path))
        opener = FakeOpener(self._routes())

        with mock.patch('swift.ring_manager.sync.urllib_request.urlopen',
                        opener), \
                mock.patch('sys.stdout', io.StringIO()):
            status = sync.main([
                '--config', conf_path,
                '--auth-token', 'from-cli-admin',
            ])

        self.assertEqual(0, status)
        for request in opener.requests:
            self.assertEqual(
                'from-cli-admin', request['headers']['x-auth-token'])

    def test_main_recon_dump_cli_overrides_config_false(self):
        conf_path = self._write_config('''
[ring-manager-sync]
source_urls = http://primary.example.com:6205
ring_manager_state_dir = %s
ring_artifact_dir = %s
recon_cache_path = %s
recon_dump = false
read_key = reader
''' % (self.state_dir, self.artifact_dir, self.recon_cache_path))
        opener = FakeOpener(self._routes())

        with mock.patch('swift.ring_manager.sync.urllib_request.urlopen',
                        opener), \
                mock.patch('sys.stdout', io.StringIO()):
            status = sync.main(['--config', conf_path, '--recon-dump'])

        self.assertEqual(0, status)
        self.assertEqual('release-1',
                         self._read_recon()[
                             'ring_manager_sync']['latest_ring_version'])

    def test_main_reports_missing_positional_config_path(self):
        conf_path = os.path.join(self.testdir, 'missing.conf')

        with mock.patch('sys.stderr', io.StringIO()) as stderr:
            status = sync.main([conf_path])

        self.assertEqual(1, status)
        self.assertIn('config file not found', stderr.getvalue())

    def test_main_reports_missing_sync_config_section(self):
        conf_path = self._write_config('[DEFAULT]\n')

        with mock.patch('sys.stderr', io.StringIO()) as stderr:
            status = sync.main([conf_path])

        self.assertEqual(1, status)
        self.assertIn('ring-manager-sync', stderr.getvalue())

    def test_main_reports_invalid_logger_config(self):
        conf_path = self._write_config('''
[ring-manager-sync]
source_urls = http://primary.example.com:6205
ring_manager_state_dir = %s
ring_artifact_dir = %s
log_statsd_port = not-an-int
''' % (self.state_dir, self.artifact_dir))

        with mock.patch('sys.stderr', io.StringIO()) as stderr:
            status = sync.main([conf_path])

        self.assertEqual(1, status)
        self.assertIn('invalid logger config', stderr.getvalue())

    def test_main_rejects_source_url_without_scheme(self):
        with mock.patch('sys.stderr', io.StringIO()) as stderr:
            status = sync.main([
                'primary.example.com:6205',
                '--ring-manager-state-dir', self.state_dir,
                '--ring-artifact-dir', self.artifact_dir,
            ])

        self.assertEqual(1, status)
        self.assertIn('source_url must be an http(s) URL',
                      stderr.getvalue())

    def test_sync_rejects_cross_origin_artifact_url(self):
        manifest = dict(self.manifest)
        manifest['files'] = [dict(
            self.manifest['files'][0],
            url='https://evil.example.com/steal')]
        routes = self._routes()
        routes[('GET', '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(manifest)
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerSyncError) as cm:
            self._syncer(opener).sync()
        self.assertIn('same-origin relative URL', str(cm.exception))
        self.assertEqual(2, len(opener.requests))

    def test_sync_rejects_artifact_url_dot_segments(self):
        manifest = dict(self.manifest)
        manifest['files'] = [dict(
            self.manifest['files'][0],
            url='/api/v1/../../healthcheck')]
        routes = self._routes()
        routes[('GET', '/api/v1/rings/releases/latest/manifest/')] = \
            json_response(manifest)
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerSyncError) as cm:
            self._syncer(opener).sync()
        self.assertIn('dot segments', str(cm.exception))
        self.assertEqual(2, len(opener.requests))

    def test_sync_parser_accepts_read_credentials(self):
        from swift.ring_manager.sync import _make_parser

        options, args = _make_parser().parse_args([
            'https://primary.example.com:6205',
            '--ring-manager-state-dir', self.state_dir,
            '--ring-artifact-dir', self.artifact_dir,
            '--read-key', 'reader',
            '--read-auth-token', 'reader-token',
        ])

        self.assertEqual(['https://primary.example.com:6205'], args)
        self.assertEqual('reader', options.read_key)
        self.assertEqual('reader-token', options.read_auth_token)

    def test_sync_parser_accepts_key_files(self):
        from swift.ring_manager.sync import _make_parser

        options, args = _make_parser().parse_args([
            'https://primary.example.com:6205',
            '--ring-manager-state-dir', self.state_dir,
            '--ring-artifact-dir', self.artifact_dir,
            '--admin-key-file', '/etc/swift/secrets/admin.key',
            '--read-key-file', '/etc/swift/secrets/read.key',
        ])

        self.assertEqual(['https://primary.example.com:6205'], args)
        self.assertEqual('/etc/swift/secrets/admin.key',
                         options.admin_key_file)
        self.assertEqual('/etc/swift/secrets/read.key', options.read_key_file)

    def test_sync_parser_accepts_multiple_sources(self):
        from swift.ring_manager.sync import _make_parser

        _options, args = _make_parser().parse_args([
            'https://primary.example.com:6205,'
            'https://ring-ro.example.com:6205',
            'https://ring-standby.example.com:6205',
            '--ring-manager-state-dir', self.state_dir,
            '--ring-artifact-dir', self.artifact_dir,
        ])

        self.assertEqual([
            'https://primary.example.com:6205,'
            'https://ring-ro.example.com:6205',
            'https://ring-standby.example.com:6205',
        ], args)

    def test_sync_normalizes_comma_separated_sources(self):
        syncer = self._syncer(
            FakeOpener(self._routes()),
            source_url=[
                'http://primary.example.com:6205,'
                'http://ring-ro.example.com:6205',
                'http://ring-standby.example.com:6205',
            ])

        self.assertEqual([
            'http://primary.example.com:6205',
            'http://ring-ro.example.com:6205',
            'http://ring-standby.example.com:6205',
        ], syncer.source_urls)

    def test_sync_main_passes_multiple_sources(self):
        with mock.patch.object(sync, 'RingManagerSync') as syncer:
            syncer.return_value.sync.return_value = {
                'latest_ring_version': 'release-1',
                'rings_synced': 1,
                'ring_versions_synced': 1,
                'manifest_files_downloaded': 1,
                'manifest_files_unchanged': 0,
            }
            status = sync.main([
                'https://primary.example.com:6205',
                'https://ring-ro.example.com:6205',
                '--ring-manager-state-dir', self.state_dir,
                '--ring-artifact-dir', self.artifact_dir,
                '--quiet',
            ])

        self.assertEqual(0, status)
        self.assertEqual([
            'https://primary.example.com:6205',
            'https://ring-ro.example.com:6205',
        ], syncer.call_args[0][0])

    def test_sync_main_cli_overrides_sync_lock_timeout(self):
        conf_path = self._write_config('''
[ring-manager-sync]
source_urls = https://primary.example.com:6205
ring_manager_state_dir = %s
ring_artifact_dir = %s
sync_lock_timeout = 9
''' % (self.state_dir, self.artifact_dir))
        with mock.patch.object(sync, 'RingManagerSync') as syncer:
            syncer.return_value.sync.return_value = {
                'latest_ring_version': 'release-1',
            }
            status = sync.main([
                '--config', conf_path,
                '--sync-lock-timeout', '3',
                '--quiet',
            ])

        self.assertEqual(0, status)
        self.assertEqual(3.0, syncer.call_args[1]['sync_lock_timeout'])

    def test_sync_main_passes_read_credentials(self):
        with mock.patch.object(sync, 'RingManagerSync') as syncer:
            syncer.return_value.sync.return_value = {
                'latest_ring_version': 'release-1',
                'rings_synced': 1,
                'ring_versions_synced': 1,
                'manifest_files_downloaded': 1,
                'manifest_files_unchanged': 0,
            }
            status = sync.main([
                'https://primary.example.com:6205',
                '--ring-manager-state-dir', self.state_dir,
                '--ring-artifact-dir', self.artifact_dir,
                '--read-key', 'reader',
                '--read-auth-token', 'reader-token',
                '--quiet',
            ])

        self.assertEqual(0, status)
        self.assertEqual('reader', syncer.call_args[1]['read_key'])
        self.assertEqual('reader-token',
                         syncer.call_args[1]['read_auth_token'])

    def test_sync_main_passes_key_files(self):
        with mock.patch.object(sync, 'RingManagerSync') as syncer:
            syncer.return_value.sync.return_value = {
                'latest_ring_version': 'release-1',
                'rings_synced': 1,
                'ring_versions_synced': 1,
                'manifest_files_downloaded': 1,
                'manifest_files_unchanged': 0,
            }
            status = sync.main([
                'https://primary.example.com:6205',
                '--ring-manager-state-dir', self.state_dir,
                '--ring-artifact-dir', self.artifact_dir,
                '--admin-key-file', '/etc/swift/secrets/admin.key',
                '--read-key-file', '/etc/swift/secrets/read.key',
                '--quiet',
            ])

        self.assertEqual(0, status)
        self.assertEqual('/etc/swift/secrets/admin.key',
                         syncer.call_args[1]['admin_key_file'])
        self.assertEqual('/etc/swift/secrets/read.key',
                         syncer.call_args[1]['read_key_file'])

    def test_sync_runs_state_change_hook_for_json_writes(self):
        hook_path, log_path = self._make_state_hook()
        syncer = RingManagerSync(
            'http://primary.example.com:6205',
            self.state_dir,
            self.artifact_dir,
            admin_key='secret',
            timeout=12,
            opener=FakeOpener(self._routes()),
            recon_cache_path=self.recon_cache_path,
            state_change_hook='%s %s' % (hook_path, log_path),
            state_change_hook_timeout=5,
            logger=debug_logger())

        syncer.sync()

        with open(log_path) as fp:
            lines = [line.rstrip('\n') for line in fp]
        self.assertEqual([
            'write|rings/account.json',
            'write|ring-versions/account/12.json',
            'write|releases/release-1/manifest.json',
            'write|index.json',
        ], lines)

    def test_sync_rejects_bad_checksum(self):
        os.makedirs(self.state_dir)
        with open(os.path.join(self.state_dir, 'index.json'), 'w') as fp:
            json.dump({'latest_ring_version': 'release-old'}, fp)
        routes = self._routes()
        routes[('GET', '/api/v1/rings/releases/release-1/files/'
                'account.ring.gz')] = FakeResponse(
                    b'x' * len(self.artifact_body))
        opener = FakeOpener(routes)
        logger = debug_logger()

        with self.assertRaises(RingManagerSyncError) as cm:
            self._syncer(
                opener, time_func=lambda: NormalTimestamp(1700000123),
                logger=logger).sync()
        self.assertIn('sha256 mismatch', str(cm.exception))
        with open(os.path.join(self.state_dir, 'index.json')) as fp:
            self.assertEqual(
                {'latest_ring_version': 'release-old'}, json.load(fp))

        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertFalse(recon_stats['success'])
        self.assertEqual('http://primary.example.com:6205',
                         recon_stats['source'])
        self.assertIn('sha256 mismatch', recon_stats['error'])
        self.assertEqual(0.0, recon_stats['sync_time'])
        self.assertEqual('1700000123.00000',
                         recon_stats['last_attempt'])
        self.assertEqual('1700000123.00000',
                         recon_stats['last_attempted_at'])
        counts = logger.statsd_client.get_stats_counts()
        self.assertEqual(1, counts['sync.attempts'])
        self.assertEqual(1, counts['sync.failures'])


if __name__ == '__main__':
    unittest.main()
