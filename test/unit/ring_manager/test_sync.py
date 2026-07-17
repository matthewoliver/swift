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

from urllib.parse import urlparse

from swift.common.recon import RECON_RING_MANAGER_FILE
from swift.common.swob import Request
from swift.common.utils import md5
from swift.ring_manager.common import NormalTimestamp
from swift.ring_manager.server import RingManagerApplication
from swift.ring_manager.sync import RingManagerSync, RingManagerSyncError
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
            'path': parsed.path,
            'headers': headers,
            'timeout': timeout,
        }
        self.requests.append(request)
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

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def _routes(self):
        def artifact(request):
            if request['headers'].get('If-none-match'.lower()) == \
                    self.artifact_md5:
                return FakeResponse(status=304)
            return FakeResponse(self.artifact_body)

        return {
            ('GET', '/api/v1/rings/releases/latest/manifest/'):
                json_response(self.manifest),
            ('GET', '/api/v1/rings/releases/release-1/files/'
             'account.ring.gz'): artifact,
            ('GET', '/api/v1/rings/'): json_response(self.rings),
            ('GET', '/api/v1/rings/account/versions/12/'):
                json_response(self.ring_version),
            ('GET', '/api/v1/rings/account/versions/12/files/'
             'account.ring.gz'): artifact,
        }

    def _syncer(self, opener, time_func=NormalTimestamp.now):
        return RingManagerSync(
            'http://primary.example.com:6205',
            self.state_dir,
            self.artifact_dir,
            admin_key='secret',
            timeout=12,
            opener=opener,
            recon_cache_path=self.recon_cache_path,
            logger=debug_logger(),
            time_func=time_func)

    def _read_recon(self):
        with open(os.path.join(
                self.recon_cache_path, RECON_RING_MANAGER_FILE)) as fp:
            return json.load(fp)

    def test_sync_latest_manifest_artifacts_and_ring_metadata(self):
        opener = FakeOpener(self._routes())
        result = self._syncer(
            opener, time_func=lambda: NormalTimestamp(1700000000)).sync()
        self.assertEqual({
            'latest_ring_version': 'release-1',
            'manifest_files_downloaded': 1,
            'manifest_files_unchanged': 0,
            'rings_synced': 1,
            'ring_versions_synced': 1,
            'ring_version_files_downloaded': 0,
            'ring_version_files_unchanged': 1,
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
        self.assertEqual('1700000000.00000',
                         index['ring_manager_sync']['synced_at'])

        recon_stats = self._read_recon()['ring_manager_sync']
        self.assertTrue(recon_stats['success'])
        self.assertEqual('http://primary.example.com:6205',
                         recon_stats['source'])
        self.assertEqual('release-1',
                         recon_stats['latest_ring_version'])
        self.assertEqual(1, recon_stats['manifest_files_downloaded'])
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

    def test_sync_rejects_bad_checksum(self):
        os.makedirs(self.state_dir)
        with open(os.path.join(self.state_dir, 'index.json'), 'w') as fp:
            json.dump({'latest_ring_version': 'release-old'}, fp)
        routes = self._routes()
        routes[('GET', '/api/v1/rings/releases/release-1/files/'
                'account.ring.gz')] = FakeResponse(
                    b'x' * len(self.artifact_body))
        opener = FakeOpener(routes)

        with self.assertRaises(RingManagerSyncError) as cm:
            self._syncer(
                opener, time_func=lambda: NormalTimestamp(1700000123)).sync()
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


if __name__ == '__main__':
    unittest.main()
