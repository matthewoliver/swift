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

import hashlib
import io
import json
import os
import shutil
import tempfile
import unittest

from urllib.parse import urlparse

from swift.cli import ring_manager as cli


RING_MANAGER_ENV_VARS = (
    'SWIFT_RING_MANAGER_URL',
    'RING_MANAGER_URL',
    'SWIFT_RING_MANAGER_ADMIN_KEY',
    'RING_MANAGER_ADMIN_KEY',
    'SWIFT_RING_MANAGER_READ_KEY',
    'RING_MANAGER_READ_KEY',
)


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
        request = {
            'method': req.get_method(),
            'path': parsed.path,
            'query': parsed.query,
            'headers': dict((key.lower(), value)
                            for key, value in req.header_items()),
            'body': req.data,
            'timeout': timeout,
        }
        self.requests.append(request)
        route = self.routes[(request['method'], request['path'])]
        return route(request) if callable(route) else route


def json_response(value):
    return FakeResponse(json.dumps(value).encode('ascii'))


class TestRingManagerCLI(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.saved_env = dict(
            (key, os.environ[key]) for key in RING_MANAGER_ENV_VARS
            if key in os.environ)
        for key in RING_MANAGER_ENV_VARS:
            os.environ.pop(key, None)

    def tearDown(self):
        for key in RING_MANAGER_ENV_VARS:
            os.environ.pop(key, None)
        os.environ.update(self.saved_env)
        shutil.rmtree(self.testdir)

    def _write_file(self, name, body):
        path = os.path.join(self.testdir, name)
        with open(path, 'w') as fp:
            fp.write(body)
        return path

    def _run(self, argv, opener):
        stdout = io.StringIO()
        stderr = io.StringIO()
        status = cli.main(argv, opener=opener, stdout=stdout, stderr=stderr)
        return status, stdout.getvalue(), stderr.getvalue()

    def test_load_devices_payload_expands_node_inventory(self):
        path = self._write_file('devices.yaml', """
nodes:
  - id: node-42
    region: 2
    zone: 7
    ip: 10.0.7.42
    port: 6200
    replication_ip: 10.1.7.42
    replication_port: 6200
    devices:
      - name: sdb
        weight: 100.0
""")
        self.assertEqual({
            'devices': [{
                'label': 'node-42:sdb',
                'node_id': 'node-42',
                'region': 2,
                'zone': 7,
                'ip': '10.0.7.42',
                'port': 6200,
                'replication_ip': '10.1.7.42',
                'replication_port': 6200,
                'device': 'sdb',
                'weight': 100.0,
            }],
        }, cli.load_devices_payload(path))

    def test_parse_device_add_values_accepts_builder_shorthand(self):
        payload = cli.parse_device_add_values([
            'd7r2z3-10.0.0.7:6200R10.1.0.7:6200/sdb_fast', '123.5',
        ])
        self.assertEqual({
            'devices': [{
                'id': 7,
                'region': 2,
                'zone': 3,
                'ip': '10.0.0.7',
                'port': 6200,
                'replication_ip': '10.1.0.7',
                'replication_port': 6200,
                'device': 'sdb',
                'meta': 'fast',
                'weight': 123.5,
            }],
        }, payload)

    def test_parse_device_add_values_requires_weight_pairs(self):
        with self.assertRaises(cli.RingManagerCLIError) as raised:
            cli.parse_device_add_values(['r1z1-10.0.0.1:6200/sdb'])
        self.assertIn('DEVICE_SPEC WEIGHT pairs', str(raised.exception))

    def test_parse_device_search_values_accepts_builder_selectors(self):
        self.assertEqual({
            'devices': [
                {'id': 7},
                {
                    'region': 2,
                    'zone': 3,
                    'ip': '10.0.0.7',
                    'port': 6200,
                    'device': 'sdb',
                    'meta': 'fast',
                },
            ],
        }, cli.parse_device_search_values([
            'd7', 'r2z3-10.0.0.7:6200/sdb_fast',
        ]))

    def test_status_sends_admin_auth_header(self):
        opener = FakeOpener({
            ('GET', '/api/v1/ring_manager/status/'): json_response({
                'mode': 'primary',
                'writable': True,
            }),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            '--admin-key', 'secret', 'status',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual({'mode': 'primary', 'writable': True},
                         json.loads(stdout))
        self.assertEqual(
            'secret', opener.requests[0]['headers']
            ['x-ring-manager-admin-key'])

    def test_read_key_is_preferred_for_read_requests(self):
        opener = FakeOpener({
            ('GET', '/api/v1/ring_manager/status/'): json_response({}),
        })
        status, _stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            '--admin-key', 'secret', '--read-key', 'reader', 'status',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual(
            'reader', opener.requests[0]['headers']['x-ring-manager-read-key'])
        self.assertNotIn('x-ring-manager-admin-key',
                         opener.requests[0]['headers'])

    def test_status_promotion_ready_prints_summary(self):
        body = {
            'mode': 'standby',
            'writable': False,
            'latest_ring_version': 'release-42',
            'ring_manager_sync': {
                'source': 'https://primary.example.com:6205',
                'latest_ring_version': 'release-42',
                'last_synced_at': '1700000000.00000',
                'age_seconds': 12.5,
                'freshness_threshold': 300,
                'synced': True,
                'fresh': True,
                'stale': False,
                'reasons': [],
                'sync_transaction': {'pending': False},
            },
            'promotion_readiness': {
                'applicable': True,
                'ready': True,
                'published_state': {
                    'ready': True,
                    'blockers': [],
                },
                'builders': {
                    'ready': True,
                    'required': 3,
                    'checked': 3,
                    'missing': [],
                    'invalid': [],
                    'version_mismatches': [],
                    'skipped_disabled': ['object-2'],
                    'blockers': [],
                },
                'blockers': [],
            },
            'operator_attention': {
                'needed': False,
                'reasons': [],
            },
        }
        opener = FakeOpener({
            ('GET', '/api/v1/ring_manager/status/'): json_response(body),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://standby.example.com:6205',
            'status', '--promotion',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual('promotion=true', opener.requests[0]['query'])
        self.assertIn('Mode: standby', stdout)
        self.assertIn('Promotion readiness: ready', stdout)
        self.assertIn('Published state: ready blockers=none', stdout)
        self.assertIn(
            'Builders: ready required=3 checked=3 skipped_disabled=object-2',
            stdout)
        self.assertIn('Attention: no reasons=none', stdout)
        self.assertIn('Blockers: none', stdout)
        with self.assertRaises(ValueError):
            json.loads(stdout)

    def test_status_promotion_blocked_prints_blockers_and_exits_one(self):
        body = {
            'mode': 'standby',
            'writable': False,
            'latest_ring_version': 'release-42',
            'ring_manager_sync': {
                'source': 'https://primary.example.com:6205',
                'latest_ring_version': 'release-42',
                'age_seconds': 15,
                'freshness_threshold': 300,
                'synced': True,
                'fresh': False,
                'stale': True,
                'reasons': ['sync_transaction_pending'],
                'sync_transaction': {'pending': True},
            },
            'promotion_readiness': {
                'applicable': True,
                'ready': False,
                'published_state': {
                    'ready': False,
                    'blockers': ['sync_transaction_pending'],
                },
                'builders': {
                    'ready': False,
                    'required': 2,
                    'checked': 1,
                    'missing': ['object-1'],
                    'invalid': [{
                        'ring_id': 'object-2',
                        'reason': 'builder_unloadable',
                    }],
                    'error': 'unable to read rings: invalid JSON',
                    'version_mismatches': [{
                        'ring_id': 'object-0',
                        'expected': 9,
                        'actual': 8,
                    }],
                    'unpublished_builder_changes': [{
                        'ring_id': 'object-3',
                        'published': 4,
                        'actual': 5,
                    }],
                    'skipped_disabled': [],
                    'blockers': [
                        'missing_builder_files',
                        'invalid_builder_files',
                        'builder_version_mismatch',
                        'unpublished_builder_changes',
                    ],
                },
                'blockers': [
                    'published_state_not_ready',
                    'missing_builder_files',
                    'invalid_builder_files',
                    'builder_version_mismatch',
                    'unpublished_builder_changes',
                ],
            },
            'operator_attention': {
                'needed': True,
                'reasons': ['sync_not_fresh', 'promotion_not_ready'],
            },
        }
        opener = FakeOpener({
            ('GET', '/api/v1/ring_manager/status/'): json_response(body),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://standby.example.com:6205',
            'status', '--promotion',
        ], opener)
        self.assertEqual(1, status)
        self.assertEqual('', stderr)
        self.assertEqual('promotion=true', opener.requests[0]['query'])
        self.assertIn('Promotion readiness: blocked', stdout)
        self.assertIn(
            'Blockers: published_state_not_ready,missing_builder_files,'
            'invalid_builder_files,builder_version_mismatch,'
            'unpublished_builder_changes',
            stdout)
        self.assertIn('Missing builders: object-1', stdout)
        self.assertIn(
            'Builder error: unable to read rings: invalid JSON', stdout)
        self.assertIn('object-2 reason=builder_unloadable', stdout)
        self.assertIn('object-0 expected=9 actual=8', stdout)
        self.assertIn('Unpublished builder changes:', stdout)
        self.assertIn('object-3 published=4 actual=5', stdout)
        self.assertIn('Sync reasons: sync_transaction_pending', stdout)
        self.assertIn('Sync transaction: pending', stdout)
        self.assertIn(
            'Attention: yes reasons=sync_not_fresh,promotion_not_ready',
            stdout)

    def test_status_promotion_partial_response_prints_unknowns(self):
        body = {
            'mode': 'standby',
        }
        opener = FakeOpener({
            ('GET', '/api/v1/ring_manager/status/'): json_response(body),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://standby.example.com:6205',
            'status', '--promotion',
        ], opener)
        self.assertEqual(1, status)
        self.assertEqual('', stderr)
        self.assertIn('Writable: unknown', stdout)
        self.assertIn(
            'Sync: fresh=unknown synced=unknown stale=unknown', stdout)
        self.assertIn('Promotion readiness: blocked', stdout)

    def test_status_promotion_json_preserves_response_and_exit_code(self):
        body = {
            'mode': 'standby',
            'writable': False,
            'ring_manager_sync': {
                'fresh': False,
            },
            'promotion_readiness': {
                'ready': False,
                'blockers': ['published_state_not_ready'],
            },
        }
        opener = FakeOpener({
            ('GET', '/api/v1/ring_manager/status/'): json_response(body),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://standby.example.com:6205',
            '--json',
            'status', '--promotion',
        ], opener)
        self.assertEqual(1, status)
        self.assertEqual('', stderr)
        self.assertEqual('promotion=true', opener.requests[0]['query'])
        self.assertEqual(body, json.loads(stdout))

    def test_rings_create_posts_builder_settings(self):
        opener = FakeOpener({
            ('POST', '/api/v1/rings/'): json_response({'id': 'object-0'}),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205', 'rings', 'create',
            '--type', 'object', '--policy-index', '0',
            '--policy-type', 'replication', '--part-power', '18',
            '--replicas', '3', '--min-part-hours', '1',
            '--builder-file', 'object.builder',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual({'id': 'object-0'}, json.loads(stdout))
        self.assertEqual({
            'builder_files': ['object.builder'],
            'min_part_hours': 1,
            'num_replicas': 3.0,
            'part_power': 18,
            'policy_type': 'replication',
            'ring_type': 'object',
            'storage_policy_index': 0,
        }, json.loads(opener.requests[0]['body'].decode('ascii')))

    def test_rings_list_uses_table_unless_json_requested(self):
        body = {'objects': [{
            'id': 'object-0',
            'name': 'objects',
            'ring_type': 'object',
            'storage_policy_index': 0,
            'policy_type': 'replication',
            'device_count': 3,
        }]}
        routes = {('GET', '/api/v1/rings/'): json_response(body)}
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205', 'rings', 'list',
        ], FakeOpener(routes))
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertIn('ID', stdout)
        self.assertIn('object-0', stdout)
        with self.assertRaises(ValueError):
            json.loads(stdout)

        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205', '--json',
            'rings', 'list',
        ], FakeOpener(routes))
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual(body, json.loads(stdout))

    def test_devices_list_uses_table_unless_json_requested(self):
        body = {'devices': [{
            'id': 7,
            'region': 1,
            'zone': 2,
            'ip': '10.0.0.7',
            'port': 6200,
            'replication_ip': '10.1.0.7',
            'replication_port': 6200,
            'device': 'sdb',
            'weight': 100.0,
        }]}
        opener = FakeOpener({
            ('GET', '/api/v1/rings/object-0/devices/'): json_response(body),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'devices', 'list', 'object-0',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertIn('REPLICATION', stdout)
        self.assertIn('10.1.0.7:6200', stdout)

    def test_ring_update_uses_put_when_requested(self):
        opener = FakeOpener({
            ('PUT', '/api/v1/rings/object-0/'): json_response({
                'id': 'object-0',
            }),
        })
        status, _stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'rings', 'update', 'object-0', '--replace',
            '--type', 'object', '--policy-index', '0',
            '--policy-type', 'replication', '--part-power', '18',
            '--replicas', '3', '--min-part-hours', '1',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual('PUT', opener.requests[0]['method'])

    def test_devices_add_posts_shorthand_payload(self):
        opener = FakeOpener({
            ('POST', '/api/v1/rings/object-0/devices/add/'):
            json_response({'device_count': 1}),
        })
        status, _stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205', 'devices', 'add',
            'object-0', 'r1z1-10.0.0.1:6200/sdb', '100',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual({
            'devices': [{
                'region': 1,
                'zone': 1,
                'ip': '10.0.0.1',
                'port': 6200,
                'replication_ip': '10.0.0.1',
                'replication_port': 6200,
                'device': 'sdb',
                'meta': '',
                'weight': 100.0,
            }],
        }, json.loads(opener.requests[0]['body'].decode('ascii')))

    def test_devices_remove_posts_shorthand_payload(self):
        opener = FakeOpener({
            ('POST', '/api/v1/rings/object-0/devices/remove/'):
            json_response({'device_count': 0}),
        })
        status, _stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205', 'devices', 'remove',
            'object-0', 'd7',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual(
            {'devices': [{'id': 7}]},
            json.loads(opener.requests[0]['body'].decode('ascii')))

    def test_devices_rejects_file_and_shorthand_together(self):
        path = self._write_file('devices.json', json.dumps({
            'devices': ['node-1:sdb'],
        }))
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'devices', 'remove', 'object-0', '--from-file', path, 'd0',
        ], FakeOpener({}))
        self.assertEqual(1, status)
        self.assertEqual('', stdout)
        self.assertIn('either --from-file or device shorthand', stderr)

    def test_dry_run_skips_mutating_device_request(self):
        opener = FakeOpener({})
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205', '--dry-run',
            'devices', 'add', 'object-0',
            'r1z1-10.0.0.1:6200/sdb', '100',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual([], opener.requests)
        self.assertEqual('POST', json.loads(stdout)['method'])

    def test_rings_delete_requires_confirm_even_for_dry_run(self):
        opener = FakeOpener({})
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205', '--dry-run',
            'rings', 'delete', 'object-0',
        ], opener)
        self.assertEqual(1, status)
        self.assertEqual('', stdout)
        self.assertIn('requires --confirm', stderr)
        self.assertEqual([], opener.requests)

    def test_rings_delete_uses_confirmed_delete(self):
        opener = FakeOpener({
            ('DELETE', '/api/v1/rings/object-0/'): FakeResponse(status=204),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205', 'rings', 'delete',
            'object-0', '--confirm',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stdout)
        self.assertEqual('', stderr)
        self.assertEqual('DELETE', opener.requests[0]['method'])

    def test_part_power_action_uses_existing_endpoint(self):
        opener = FakeOpener({
            ('POST', '/api/v1/rings/object-0/partition_power_increase/'
             'prepare/'): json_response({'partition_power_increase_state':
                                         'prepared'}),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'rings', 'part-power', 'prepare', 'object-0',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual('prepared', json.loads(stdout)
                         ['partition_power_increase_state'])

    def test_rings_build_posts_artifact_only_request(self):
        opener = FakeOpener({
            ('POST', '/api/v1/rings/object-1/versions/'):
            json_response({'ring_id': 'object-1', 'version': '7'}),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'rings', 'build', 'object-1', '--seed', '1',
            '--format-version', '1',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual({'format_version': 1, 'seed': '1'},
                         json.loads(
                             opener.requests[0]['body'].decode('ascii')))
        self.assertEqual('object-1', json.loads(stdout)['ring_id'])

    def test_versions_publish_posts_complete_release_request(self):
        opener = FakeOpener({
            ('POST', '/api/v1/rings/releases/'):
            json_response({'version': 'release-demo'}),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'versions', 'publish', '--version', 'release-demo',
            '--ring', 'account', '--ring', 'container', '--seed', '1',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual({
            'rings': ['account', 'container'],
            'seed': '1',
            'version': 'release-demo',
        }, json.loads(opener.requests[0]['body'].decode('ascii')))
        self.assertEqual('release-demo', json.loads(stdout)['version'])

    def test_builds_show_fetches_build_job(self):
        opener = FakeOpener({
            ('GET', '/api/v1/rings/builds/build-1/'):
            json_response({'id': 'build-1', 'state': 'completed'}),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'builds', 'show', 'build-1',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual('/api/v1/rings/builds/build-1/',
                         opener.requests[0]['path'])
        self.assertEqual('completed', json.loads(stdout)['state'])

    def test_builds_actions_post_reason(self):
        opener = FakeOpener({
            ('POST', '/api/v1/rings/builds/build-1/cancel/'):
            json_response({'id': 'build-1', 'state': 'cancelled'}),
            ('POST', '/api/v1/rings/builds/build-1/retry/'):
            json_response({'id': 'build-2', 'state': 'queued'}),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'builds', 'cancel', 'build-1', '--reason', 'superseded',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual({'reason': 'superseded'}, json.loads(
            opener.requests[0]['body'].decode('ascii')))
        self.assertEqual('cancelled', json.loads(stdout)['state'])

        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'builds', 'retry', 'build-1', '--reason', 'transient failure',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual('/api/v1/rings/builds/build-1/retry/',
                         opener.requests[1]['path'])
        self.assertEqual({'reason': 'transient failure'}, json.loads(
            opener.requests[1]['body'].decode('ascii')))
        self.assertEqual('queued', json.loads(stdout)['state'])

    def test_builds_list_filters_retry_lineage(self):
        opener = FakeOpener({
            ('GET', '/api/v1/rings/builds/'):
            json_response({'objects': []}),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'builds', 'list', '--retry-of', 'failed-1',
            '--retry-root', 'root-1',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual('retry_of=failed-1&retry_root=root-1',
                         opener.requests[0]['query'])
        self.assertEqual({'objects': []}, json.loads(stdout))

        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'builds', 'list', '--retry-of', '',
        ], FakeOpener({}))
        self.assertEqual(1, status)
        self.assertEqual('', stdout)
        self.assertIn('--retry-of must not be empty', stderr)

    def test_versions_download_verifies_release_artifacts(self):
        artifact_body = b'object ring bytes'
        artifact_sha256 = hashlib.sha256(artifact_body).hexdigest()
        opener = FakeOpener({
            ('GET', '/api/v1/rings/releases/latest/manifest/'):
            json_response({
                'version': 'release-1',
                'files': [{
                    'name': 'object.ring.gz',
                    'url': '/api/v1/rings/releases/release-1/files/'
                    'object.ring.gz',
                    'bytes': len(artifact_body),
                    'sha256': artifact_sha256,
                }],
            }),
            ('GET', '/api/v1/rings/releases/release-1/files/object.ring.gz'):
            FakeResponse(artifact_body),
        })
        output_dir = os.path.join(self.testdir, 'rings')
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'versions', 'download', 'latest', '--output-dir', output_dir,
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual('release-1', json.loads(stdout)['version'])
        with open(os.path.join(output_dir, 'object.ring.gz'), 'rb') as fp:
            self.assertEqual(artifact_body, fp.read())

    def test_versions_download_rejects_cross_origin_artifact_url(self):
        opener = FakeOpener({
            ('GET', '/api/v1/rings/releases/latest/manifest/'):
            json_response({
                'version': 'release-1',
                'files': [{
                    'name': 'object.ring.gz',
                    'url': 'https://evil.example.com/steal',
                }],
            }),
        })
        output_dir = os.path.join(self.testdir, 'rings')
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'versions', 'download', 'latest', '--output-dir', output_dir,
        ], opener)
        self.assertEqual(1, status)
        self.assertEqual('', stdout)
        self.assertIn('same-origin relative URL', stderr)
        self.assertEqual(1, len(opener.requests))

    def test_versions_download_rejects_artifact_url_dot_segments(self):
        opener = FakeOpener({
            ('GET', '/api/v1/rings/releases/latest/manifest/'):
            json_response({
                'version': 'release-1',
                'files': [{
                    'name': 'object.ring.gz',
                    'url': '/api/v1/../../healthcheck',
                }],
            }),
        })
        output_dir = os.path.join(self.testdir, 'rings')
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'versions', 'download', 'latest', '--output-dir', output_dir,
        ], opener)
        self.assertEqual(1, status)
        self.assertEqual('', stdout)
        self.assertIn('dot segments', stderr)
        self.assertEqual(1, len(opener.requests))

    def test_analysis_options_are_encoded_in_query(self):
        opener = FakeOpener({
            ('GET', '/api/v1/rings/object-0/dispersion/'): json_response({}),
        })
        status, _stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'analyze', 'dispersion', 'object-0', '--level', 'zone',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual('level=zone', opener.requests[0]['query'])

    def test_partitions_at_risk_requires_a_down_selector(self):
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'analyze', 'partitions_at_risk', 'object-0',
        ], FakeOpener({}))
        self.assertEqual(1, status)
        self.assertEqual('', stdout)
        self.assertIn('requires at least one down selector', stderr)

    def test_partitions_at_risk_get_uses_query_and_summary_output(self):
        opener = FakeOpener({
            ('GET', '/api/v1/rings/object-0/partitions_at_risk/'):
            json_response({
                'risk_count': 2,
                'selectors': {
                    'node_ips': ['10.0.0.1'],
                    'replication_ips': [],
                    'device_ids': [],
                },
                'summary': {
                    'matched_devices': 1,
                    'affected_partitions': 5,
                    'at_risk_partitions': 2,
                    'max_down_replicas': 2,
                },
            }),
        })
        status, stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            'analyze', 'partitions_at_risk', 'object-0',
            '--node-ip', '10.0.0.1', '--risk-count', '2',
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual(
            'node_ip=10.0.0.1&risk_count=2&details=false',
            opener.requests[0]['query'])
        self.assertIn('At-risk partitions: 2', stdout)

    def test_partitions_at_risk_file_uses_read_credentials(self):
        selector_file = self._write_file('selectors.json', json.dumps({
            'node_ips': ['10.0.0.1'],
            'risk_count': 2,
        }))
        opener = FakeOpener({
            ('POST', '/api/v1/rings/object-0/partitions_at_risk/'):
            json_response({'risk_count': 2, 'selectors': {},
                           'summary': {}}),
        })
        status, _stdout, stderr = self._run([
            '--url', 'http://primary.example.com:6205',
            '--admin-key', 'secret', '--read-key', 'reader',
            'analyze', 'partitions_at_risk', 'object-0',
            '--from-file', selector_file,
        ], opener)
        self.assertEqual(0, status)
        self.assertEqual('', stderr)
        self.assertEqual(
            'reader', opener.requests[0]['headers']['x-ring-manager-read-key'])
        self.assertNotIn('x-ring-manager-admin-key',
                         opener.requests[0]['headers'])
        self.assertEqual({
            'node_ips': ['10.0.0.1'],
            'risk_count': 2,
        }, json.loads(opener.requests[0]['body'].decode('ascii')))
