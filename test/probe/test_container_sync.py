#!/usr/bin/python -u
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
import json
import os
import time
import uuid
import random
from ConfigParser import ConfigParser
from contextlib import contextmanager
from shutil import rmtree
from tempfile import mkdtemp

import errno
from nose import SkipTest
import unittest

from six.moves.urllib.parse import urlparse
from swiftclient import client, ClientException

from swift.common.manager import Manager, Server, SWIFT_DIR
from swift.common.http import HTTP_NOT_FOUND
from swift.common.utils import readconf
from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, ENABLED_POLICIES


def get_current_realm_cluster(url):
    parts = urlparse(url)
    url = parts.scheme + '://' + parts.netloc + '/info'
    http_conn = client.http_connection(url)
    try:
        info = client.get_capabilities(http_conn)
    except client.ClientException:
        raise SkipTest('Unable to retrieve cluster info')
    try:
        realms = info['container_sync']['realms']
    except KeyError:
        raise SkipTest('Unable to find container sync realms')
    for realm, realm_info in realms.items():
        for cluster, options in realm_info['clusters'].items():
            if options.get('current', False):
                return realm, cluster
    raise SkipTest('Unable find current realm cluster')


class FakeServer(Server):
    def __init__(self, server_name, tempdir, custom_conf):
        self.custom_conf = custom_conf
        self.tempdir = tempdir
        super(FakeServer, self).__init__(server_name, run_dir=tempdir)

    def conf_files(self):
        # overrides superclass to return custom conf files in a temp dir.
        # each custom conf files will have custom_conf applied.
        new_conf_files = []
        conf_files = super(FakeServer, self).conf_files()
        for conf_file in conf_files:
            conf = readconf(conf_file)  # read from a file or a dir of files
            cp = ConfigParser()
            for conf_source in (conf, self.custom_conf):
                for section_name, options in conf_source.items():
                    if not isinstance(options, dict):
                        continue
                    if not cp.has_section(section_name):
                        cp.add_section(section_name)
                    for name, value in options.items():
                        cp.set(section_name, name, value)

            # the path to the conf file is significant because it determines
            # where the Server writes and then looks for its pid files
            tmp_conf_file_dir = os.path.dirname(conf_file).replace(
                SWIFT_DIR, self.tempdir)
            try:
                os.mkdir(tmp_conf_file_dir)
            except OSError as err:
                if err.errno != errno.EEXIST:
                    raise
            tmp_conf_file = os.path.join(tmp_conf_file_dir,
                                         os.path.basename(conf_file))
            with open(tmp_conf_file, 'w') as fp:
                cp.write(fp)

            new_conf_files.append(tmp_conf_file)

        return new_conf_files


class TestContainerSync(ReplProbeTest):

    def setUp(self):
        super(TestContainerSync, self).setUp()
        self.realm, self.cluster = get_current_realm_cluster(self.url)
        self.source_containers = []
        self.dest_containers = []
        self.tempdir = mkdtemp()

    def tearDown(self):
        rmtree(self.tempdir)
        super(TestContainerSync, self).tearDown()

    @contextmanager
    def run_server(self, server_name, custom_conf, **kwargs):
        # kick off server daemons, yield, then kill the daemons on exit
        server = FakeServer(server_name, self.tempdir, custom_conf)
        try:
            server.launch(**kwargs)
            yield
        finally:
            server.stop(verbose=True)

    def create_source_and_dest_containers(self, num,
                                          skey='secret', dkey='secret',
                                          source_overrides=None,
                                          dest_overrides=None):
        # unless overridden by source_overrides and/or dest_overrides
        default_params = {'url': self.url,
                          'token': self.token,
                          'account': self.account}
        container_suffixes = (uuid.uuid4() for i in range(num))
        for container_suffix in container_suffixes:
            # setup dest container
            dest = dict(default_params)
            dest['name'] = 'dest-container-%s' % container_suffix
            dest.update(dest_overrides or {})
            dest_headers = {}
            dest_policy = None
            if len(ENABLED_POLICIES) > 1:
                dest_policy = random.choice(ENABLED_POLICIES)
                dest_headers['X-Storage-Policy'] = dest_policy.name
            if dkey is not None:
                dest_headers['X-Container-Sync-Key'] = dkey
            client.put_container(dest['url'], dest['token'], dest['name'],
                                 headers=dest_headers)
            self.dest_containers.append(dest['name'])

            # setup source container
            source = dict(default_params)
            source['name'] = 'source-container-%s' % container_suffix
            source.update(source_overrides or {})
            source_headers = {}
            sync_to = '//%s/%s/%s/%s' % (self.realm,
                                         self.cluster,
                                         dest['account'],
                                         dest['name'])
            source_headers['X-Container-Sync-To'] = sync_to
            if skey is not None:
                source_headers['X-Container-Sync-Key'] = skey
            if dest_policy:
                source_policy = random.choice([p for p in ENABLED_POLICIES
                                               if p is not dest_policy])
                source_headers['X-Storage-Policy'] = source_policy.name
            client.put_container(source['url'], source['token'], source['name'],
                                 headers=source_headers)
            self.source_containers.append(source['name'])

    def create_synced_containers_pair(self, skey='secret', dkey='secret',
                                      source_overrides=None,
                                      dest_overrides=None):
        self.create_source_and_dest_containers(1, skey, dkey, source_overrides,
                                               dest_overrides)
        return self.source_containers[0], self.dest_containers[0]

    def populate_source_containers(self, num):
        self.obj_names = [str(uuid.uuid4()) for i in range(num)]
        for source_container in self.source_containers:
            for obj_name in self.obj_names:
                client.put_object(self.url,
                                  self.token,
                                  source_container,
                                  obj_name,
                                  '%s\n%s' % (source_container, obj_name))

    def validate_container_sync(self):
        errors = []
        for i, dest_container in enumerate(self.dest_containers):
            for obj_name in self.obj_names:
                try:
                    headers, body = client.get_object(self.url, self.token,
                                                      dest_container, obj_name)
                except ClientException as err:
                    errors.append('Failed to GET %s/%s: %s' %
                                  (dest_container, obj_name, err))
                else:
                    obj_body = '%s\n%s' % (self.source_containers[i], obj_name)
                    if obj_body != body:
                        errors.append(
                            'Expected body %s but got %s for %s/%s' %
                            (obj_body, body, dest_container, obj_name))

        if errors:
            msg = 'Unexpected errors: \n' + '\n'.join(errors)
            self.fail(msg)

    def run_sync_forever(self, conf, wait_time):
        Manager(['container-replicator']).once()
        with self.run_server('container-sync', {'container-sync': conf}):
            time.sleep(wait_time)  # leave time for at least one sync cycle

    def _test_sync(self, object_post_as_copy):
        source_container, dest_container = self.create_synced_containers_pair()

        # upload to source
        object_name = 'object-%s' % uuid.uuid4()
        put_headers = {'X-Object-Meta-Test': 'put_value'}
        client.put_object(self.url, self.token, source_container, object_name,
                          'test-body', headers=put_headers)

        # cycle container-sync
        Manager(['container-sync']).once()

        resp_headers, body = client.get_object(self.url, self.token,
                                               dest_container, object_name)
        self.assertEqual(body, 'test-body')
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('put_value', resp_headers['x-object-meta-test'])

        # update metadata with a POST, using an internal client so we can
        # vary the object_post_as_copy setting - first use post-as-copy
        post_headers = {'Content-Type': 'image/jpeg',
                        'X-Object-Meta-Test': 'post_value'}
        int_client = self.make_internal_client(
            object_post_as_copy=object_post_as_copy)
        int_client.set_object_metadata(self.account, source_container,
                                       object_name, post_headers)
        # sanity checks...
        resp_headers = client.head_object(
            self.url, self.token, source_container, object_name)
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('post_value', resp_headers['x-object-meta-test'])
        self.assertEqual('image/jpeg', resp_headers['content-type'])

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify that metadata changes were sync'd
        resp_headers, body = client.get_object(self.url, self.token,
                                               dest_container, object_name)
        self.assertEqual(body, 'test-body')
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('post_value', resp_headers['x-object-meta-test'])
        self.assertEqual('image/jpeg', resp_headers['content-type'])

        # delete the object
        client.delete_object(
            self.url, self.token, source_container, object_name)
        with self.assertRaises(ClientException) as cm:
            client.get_object(
                self.url, self.token, source_container, object_name)
        self.assertEqual(404, cm.exception.http_status)  # sanity check

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify delete has been sync'd
        with self.assertRaises(ClientException) as cm:
            client.get_object(
                self.url, self.token, dest_container, object_name)
        self.assertEqual(404, cm.exception.http_status)  # sanity check

    def test_sync_with_post_as_copy(self):
        self._test_sync(True)

    def test_sync_with_fast_post(self):
        self._test_sync(False)

    def test_sync_slo_manifest(self):
        # Verify that SLO manifests are sync'd even if their segments can not
        # be found in the destination account at time of sync'ing.
        # Create source and dest containers for manifest in separate accounts.
        dest_account = self.account_2
        source_container, dest_container = self.create_synced_containers_pairs(
            dest_overrides=dest_account)

        # Create source and dest containers for segments in separate accounts.
        # These containers must have same name for the destination SLO manifest
        # to be able to resolve segments. Initially the destination has no sync
        # key so segments will not sync.
        segs_container = 'segments-%s' % uuid.uuid4()
        dest_segs_info = dict(dest_account)
        dest_segs_info.update({'name': segs_container})
        self.create_source_and_dest_containers(
            source_overrides={'name': segs_container},
            dest_overrides=dest_segs_info, dkey=None)

        # upload a segment to source
        segment_name = 'segment-%s' % uuid.uuid4()
        segment_data = 'segment body'  # it's ok for first segment to be small
        segment_etag = client.put_object(
            self.url, self.token, segs_container, segment_name,
            segment_data)

        manifest = [{'etag': segment_etag,
                     'size_bytes': len(segment_data),
                     'path': '/%s/%s' % (segs_container, segment_name)}]
        manifest_name = 'manifest-%s' % uuid.uuid4()
        put_headers = {'X-Object-Meta-Test': 'put_value'}
        client.put_object(
            self.url, self.token, source_container, manifest_name,
            json.dumps(manifest), headers=put_headers,
            query_string='multipart-manifest=put')

        resp_headers, manifest_body = client.get_object(
            self.url, self.token, source_container, manifest_name,
            query_string='multipart-manifest=get')
        int_manifest = json.loads(manifest_body)

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify manifest was sync'd
        resp_headers, dest_listing = client.get_container(
            dest_account['url'], dest_account['token'], dest_container)
        self.assertFalse(dest_listing[1:])
        self.assertEqual(manifest_name, dest_listing[0]['name'])

        # verify manifest body
        resp_headers, body = client.get_object(
            dest_account['url'], dest_account['token'], dest_container,
            manifest_name, query_string='multipart-manifest=get')
        self.assertEqual(int_manifest, json.loads(body))
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('put_value', resp_headers['x-object-meta-test'])

        # attempt to GET the SLO will fail because the segment wasn't sync'd
        with self.assertRaises(ClientException) as cm:
            client.get_object(dest_account['url'], dest_account['token'],
                              dest_container, manifest_name)
        self.assertEqual(409, cm.exception.http_status)

        # now set sync key on destination segments container
        client.put_container(
            dest_account['url'], dest_account['token'], segs_container,
            headers={'X-Container-Sync-Key': 'secret'})

        # cycle container-sync
        Manager(['container-sync']).once()

        # sanity check - verify manifest body
        resp_headers, body = client.get_object(
            dest_account['url'], dest_account['token'], dest_container,
            manifest_name, query_string='multipart-manifest=get')
        self.assertEqual(int_manifest, json.loads(body))
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('put_value', resp_headers['x-object-meta-test'])

        # verify GET of SLO manifest now succeeds
        resp_headers, body = client.get_object(
            dest_account['url'], dest_account['token'], dest_container,
            manifest_name)
        self.assertEqual(segment_data, body)

    def test_sync_lazy_skey(self):
        source_container, dest_container = \
            self.create_synced_containers_pair(None, 'secret')
        self.populate_source_containers(1)
        Manager(['container-sync']).once()
        with self.assertRaises(ClientException) as err:
            _junk, body = client.get_object(self.url, self.token,
                                            dest_container, self.obj_names[0])
        self.assertEqual(err.exception.http_status, HTTP_NOT_FOUND)

        # amend source key
        source_headers = {'X-Container-Sync-Key': 'secret'}
        client.put_container(self.url, self.token, source_container,
                             headers=source_headers)
        # cycle container-sync, should replicate
        Manager(['container-sync']).once()
        self.validate_container_sync()

    def test_sync_lazy_dkey(self):
        source_container, dest_container = \
            self.create_synced_containers_pair('secret', None)
        self.populate_source_containers(1)

        # cycle container-sync, nothing should happen
        Manager(['container-sync']).once()
        with self.assertRaises(ClientException) as err:
            _junk, body = client.get_object(self.url, self.token,
                                            dest_container, self.obj_names[0])
        self.assertEqual(err.exception.http_status, HTTP_NOT_FOUND)

        # amend dest key
        dest_headers = {'X-Container-Sync-Key': 'secret'}
        client.put_container(self.url, self.token, dest_container,
                             headers=dest_headers)
        # cycle container-sync, should replicate
        Manager(['container-sync']).once()
        self.validate_container_sync()

    def test_sync_forever_multiple_containers(self):
        self.create_source_and_dest_containers(2)
        self.populate_source_containers(1)
        conf = {'processes': 2, 'interval': 1}
        self.run_sync_forever(conf, 10)
        self.validate_container_sync()

    def test_sync_forever_multiple_containers_and_objects(self):
        self.create_source_and_dest_containers(4)
        self.populate_source_containers(4)
        conf = {'processes': 2, 'interval': 1}
        self.run_sync_forever(conf, 10)
        self.validate_container_sync()

    def test_sync_with_stale_container_rows(self):
        source_container, dest_container = self.create_synced_containers_pair()

        brain = BrainSplitter(self.url, self.token, source_container,
                              None, 'container')

        # upload to source
        object_name = 'object-%s' % uuid.uuid4()
        client.put_object(self.url, self.token, source_container, object_name,
                          'test-body')

        # check source container listing
        _, listing = client.get_container(
            self.url, self.token, source_container)
        for expected_obj_dict in listing:
            if expected_obj_dict['name'] == object_name:
                break
        else:
            self.fail('Failed to find source object %r in container listing %r'
                      % (object_name, listing))

        # stop all container servers
        brain.stop_primary_half()
        brain.stop_handoff_half()

        # upload new object content to source - container updates will fail
        client.put_object(self.url, self.token, source_container, object_name,
                          'new-test-body')
        source_headers = client.head_object(
            self.url, self.token, source_container, object_name)

        # start all container servers
        brain.start_primary_half()
        brain.start_handoff_half()

        # sanity check: source container listing should not have changed
        _, listing = client.get_container(
            self.url, self.token, source_container)
        for actual_obj_dict in listing:
            if actual_obj_dict['name'] == object_name:
                self.assertDictEqual(expected_obj_dict, actual_obj_dict)
                break
        else:
            self.fail('Failed to find source object %r in container listing %r'
                      % (object_name, listing))

        # cycle container-sync - object should be correctly sync'd despite
        # stale info in container row
        Manager(['container-sync']).once()

        # verify sync'd object has same content and headers
        dest_headers, body = client.get_object(self.url, self.token,
                                               dest_container, object_name)
        self.assertEqual(body, 'new-test-body')
        mismatched_headers = []
        for k in ('etag', 'content-length', 'content-type', 'x-timestamp',
                  'last-modified'):
            if source_headers[k] == dest_headers[k]:
                continue
            mismatched_headers.append((k, source_headers[k], dest_headers[k]))
        if mismatched_headers:
            msg = '\n'.join([('Mismatched header %r, expected %r but got %r'
                              % item) for item in mismatched_headers])
            self.fail(msg)

    def test_sync_newer_remote(self):
        source_container, dest_container = self.create_synced_containers_pair()

        # upload to source
        object_name = 'object-%s' % uuid.uuid4()
        client.put_object(self.url, self.token, source_container, object_name,
                          'old-source-body')

        # upload to dest with same name
        client.put_object(self.url, self.token, dest_container, object_name,
                          'new-test-body')

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify that the remote object did not change
        resp_headers, body = client.get_object(self.url, self.token,
                                               dest_container, object_name)
        self.assertEqual(body, 'new-test-body')


if __name__ == "__main__":
    unittest.main()
