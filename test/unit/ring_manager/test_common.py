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

import os
import shutil
import tempfile
import unittest
from unittest import mock

from swift.ring_manager.common import ArtifactLifecycleHook, \
    DEFAULT_ARTIFACT_HOOK_TIMEOUT, DEFAULT_STATE_CHANGE_HOOK_TIMEOUT, \
    StateChangeHook, load_secret, load_secret_from_conf, read_secret_file, \
    validate_relative_api_url


class TestRingManagerCommon(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def _write_file(self, name, body):
        path = os.path.join(self.testdir, name)
        with open(path, 'wb') as fp:
            fp.write(body)
        os.chmod(path, 0o600)
        return path

    def test_state_change_hook_zero_timeout_uses_default(self):
        seen_timeouts = []

        class FakeHookProcess(object):
            returncode = 0

            def communicate(self, timeout=None):
                seen_timeouts.append(timeout)
                return b'', b''

        with mock.patch(
                'swift.ring_manager.common.subprocess.Popen',
                return_value=FakeHookProcess()):
            StateChangeHook(
                '/bin/true', state_dir=self.testdir,
                timeout=0).run('write', os.path.join(
                    self.testdir, 'index.json'))

        self.assertEqual([DEFAULT_STATE_CHANGE_HOOK_TIMEOUT], seen_timeouts)

    def test_artifact_hook_uses_event_environment(self):
        seen_timeouts = []

        class FakeHookProcess(object):
            returncode = 0

            def communicate(self, timeout=None):
                seen_timeouts.append(timeout)
                return b'', b''

        with mock.patch(
                'swift.ring_manager.common.subprocess.Popen',
                return_value=FakeHookProcess()) as mock_popen:
            ArtifactLifecycleHook(
                '/bin/true', artifact_dir=self.testdir, timeout=0).run(
                'artifact-publish', 'artifact-object-0-1',
                ring_id='object-0', version=7)

        self.assertEqual([DEFAULT_ARTIFACT_HOOK_TIMEOUT], seen_timeouts)
        _args, kwargs = mock_popen.call_args
        self.assertEqual(self.testdir, kwargs['cwd'])
        self.assertEqual({
            'RING_MANAGER_ARTIFACT_EVENT': 'artifact-publish',
            'RING_MANAGER_ARTIFACT_DIR': self.testdir,
            'RING_MANAGER_ARTIFACT_NAMESPACE': 'artifact-object-0-1',
            'RING_MANAGER_ARTIFACT_PATH': os.path.join(
                self.testdir, 'artifact-object-0-1'),
            'RING_MANAGER_ARTIFACT_RELPATH': 'artifact-object-0-1',
            'RING_MANAGER_ARTIFACT_RELEASE': '',
            'RING_MANAGER_ARTIFACT_RING_ID': 'object-0',
            'RING_MANAGER_ARTIFACT_VERSION': '7',
        }, dict((key, kwargs['env'][key]) for key in (
            'RING_MANAGER_ARTIFACT_EVENT',
            'RING_MANAGER_ARTIFACT_DIR',
            'RING_MANAGER_ARTIFACT_NAMESPACE',
            'RING_MANAGER_ARTIFACT_PATH',
            'RING_MANAGER_ARTIFACT_RELPATH',
            'RING_MANAGER_ARTIFACT_RELEASE',
            'RING_MANAGER_ARTIFACT_RING_ID',
            'RING_MANAGER_ARTIFACT_VERSION',
        )))

    def test_artifact_hook_marks_complete_release(self):
        class FakeHookProcess(object):
            returncode = 0

            def communicate(self, timeout=None):
                return b'', b''

        with mock.patch(
                'swift.ring_manager.common.subprocess.Popen',
                return_value=FakeHookProcess()) as mock_popen:
            ArtifactLifecycleHook(
                '/bin/true', artifact_dir=self.testdir).run(
                'publish', 'release-1', release='release-1')

        env = mock_popen.call_args[1]['env']
        self.assertEqual('publish', env['RING_MANAGER_ARTIFACT_EVENT'])
        self.assertEqual('release-1',
                         env['RING_MANAGER_ARTIFACT_NAMESPACE'])
        self.assertEqual('release-1', env['RING_MANAGER_ARTIFACT_RELEASE'])
        self.assertEqual('', env['RING_MANAGER_ARTIFACT_RING_ID'])
        self.assertEqual('', env['RING_MANAGER_ARTIFACT_VERSION'])

    def test_read_secret_file_strips_one_trailing_newline(self):
        self.assertEqual('secret', read_secret_file(
            self._write_file('secret-nl', b'secret\n'), 'read_key_file'))
        self.assertEqual('secret', read_secret_file(
            self._write_file('secret-crlf', b'secret\r\n'), 'read_key_file'))

    def test_read_secret_file_rejects_bad_files(self):
        values = {
            'empty': b'',
            'blank': b'   \n',
            'multiline': b'secret\nother',
            'nul': b'secret\x00',
            'control': b'secret\t',
        }
        for name, body in values.items():
            with self.assertRaises(ValueError):
                read_secret_file(self._write_file(name, body), 'read_key_file')

        with self.assertRaises(ValueError):
            read_secret_file(os.path.join(self.testdir, 'missing'),
                             'read_key_file')
        with self.assertRaises(ValueError):
            read_secret_file(self.testdir, 'read_key_file')

        loose_path = self._write_file('loose', b'secret')
        os.chmod(loose_path, 0o644)
        with self.assertRaises(ValueError):
            read_secret_file(loose_path, 'read_key_file')

        writable_path = self._write_file('writable', b'secret')
        os.chmod(writable_path, 0o620)
        with self.assertRaises(ValueError):
            read_secret_file(writable_path, 'read_key_file')

        target = self._write_file('target', b'secret')
        link = os.path.join(self.testdir, 'link')
        try:
            os.symlink(target, link)
        except (AttributeError, OSError):
            pass
        else:
            with self.assertRaises(ValueError):
                read_secret_file(link, 'read_key_file')

    def test_load_secret_uses_file_and_rejects_conflict(self):
        path = self._write_file('read.key', b'reader\n')
        self.assertEqual('reader', load_secret(
            None, 'read_key', path, 'read_key_file'))
        self.assertEqual('inline', load_secret(
            'inline', 'read_key', None, 'read_key_file'))
        with self.assertRaises(ValueError):
            load_secret('inline', 'read_key', path, 'read_key_file')

    def test_load_secret_from_conf_supports_aliases(self):
        path = self._write_file('admin.key', b'admin')
        conf = {'ring_manager_admin_key_file': path}
        self.assertEqual('admin', load_secret_from_conf(
            conf, ('admin_key', 'ring_manager_admin_key'),
            ('admin_key_file', 'ring_manager_admin_key_file')))

    def test_validate_relative_api_url(self):
        self.assertEqual(
            '/api/v1/rings/releases/latest/manifest/',
            validate_relative_api_url(
                '/api/v1/rings/releases/latest/manifest/'))

    def test_validate_relative_api_url_rejects_unsafe_paths(self):
        values = (
            'https://evil.example.com/steal',
            '/api/v1/../../healthcheck',
            '/api/v1/%2e%2e/%2e%2e/healthcheck',
            '/api/v1/files/foo%5cbar',
            '/api/v1/rings/releases/latest/#fragment',
        )
        for value in values:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    validate_relative_api_url(value)


if __name__ == '__main__':
    unittest.main()
