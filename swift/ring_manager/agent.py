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

import errno
import hashlib
import json
import os
import random
import sys
import time
import uuid

from urllib.parse import quote, urljoin

from swift.common.concurrency import socket, urllib_request
from swift.common.daemon import Daemon, run_daemon
from swift.common.recon import DEFAULT_RECON_CACHE_PATH, \
    RECON_RING_MANAGER_AGENT_FILE
from swift.common.ring import RingData
from swift.common.utils import config_true_value, dump_recon_cache, \
    fsync, get_logger, list_from_csv, lock_path, md5, non_negative_float, \
    parse_options
from swift.ring_manager.common import load_secret_from_conf, NormalTimestamp, \
    normal_timestamp, stats_increment, stats_timing, validate_relative_api_url


USER_AGENT = 'swift-ring-manager-agent'
DEFAULT_MODE = 'enforce'
AGENT_MODES = ('enforce', 'observe', 'validate-only')
DEFAULT_INTERVAL = 300.0
DEFAULT_JITTER = 30.0
DEFAULT_REQUEST_TIMEOUT = 30.0
DEFAULT_SWIFT_DIR = '/etc/swift'
DEFAULT_STATE_FILE = 'ring-manager-agent-state.json'
DEFAULT_LOCK_TIMEOUT = 10.0
LOCK_NAME = 'ring-manager-agent'
INSTALL_JOURNAL = '.ring-manager-agent-install.json'
BACKUP_MARKER = '.ring-manager-backup-'
OPERATOR_ATTENTION_FILE_LIMIT = 20
ENFORCE_RECON_FIELDS = (
    'source', 'desired_ring_version', 'files_installed', 'files_downloaded',
    'files_unchanged', 'sync_time', 'last_synced_at')
LEGACY_ENFORCE_RECON_FIELDS = ('latest_ring_version',)
OBSERVE_RECON_FIELDS = (
    'files_observed', 'files_valid', 'files_invalid', 'files',
    'observe_time', 'last_observed_at')
VALIDATE_RECON_FIELDS = (
    'validation_source', 'release_selector', 'release', 'converged',
    'files_expected', 'files_local', 'files_matching', 'files_stale',
    'files_missing', 'files_unknown', 'files_extra', 'files_error',
    'validation_files', 'validation_time', 'last_validated_at')
MODE_RECON_FIELDS = {
    'enforce': frozenset(ENFORCE_RECON_FIELDS),
    'observe': frozenset(OBSERVE_RECON_FIELDS),
    'validate-only': frozenset(VALIDATE_RECON_FIELDS),
}


class RingManagerAgentError(Exception):
    pass


class RingManagerAgentLocalError(RingManagerAgentError):
    pass


class RingManagerAgentInstallError(RingManagerAgentLocalError):
    pass


class RingManagerAgent(Daemon):
    """
    Observe, validate, or enforce published ring state on a storage node.

    Enforce mode consumes the ring-manager desired manifest API, verifies each
    artifact against the manifest metadata, and installs the ring files into
    ``swift_dir`` using same-directory atomic renames. Observe mode only
    inventories local ring files. Validate-only mode compares them with one
    selected release without downloading or installing artifacts.
    """

    def __init__(self, conf, logger=None, opener=None, sleep=time.sleep,
                 random_func=random.random, time_func=NormalTimestamp.now):
        super(RingManagerAgent, self).__init__(conf)
        self.logger = logger or get_logger(
            conf, log_route=USER_AGENT, statsd_tail_prefix=USER_AGENT)
        self.opener = opener or urllib_request.urlopen
        self.sleep = sleep
        self.random_func = random_func
        self.time_func = time_func

        self.mode = str(conf.get('mode', DEFAULT_MODE)).strip().lower()
        if self.mode not in AGENT_MODES:
            raise RingManagerAgentError(
                'mode must be one of %s' % ', '.join(AGENT_MODES))

        urls = []
        urls.extend(list_from_csv(conf.get('ring_manager_urls')))
        urls.extend(list_from_csv(conf.get('ring_manager_url')))
        self.ring_manager_urls = [url.rstrip('/') for url in urls if url]
        if self.mode != 'observe' and not self.ring_manager_urls:
            raise RingManagerAgentError('ring_manager_urls is required')
        self.release = str(conf.get('release', '')).strip()
        if self.mode == 'validate-only' and not self.release:
            raise RingManagerAgentError(
                'release is required in validate-only mode')
        if self.mode != 'validate-only' and self.release:
            raise RingManagerAgentError(
                'release is only valid in validate-only mode')
        self.release_manifest_path = None
        if self.mode == 'validate-only':
            path = '/api/v1/rings/releases/%s/manifest/' % self._safe_id(
                self.release)
            try:
                self.release_manifest_path = validate_relative_api_url(
                    path, 'release manifest URL')
            except ValueError as err:
                raise RingManagerAgentError(str(err))
        self.shuffle_ring_manager_urls = config_true_value(conf.get(
            'shuffle_ring_manager_urls', 'false'))
        self.allow_ring_version_rollback = config_true_value(conf.get(
            'allow_ring_version_rollback', 'false'))

        self.swift_dir = conf.get('swift_dir', DEFAULT_SWIFT_DIR)
        self.interval = non_negative_float(
            conf.get('interval', DEFAULT_INTERVAL))
        self.jitter = non_negative_float(conf.get('jitter', DEFAULT_JITTER))
        self.request_timeout = non_negative_float(conf.get(
            'request_timeout', DEFAULT_REQUEST_TIMEOUT))
        self.lock_timeout = non_negative_float(
            conf.get('lock_timeout', DEFAULT_LOCK_TIMEOUT))
        try:
            self.read_key = load_secret_from_conf(
                conf, ('read_key', 'ring_manager_read_key'),
                ('read_key_file', 'ring_manager_read_key_file'))
            self.admin_key = load_secret_from_conf(
                conf, ('admin_key', 'ring_manager_admin_key'),
                ('admin_key_file', 'ring_manager_admin_key_file'))
        except ValueError as err:
            raise RingManagerAgentError(str(err))
        self.read_auth_token = conf.get('read_auth_token') or conf.get(
            'ring_manager_read_auth_token')
        self.auth_token = conf.get('auth_token')
        self.recon_cache_path = conf.get('recon_cache_path',
                                         DEFAULT_RECON_CACHE_PATH)
        self.recon_cache = os.path.join(
            self.recon_cache_path, RECON_RING_MANAGER_AGENT_FILE)
        self.state_file = conf.get('state_file') or os.path.join(
            self.recon_cache_path, DEFAULT_STATE_FILE)
        self.install_journal = os.path.join(self.swift_dir, INSTALL_JOURNAL)

    def _timestamp(self, timestamp=None):
        timestamp = self.time_func() if timestamp is None else timestamp
        return normal_timestamp(timestamp)

    def _timestamp_internal(self, timestamp=None):
        return self._timestamp(timestamp).internal

    def _safe_id(self, value):
        return quote(str(value), safe='')

    def _api_url(self, source_url, path_or_url):
        return urljoin(source_url + '/', path_or_url)

    def _headers(self, extra=None):
        headers = {'User-Agent': USER_AGENT}
        has_read_credentials = self.read_key or self.read_auth_token
        if self.read_key:
            headers['X-Ring-Manager-Read-Key'] = self.read_key
        elif not has_read_credentials and self.admin_key:
            headers['X-Ring-Manager-Admin-Key'] = self.admin_key
        if self.read_auth_token:
            headers['X-Auth-Token'] = self.read_auth_token
        elif not has_read_credentials and self.auth_token:
            headers['X-Auth-Token'] = self.auth_token
        if extra:
            headers.update(extra)
        return headers

    def _source_urls(self):
        urls = list(self.ring_manager_urls)
        if self.shuffle_ring_manager_urls:
            urls.sort(key=lambda _url: self.random_func())
        return urls

    def _request(self, source_url, path_or_url, headers=None):
        url = self._api_url(source_url, path_or_url)
        req = urllib_request.Request(url, headers=self._headers(headers))
        try:
            resp = self.opener(req, timeout=self.request_timeout)
        except urllib_request.HTTPError as err:
            if err.code == 304:
                return 304, b'', err.headers
            body = err.read()
            raise RingManagerAgentError(
                'GET %s failed with HTTP %s: %s' % (
                    url, err.code, body.decode('utf-8', 'replace')))
        except (urllib_request.URLError, socket.timeout) as err:
            raise RingManagerAgentError('GET %s failed: %s' % (url, err))

        status = getattr(resp, 'status', None)
        if status is None:
            status = getattr(resp, 'code', None)
        if status is None and hasattr(resp, 'getcode'):
            status = resp.getcode()
        if status is None:
            status = 200
        body = resp.read()
        if status == 304:
            return status, b'', resp.info()
        if status < 200 or status >= 300:
            raise RingManagerAgentError(
                'GET %s failed with HTTP %s: %s' % (
                    url, status, body.decode('utf-8', 'replace')))
        return status, body, resp.info()

    def _json_request(self, source_url, path):
        _status, body, _headers = self._request(source_url, path)
        try:
            value = json.loads(body.decode('utf-8'))
        except (TypeError, ValueError, UnicodeDecodeError) as err:
            raise RingManagerAgentError(
                'GET %s returned invalid JSON: %s' % (path, err))
        if not isinstance(value, dict):
            raise RingManagerAgentError(
                'GET %s returned JSON %s, not an object' % (
                    path, type(value).__name__))
        return value

    def _artifact_name(self, file_info):
        name = file_info.get('name')
        if not isinstance(name, str) or not name:
            raise RingManagerAgentError(
                'manifest files must contain objects with name')
        if name in ('.', '..') or '/' in name or '\\' in name:
            raise RingManagerAgentError(
                'manifest file name %r is not safe for swift_dir' % name)
        if (name.startswith('.') or name == INSTALL_JOURNAL or
                '.tmp-' in name or BACKUP_MARKER in name):
            raise RingManagerAgentError(
                'manifest file name %r is reserved for ring-manager-agent' %
                name)
        return name

    def _artifact_path(self, file_info):
        return os.path.join(self.swift_dir, self._artifact_name(file_info))

    def _manifest_files(self, manifest, field_name='desired manifest'):
        version = str(manifest.get('version', ''))
        if not version:
            raise RingManagerAgentError('%s has no version' % field_name)
        files = manifest.get('files', [])
        if not isinstance(files, list):
            raise RingManagerAgentError(
                '%s files must be a list' % field_name)
        seen_names = set()
        for file_info in files:
            if not isinstance(file_info, dict):
                raise RingManagerAgentError(
                    '%s files must contain objects' % field_name)
            name = self._artifact_name(file_info)
            if name in seen_names:
                raise RingManagerAgentError(
                    '%s contains duplicate file name %s' % (
                        field_name, name))
            seen_names.add(name)
        return version, files

    def _mkdirs(self, path):
        directory = os.path.dirname(path)
        self._ensure_directory_durable(directory)

    def _ensure_directory_durable(self, directory):
        if not directory or os.path.isdir(directory):
            return
        missing = []
        path = directory
        while path and not os.path.isdir(path):
            missing.append(path)
            parent = os.path.dirname(path)
            if parent == path:
                break
            path = parent
        try:
            os.makedirs(directory)
        except OSError as err:
            if err.errno != errno.EEXIST or not os.path.isdir(directory):
                raise
        for created in reversed(missing):
            parent = os.path.dirname(created)
            if parent:
                self._fsync_dir_strict(parent)

    def _fsync_dir_strict(self, directory):
        dirfd = os.open(directory, os.O_DIRECTORY | os.O_RDONLY)
        try:
            fsync(dirfd)
        finally:
            os.close(dirfd)

    def _fsync_parent(self, path):
        directory = os.path.dirname(path)
        if directory:
            self._fsync_dir_strict(directory)

    def _write_file_atomic(self, path, body):
        self._mkdirs(path)
        temp_path = '%s.tmp-%s' % (path, uuid.uuid4().hex)
        try:
            with open(temp_path, 'wb') as fp:
                fp.write(body)
                fp.flush()
                fsync(fp.fileno())
            os.rename(temp_path, path)
            temp_path = None
            self._fsync_parent(path)
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except OSError as err:
                    if err.errno != errno.ENOENT:
                        raise

    def _write_json_atomic(self, path, value):
        body = json.dumps(value, sort_keys=True, indent=2).encode('ascii')
        self._write_file_atomic(path, body + b'\n')

    def _read_json(self, path, default=None):
        try:
            with open(path, 'r') as fp:
                return json.load(fp)
        except IOError as err:
            if err.errno == errno.ENOENT:
                return default
            raise

    def _read_state(self):
        try:
            state = self._read_json(self.state_file, {})
        except (IOError, ValueError) as err:
            raise RingManagerAgentLocalError(
                'local ring-manager agent state read failed: %s' % err)
        if not isinstance(state, dict):
            raise RingManagerAgentLocalError(
                'local ring-manager agent state must be an object')
        return state

    def _manifest_timestamp(self, manifest, field_name):
        value = manifest.get('created_at')
        if value in (None, ''):
            raise RingManagerAgentError('%s has no created_at' % field_name)
        try:
            return NormalTimestamp(value).internal
        except (TypeError, ValueError, AssertionError):
            raise RingManagerAgentError(
                '%s has invalid created_at %r' % (field_name, value))

    def _state_manifest_timestamp(self, state):
        value = state.get('installed_ring_created_at')
        if value in (None, ''):
            value = state.get('latest_ring_created_at')
        if value in (None, ''):
            manifest = state.get('manifest', {})
            if isinstance(manifest, dict):
                value = manifest.get('created_at')
        if value in (None, ''):
            return None
        try:
            return NormalTimestamp(value).internal
        except (TypeError, ValueError, AssertionError):
            raise RingManagerAgentLocalError(
                'local ring-manager agent state has invalid installed ring '
                'created_at %r' % value)

    def _check_manifest_not_rollback(self, source_url, manifest):
        if self.allow_ring_version_rollback:
            return
        state = self._read_state()
        installed_created_at = self._state_manifest_timestamp(state)
        if installed_created_at is None:
            return
        manifest_created_at = self._manifest_timestamp(
            manifest, 'desired manifest')
        if manifest_created_at >= installed_created_at:
            return
        installed_version = state.get('installed_ring_version') or \
            state.get('latest_ring_version')
        raise RingManagerAgentError(
            'source %s desired manifest version %s created at %s is older '
            'than installed version %s created at %s' % (
                source_url, manifest.get('version'), manifest_created_at,
                installed_version, installed_created_at))

    def _delete_file_durable(self, path):
        try:
            os.unlink(path)
        except OSError as err:
            if err.errno == errno.ENOENT:
                return
            raise
        self._fsync_parent(path)

    def _read_file(self, path):
        try:
            with open(path, 'rb') as fp:
                return fp.read()
        except IOError as err:
            if err.errno == errno.ENOENT:
                return None
            raise

    def _verify_existing(self, path, file_info):
        return self._verified_existing_etag(path, file_info) is not None

    def _verified_existing_etag(self, path, file_info):
        body = self._read_file(path)
        if body is None:
            return None
        if not self._body_matches(body, file_info):
            return None
        return md5(body, usedforsecurity=False).hexdigest()

    def _body_matches(self, body, file_info):
        expected_bytes = file_info.get('bytes')
        if expected_bytes is not None and len(body) != expected_bytes:
            return False
        expected_sha256 = file_info.get('sha256')
        if expected_sha256 is not None:
            actual = hashlib.sha256(body).hexdigest()
            if actual != expected_sha256:
                return False
        return True

    def _verify_download(self, body, file_info, url):
        expected_bytes = file_info.get('bytes')
        if expected_bytes is not None and len(body) != expected_bytes:
            raise RingManagerAgentError(
                '%s returned %d bytes, expected %d' % (
                    url, len(body), expected_bytes))
        expected_sha256 = file_info.get('sha256')
        if expected_sha256 is not None:
            actual = hashlib.sha256(body).hexdigest()
            if actual != expected_sha256:
                stats_increment(self.logger, 'agent.checksum_failures')
                raise RingManagerAgentError(
                    '%s sha256 mismatch: got %s, expected %s' % (
                        url, actual, expected_sha256))

    def _verify_staged_file(self, path, file_info):
        body = self._read_file(path)
        if body is None or not self._body_matches(body, file_info):
            raise RingManagerAgentError(
                'staged file %s failed manifest verification' % path)

    def _file_url(self, version, file_info):
        url = file_info.get('url') or \
            '/api/v1/rings/releases/%s/files/%s' % (
                self._safe_id(version), quote(self._artifact_name(file_info),
                                              safe=''))
        try:
            return validate_relative_api_url(url, 'manifest artifact URL')
        except ValueError as err:
            raise RingManagerAgentError(str(err))

    def _stage_file(self, source_url, version, file_info):
        local_path = self._artifact_path(file_info)
        headers = {}
        if file_info.get('sha256'):
            etag = self._verified_existing_etag(local_path, file_info)
            if etag:
                headers['If-None-Match'] = etag

        file_url = self._file_url(version, file_info)
        status, body, _headers = self._request(
            source_url, file_url, headers=headers)
        if status == 304:
            if 'If-None-Match' not in headers:
                raise RingManagerAgentError(
                    '%s returned 304 Not Modified without a conditional '
                    'request' % file_url)
            if not self._verify_existing(local_path, file_info):
                raise RingManagerAgentError(
                    '%s returned 304 Not Modified but local file %s no '
                    'longer matches the manifest' % (file_url, local_path))
            return {
                'result': 'unchanged',
                'local_path': local_path,
                'temp_path': None,
                'file_info': file_info,
            }

        self._verify_download(body, file_info, file_url)
        stats_increment(self.logger, 'agent.bytes_downloaded', len(body))

        temp_path = None
        try:
            self._mkdirs(local_path)
            temp_path = '%s.tmp-%s' % (local_path, uuid.uuid4().hex)
            try:
                with open(temp_path, 'wb') as fp:
                    fp.write(body)
                    fp.flush()
                    fsync(fp.fileno())
                self._verify_staged_file(temp_path, file_info)
            except Exception as err:
                try:
                    os.unlink(temp_path)
                    self._fsync_parent(temp_path)
                except OSError as cleanup_err:
                    if cleanup_err.errno != errno.ENOENT:
                        raise RingManagerAgentLocalError(
                            'local cleanup after staging failure failed for '
                            '%s: %s (original error: %s)' % (
                                temp_path, cleanup_err, err))
                raise RingManagerAgentLocalError(
                    'local staging failed for %s: %s' % (
                        local_path, err))
        except RingManagerAgentLocalError:
            raise
        except Exception as err:
            raise RingManagerAgentLocalError(
                'local staging failed for %s: %s' % (local_path, err))
        return {
            'result': 'downloaded',
            'local_path': local_path,
            'temp_path': temp_path,
            'file_info': file_info,
        }

    def _write_install_journal(self, version, staged_files):
        files = []
        for staged in staged_files:
            if not staged.get('temp_path'):
                continue
            files.append({
                'name': self._artifact_name(staged['file_info']),
                'local_path': staged['local_path'],
                'temp_path': staged['temp_path'],
                'backup_path': staged.get('backup_path'),
                'had_existing': staged.get('had_existing', False),
                'file_info': staged['file_info'],
            })
        if not files:
            return False
        journal = {
            'version': version,
            'swift_dir': self.swift_dir,
            'created_at': self._timestamp_internal(),
            'files': files,
        }
        self._write_json_atomic(self.install_journal, journal)
        return True

    def _clear_install_journal(self):
        self._delete_file_durable(self.install_journal)

    def _prepare_install_transaction(self, version, staged_files):
        install_files = [
            staged for staged in staged_files if staged.get('temp_path')]
        for staged in install_files:
            local_path = staged['local_path']
            had_existing = os.path.exists(local_path)
            staged['had_existing'] = had_existing
            if had_existing:
                staged['backup_path'] = '%s%s%s' % (
                    local_path, BACKUP_MARKER, uuid.uuid4().hex)
        journal_written = self._write_install_journal(version, install_files)
        for staged in install_files:
            backup_path = staged.get('backup_path')
            if not backup_path:
                continue
            os.link(staged['local_path'], backup_path)
            self._fsync_parent(backup_path)
        return journal_written

    def _install_staged_files(self, version, staged_files):
        installed = []
        journal_written = False
        try:
            journal_written = self._prepare_install_transaction(
                version, staged_files)
            for staged in staged_files:
                temp_path = staged.get('temp_path')
                if not temp_path:
                    continue
                local_path = staged['local_path']
                os.rename(temp_path, local_path)
                staged['temp_path'] = None
                staged['installed'] = True
                self._fsync_parent(local_path)
                self._verify_staged_file(local_path, staged['file_info'])
                installed.append(staged)
            if journal_written:
                self._clear_install_journal()
                journal_written = False
        except Exception as err:
            rollback_errors = self._rollback_installed_files(staged_files)
            if not rollback_errors and journal_written:
                self._clear_install_journal()
            if rollback_errors:
                raise RingManagerAgentError(
                    'ring install failed and rollback was incomplete: %s; '
                    'rollback errors: %s' % (
                        err, '; '.join(rollback_errors)))
            raise
        self._cleanup_backup_files(installed)

    def _rollback_installed_files(self, staged_files):
        errors = []
        for staged in reversed(staged_files):
            local_path = staged.get('local_path')
            backup_path = staged.get('backup_path')
            try:
                if staged.get('installed'):
                    if backup_path:
                        os.rename(backup_path, local_path)
                        staged['backup_path'] = None
                    else:
                        try:
                            os.unlink(local_path)
                        except OSError as err:
                            if err.errno != errno.ENOENT:
                                raise
                    self._fsync_parent(local_path)
                elif backup_path:
                    try:
                        os.unlink(backup_path)
                        staged['backup_path'] = None
                        self._fsync_parent(backup_path)
                    except OSError as err:
                        if err.errno != errno.ENOENT:
                            raise
            except Exception as err:
                errors.append('%s: %s' % (local_path, err))
        return errors

    def _cleanup_backup_files(self, staged_files):
        synced_dirs = set()
        errors = []
        for staged in staged_files:
            backup_path = staged.get('backup_path')
            if not backup_path:
                continue
            try:
                os.unlink(backup_path)
                staged['backup_path'] = None
                synced_dirs.add(os.path.dirname(backup_path))
            except OSError as err:
                if err.errno != errno.ENOENT:
                    errors.append('%s: %s' % (backup_path, err))
        for directory in synced_dirs:
            self._fsync_dir_strict(directory)
        if errors:
            raise RingManagerAgentError(
                'unable to remove ring-manager backup files: %s' %
                '; '.join(errors))

    def _cleanup_staged_files(self, staged_files):
        synced_dirs = set()
        for staged in staged_files:
            temp_path = staged.get('temp_path')
            if temp_path:
                try:
                    os.unlink(temp_path)
                    staged['temp_path'] = None
                    synced_dirs.add(os.path.dirname(temp_path))
                except OSError as err:
                    if err.errno != errno.ENOENT:
                        self.logger.warning(
                            'Unable to remove staged ring file %s: %s',
                            temp_path, err)
            backup_path = staged.get('backup_path')
            if not backup_path:
                continue
            if staged.get('installed'):
                continue
            try:
                os.unlink(backup_path)
                staged['backup_path'] = None
                synced_dirs.add(os.path.dirname(backup_path))
            except OSError as err:
                if err.errno != errno.ENOENT:
                    self.logger.warning(
                        'Unable to remove ring-manager backup %s: %s',
                        backup_path, err)
        for directory in synced_dirs:
            if directory:
                self._fsync_dir_strict(directory)

    def _recover_install_journal(self):
        missing = object()
        try:
            journal = self._read_json(self.install_journal, missing)
        except Exception as err:
            raise RingManagerAgentLocalError(
                'unable to read ring install journal %s: %s' % (
                    self.install_journal, err))
        if journal is missing:
            return
        if not isinstance(journal, dict):
            raise RingManagerAgentLocalError(
                'ring install journal %s is malformed' % self.install_journal)
        files = journal.get('files')
        if not isinstance(files, list):
            raise RingManagerAgentLocalError(
                'ring install journal %s is malformed' % self.install_journal)
        errors = []
        synced_dirs = set()
        try:
            for entry in reversed(files):
                local_path = entry.get('local_path')
                temp_path = entry.get('temp_path')
                backup_path = entry.get('backup_path')
                file_info = entry.get('file_info') or {}
                try:
                    if backup_path and os.path.exists(backup_path):
                        os.rename(backup_path, local_path)
                        synced_dirs.add(os.path.dirname(local_path))
                    elif entry.get('had_existing') and self._verify_existing(
                            local_path, file_info):
                        raise RingManagerAgentError(
                            'missing rollback backup %s for %s' % (
                                backup_path, local_path))
                    elif (not entry.get('had_existing') and
                          self._verify_existing(local_path, file_info)):
                        os.unlink(local_path)
                        synced_dirs.add(os.path.dirname(local_path))
                    if temp_path:
                        try:
                            os.unlink(temp_path)
                            synced_dirs.add(os.path.dirname(temp_path))
                        except OSError as err:
                            if err.errno != errno.ENOENT:
                                raise
                except Exception as err:
                    errors.append('%s: %s' % (local_path, err))
            for directory in synced_dirs:
                if directory:
                    self._fsync_dir_strict(directory)
            if errors:
                raise RingManagerAgentLocalError(
                    'ring install journal recovery failed: %s' %
                    '; '.join(errors))
            self._clear_install_journal()
        except RingManagerAgentLocalError:
            raise
        except Exception as err:
            raise RingManagerAgentLocalError(
                'ring install journal recovery failed: %s' % err)
        stats_increment(self.logger, 'agent.install_recoveries')

    def _operator_attention_status(self, err=None, source_errors=None,
                                   additional_reasons=None):
        source_errors = list(source_errors or [])
        reasons = list(additional_reasons or [])
        backup_files = []
        install_journal_path = None
        scan_error = None

        if isinstance(err, RingManagerAgentLocalError):
            reasons.append('local_failure')
        if source_errors:
            reasons.append('source_errors')

        try:
            if os.path.exists(self.install_journal):
                install_journal_path = self.install_journal
                reasons.append('install_journal_present')
            names = os.listdir(self.swift_dir)
            for name in sorted(names):
                if BACKUP_MARKER in name:
                    backup_files.append(os.path.join(self.swift_dir, name))
            if backup_files:
                reasons.append('rollback_backups_present')
        except OSError as scan_err:
            scan_error = str(scan_err)
            if scan_err.errno != errno.ENOENT:
                reasons.append('attention_scan_failed')

        reasons = sorted(set(reasons))
        status = {
            'needed': bool(reasons),
            'reasons': reasons,
            'source_errors': len(source_errors),
            'install_journal_path': install_journal_path,
            'backup_files_count': len(backup_files),
            'backup_files': backup_files[:OPERATOR_ATTENTION_FILE_LIMIT],
        }
        if len(backup_files) > OPERATOR_ATTENTION_FILE_LIMIT:
            status['backup_files_truncated'] = True
        if scan_error:
            status['scan_error'] = scan_error
        return status

    def _ring_file_inventory(self, name):
        path = os.path.join(self.swift_dir, name)
        info = {
            'name': name,
            'path': path,
        }
        try:
            before = os.stat(path)
            info.update({
                'bytes': before.st_size,
                'mtime': self._timestamp_internal(before.st_mtime),
            })
            digest = hashlib.sha256()
            with open(path, 'rb') as fp:
                while True:
                    chunk = fp.read(65536)
                    if not chunk:
                        break
                    digest.update(chunk)
            info['sha256'] = digest.hexdigest()
            ring_data = RingData.load(path)
            after = os.stat(path)
            before_identity = (
                before.st_ino, before.st_size,
                getattr(before, 'st_mtime_ns', before.st_mtime))
            after_identity = (
                after.st_ino, after.st_size,
                getattr(after, 'st_mtime_ns', after.st_mtime))
            if before_identity != after_identity:
                raise RingManagerAgentLocalError(
                    'ring file changed during inventory')
            info.update({
                'status': 'valid',
                'swift_ring_version': ring_data.version,
                'part_power': ring_data.part_power,
                'replicas': ring_data.replica_count,
            })
        except Exception as err:
            info.update({
                'status': 'error',
                'error': str(err),
            })
        return info

    def _local_ring_names(self):
        try:
            return sorted(
                name for name in os.listdir(self.swift_dir)
                if name.endswith('.ring.gz'))
        except OSError as err:
            raise RingManagerAgentLocalError(
                'unable to inventory local rings in %s: %s' % (
                    self.swift_dir, err))

    def _observe_local_rings(self):
        names = self._local_ring_names()
        files = [self._ring_file_inventory(name) for name in names]
        valid = sum(item['status'] == 'valid' for item in files)
        invalid = len(files) - valid
        return {
            'mode': 'observe',
            'files_observed': len(files),
            'files_valid': valid,
            'files_invalid': invalid,
            'files': files,
        }

    def _is_sha256(self, value):
        if not isinstance(value, str) or len(value) != 64:
            return False
        return all(char in '0123456789abcdefABCDEF' for char in value)

    def _comparison_file(self, local, file_info):
        name = self._artifact_name(file_info)
        expected = {
            'name': name,
            'path': os.path.join(self.swift_dir, name),
            'expected_bytes': file_info.get('bytes'),
            'expected_sha256': file_info.get('sha256'),
        }
        if local is None:
            expected['status'] = 'missing'
            return expected

        result = dict(local)
        result['ring_status'] = result.pop('status')
        result.update(expected)
        if result['ring_status'] != 'valid':
            result['status'] = 'error'
        elif (expected['expected_bytes'] is not None and
              result['bytes'] != expected['expected_bytes']):
            result['status'] = 'stale'
        elif not self._is_sha256(expected['expected_sha256']):
            result['status'] = 'unknown'
        elif (result['sha256'].lower() ==
              expected['expected_sha256'].lower()):
            result['status'] = 'matching'
        else:
            result['status'] = 'stale'
        return result

    def _validate_local_rings(self, manifest):
        version, manifest_files = self._manifest_files(
            manifest, 'selected manifest')
        if self.release != 'latest' and version != self.release:
            raise RingManagerAgentError(
                'selected release %s returned manifest version %s' % (
                    self.release, version))
        if not manifest_files:
            raise RingManagerAgentError(
                'selected manifest contains no ring files')
        for file_info in manifest_files:
            name = self._artifact_name(file_info)
            if not name.endswith('.ring.gz'):
                raise RingManagerAgentError(
                    'selected manifest file %s is not a ring file' % name)

        names = self._local_ring_names()
        local_files = dict(
            (name, self._ring_file_inventory(name)) for name in names)
        files = []
        for file_info in manifest_files:
            name = self._artifact_name(file_info)
            files.append(self._comparison_file(
                local_files.pop(name, None), file_info))
        for name in sorted(local_files):
            local = dict(local_files[name])
            local['ring_status'] = local.pop('status')
            local['status'] = 'extra'
            files.append(local)

        counts = dict((status, sum(
            item['status'] == status for item in files)) for status in (
                'matching', 'stale', 'missing', 'unknown', 'extra',
                'error'))
        return {
            'release_selector': self.release,
            'release': version,
            'converged': all(
                item['status'] == 'matching' for item in files),
            'files_expected': len(manifest_files),
            'files_local': len(names),
            'files_matching': counts['matching'],
            'files_stale': counts['stale'],
            'files_missing': counts['missing'],
            'files_unknown': counts['unknown'],
            'files_extra': counts['extra'],
            'files_error': counts['error'],
            'files': files,
        }

    def observe_once(self):
        started_at = self._timestamp()
        stats_increment(self.logger, 'agent.observe.attempts')
        try:
            result = self._observe_local_rings()
        except Exception as err:
            ended_at = self._timestamp()
            stats_increment(self.logger, 'agent.observe.failures')
            stats_timing(
                self.logger, 'agent.observe.timing',
                float(ended_at) - float(started_at))
            stats = self._failure_stats(started_at, err, [])
            stats['observe_time'] = stats.pop('sync_time')
            stats['sources'] = {}
            stats['source_errors'] = {}
            self._dump_recon(stats)
            raise

        ended_at = self._timestamp()
        observed_at = ended_at.internal
        attention_reasons = []
        if result['files_invalid']:
            attention_reasons.append('invalid_ring_files')
        operator_attention = self._operator_attention_status(
            additional_reasons=attention_reasons)
        self._emit_operator_attention_metrics(operator_attention)
        recon = dict(result)
        recon.update({
            'success': True,
            'swift_dir': self.swift_dir,
            'observe_time': float(ended_at) - float(started_at),
            'last_attempt': observed_at,
            'last_attempted_at': observed_at,
            'last_success': observed_at,
            'last_observed_at': observed_at,
            'error': {},
            'operator_attention': operator_attention,
        })
        stats_increment(self.logger, 'agent.observe.successes')
        stats_timing(
            self.logger, 'agent.observe.timing',
            float(ended_at) - float(started_at))
        stats_increment(
            self.logger, 'agent.observe.files', result['files_observed'])
        stats_increment(
            self.logger, 'agent.observe.files_valid', result['files_valid'])
        stats_increment(
            self.logger, 'agent.observe.files_invalid',
            result['files_invalid'])
        self._dump_recon(recon)
        return result

    def _emit_operator_attention_metrics(self, status):
        if not status.get('needed'):
            return
        stats_increment(self.logger, 'agent.operator_attention')
        if status.get('install_journal_path'):
            stats_increment(self.logger, 'agent.operator_attention.journal')
        backup_count = status.get('backup_files_count', 0)
        if backup_count:
            stats_increment(
                self.logger, 'agent.operator_attention.backup_files',
                backup_count)
        reasons = status.get('reasons') or []
        if 'local_failure' in reasons:
            stats_increment(
                self.logger, 'agent.operator_attention.local_failures')
        if 'source_errors' in reasons:
            stats_increment(
                self.logger, 'agent.operator_attention.source_errors')

    def _sync_manifest_files(self, source_url, manifest):
        version, files = self._manifest_files(manifest)

        downloaded = unchanged = 0
        installed_files = []
        staged_files = []
        try:
            for file_info in files:
                staged = self._stage_file(
                    source_url, version, file_info)
                result = staged['result']
                local_path = staged['local_path']
                if result == 'downloaded':
                    downloaded += 1
                else:
                    unchanged += 1
                staged_files.append(staged)
                installed_files.append({
                    'name': self._artifact_name(file_info),
                    'path': local_path,
                    'bytes': file_info.get('bytes'),
                    'sha256': file_info.get('sha256'),
                })
            try:
                self._install_staged_files(version, staged_files)
            except Exception as err:
                stats_increment(self.logger, 'agent.install_failures')
                if isinstance(err, RingManagerAgentLocalError):
                    raise
                raise RingManagerAgentInstallError(str(err))
        except Exception as err:
            try:
                self._cleanup_staged_files(staged_files)
            except Exception as cleanup_err:
                raise RingManagerAgentLocalError(
                    'local cleanup after ring sync failure failed: %s '
                    '(original error: %s)' % (cleanup_err, err))
            raise

        return version, installed_files, downloaded, unchanged

    def _write_state(self, source_url, version, manifest, installed_files,
                     synced_at):
        created_at = self._manifest_timestamp(manifest, 'desired manifest')
        state = {
            'source': source_url,
            'installed_ring_version': version,
            'installed_ring_created_at': created_at,
            'synced_at': synced_at,
            'swift_dir': self.swift_dir,
            'files': installed_files,
            'manifest': manifest,
        }
        self._write_json_atomic(self.state_file, state)

    def _dump_recon(self, stats):
        try:
            self._ensure_directory_durable(os.path.dirname(self.recon_cache))
        except Exception as err:
            self.logger.exception('Exception creating recon cache path: %s' %
                                  err)
            return
        current_fields = MODE_RECON_FIELDS.get(stats.get('mode'), frozenset())
        all_fields = frozenset().union(
            *MODE_RECON_FIELDS.values()) | frozenset(
                LEGACY_ENFORCE_RECON_FIELDS)
        stale_fields = all_fields - current_fields
        for field in stale_fields:
            stats.setdefault(field, {})
        if stats.get('success'):
            # Recon cache updates merge nested keys, so success must delete
            # stale failure fields instead of relying on omission.
            stats = dict(stats)
            stats.setdefault('source_errors', {})
            stats.setdefault('sources', {})
        dump_recon_cache({'ring_manager_agent': stats},
                         self.recon_cache, self.logger)

    def _success_stats(self, source_url, started_at, ended_at, synced_at,
                       result, source_errors=None):
        source_errors = list(source_errors or [])
        stats = dict(result)
        operator_attention = self._operator_attention_status(
            source_errors=source_errors)
        self._emit_operator_attention_metrics(operator_attention)
        stats.update({
            'mode': self.mode,
            'source': source_url,
            'success': True,
            'swift_dir': self.swift_dir,
            'sync_time': float(ended_at) - float(started_at),
            'last_attempt': ended_at.internal,
            'last_attempted_at': synced_at,
            'last_success': ended_at.internal,
            'last_synced_at': synced_at,
            'error': {},
            'operator_attention': operator_attention,
        })
        if source_errors:
            stats['source_errors'] = source_errors
        return stats

    def _failure_stats(self, started_at, err, source_errors):
        ended_at = self._timestamp()
        attempted_at = ended_at.internal
        operator_attention = self._operator_attention_status(err)
        self._emit_operator_attention_metrics(operator_attention)
        return {
            'mode': self.mode,
            'sources': list(self.ring_manager_urls),
            'success': False,
            'sync_time': float(ended_at) - float(started_at),
            'last_attempt': ended_at.internal,
            'last_attempted_at': attempted_at,
            'error': str(err),
            'source_errors': source_errors,
            'operator_attention': operator_attention,
        }

    def _sync_from_source(self, source_url):
        manifest = self._json_request(
            source_url, '/api/v1/rings/releases/desired/manifest/')
        if manifest.get('desired') is not True:
            raise RingManagerAgentError(
                'source %s desired manifest is not marked desired' %
                source_url)
        self._manifest_timestamp(manifest, 'desired manifest')
        self._check_manifest_not_rollback(source_url, manifest)
        version, installed_files, downloaded, unchanged = \
            self._sync_manifest_files(source_url, manifest)
        synced_at = self._timestamp_internal()
        try:
            self._write_state(source_url, version, manifest, installed_files,
                              synced_at)
        except Exception as err:
            raise RingManagerAgentLocalError(
                'local ring-manager agent state write failed: %s' % err)
        return {
            'desired_ring_version': version,
            'files_installed': len(installed_files),
            'files_downloaded': downloaded,
            'files_unchanged': unchanged,
        }, synced_at

    def _record_sync_failure(self, started_at, err, source_errors):
        ended_at = self._timestamp()
        stats_increment(self.logger, 'agent.sync.failures')
        stats_timing(
            self.logger, 'agent.sync.timing',
            float(ended_at) - float(started_at))
        self._dump_recon(self._failure_stats(started_at, err, source_errors))
        try:
            err.ring_manager_failure_recorded = True
        except AttributeError:
            pass

    def _validation_success_stats(self, source_url, started_at, ended_at,
                                  validated_at, result,
                                  source_errors=None):
        source_errors = list(source_errors or [])
        stats = dict(result)
        files = stats.pop('files')
        attention_reasons = []
        if not result['converged']:
            attention_reasons.append('ring_validation_not_converged')
        operator_attention = self._operator_attention_status(
            source_errors=source_errors,
            additional_reasons=attention_reasons)
        self._emit_operator_attention_metrics(operator_attention)
        stats.update({
            'mode': self.mode,
            'validation_source': source_url,
            'success': True,
            'swift_dir': self.swift_dir,
            'validation_files': files,
            'validation_time': float(ended_at) - float(started_at),
            'last_attempt': ended_at.internal,
            'last_attempted_at': validated_at,
            'last_success': ended_at.internal,
            'last_validated_at': validated_at,
            'error': {},
            'operator_attention': operator_attention,
        })
        if source_errors:
            stats['source_errors'] = source_errors
        return stats

    def _validation_failure_stats(self, started_at, err, source_errors):
        stats = self._failure_stats(started_at, err, source_errors)
        stats['validation_time'] = stats.pop('sync_time')
        return stats

    def _record_validation_failure(self, started_at, err, source_errors):
        ended_at = self._timestamp()
        stats_increment(self.logger, 'agent.validate.failures')
        stats_timing(
            self.logger, 'agent.validate.timing',
            float(ended_at) - float(started_at))
        self._dump_recon(self._validation_failure_stats(
            started_at, err, source_errors))
        try:
            err.ring_manager_failure_recorded = True
        except AttributeError:
            pass

    def _validate_from_source(self, source_url):
        manifest = self._json_request(
            source_url, self.release_manifest_path)
        result = self._validate_local_rings(manifest)
        return result, self._timestamp_internal()

    def _emit_validation_metrics(self, result):
        stats_increment(self.logger, 'agent.validate.successes')
        for status in ('matching', 'stale', 'missing', 'unknown', 'extra',
                       'error'):
            stats_increment(
                self.logger, 'agent.validate.files_%s' % status,
                result['files_%s' % status])
        if result['converged']:
            stats_increment(self.logger, 'agent.validate.converged')
        else:
            stats_increment(self.logger, 'agent.validate.not_converged')

    def _validate_once_sources(self, started_at):
        source_errors = []
        for source_url in self._source_urls():
            try:
                result, validated_at = self._validate_from_source(source_url)
            except RingManagerAgentLocalError as err:
                self.logger.warning(
                    'Aborting ring validation after local failure from %s: '
                    '%s', source_url, err)
                source_errors.append({
                    'source': source_url,
                    'error': str(err),
                })
                self._record_validation_failure(
                    started_at, err, source_errors)
                raise
            except Exception as err:
                stats_increment(self.logger, 'agent.source.failures')
                self.logger.warning(
                    'Unable to validate rings from %s: %s', source_url, err)
                source_errors.append({
                    'source': source_url,
                    'error': str(err),
                })
                continue

            ended_at = self._timestamp()
            self._emit_validation_metrics(result)
            stats_timing(
                self.logger, 'agent.validate.timing',
                float(ended_at) - float(started_at))
            self._dump_recon(self._validation_success_stats(
                source_url, started_at, ended_at, validated_at, result,
                source_errors=source_errors))
            return result

        err = RingManagerAgentError(
            'all ring-manager sources failed: %s' % '; '.join(
                '%s: %s' % (item['source'], item['error'])
                for item in source_errors))
        self._record_validation_failure(started_at, err, source_errors)
        raise err

    def validate_once(self):
        started_at = self._timestamp()
        stats_increment(self.logger, 'agent.validate.attempts')
        try:
            return self._validate_once_sources(started_at)
        except Exception as err:
            if getattr(err, 'ring_manager_failure_recorded', False):
                raise
            self._record_validation_failure(started_at, err, [])
            raise

    def _sync_once_locked(self, started_at):
        source_errors = []
        self._recover_install_journal()
        for source_url in self._source_urls():
            try:
                result, synced_at = self._sync_from_source(source_url)
            except RingManagerAgentLocalError as err:
                self.logger.warning(
                    'Aborting ring sync after local failure from %s: %s',
                    source_url, err)
                source_errors.append({
                    'source': source_url,
                    'error': str(err),
                })
                self._record_sync_failure(started_at, err, source_errors)
                raise
            except Exception as err:
                stats_increment(self.logger, 'agent.source.failures')
                self.logger.warning(
                    'Unable to sync rings from %s: %s', source_url, err)
                source_errors.append({
                    'source': source_url,
                    'error': str(err),
                })
                continue

            ended_at = self._timestamp()
            stats_increment(self.logger, 'agent.sync.successes')
            stats_timing(
                self.logger, 'agent.sync.timing',
                float(ended_at) - float(started_at))
            stats_increment(
                self.logger, 'agent.files.downloaded',
                result.get('files_downloaded', 0))
            stats_increment(
                self.logger, 'agent.files.unchanged',
                result.get('files_unchanged', 0))
            stats_increment(
                self.logger, 'agent.files.installed',
                result.get('files_installed', 0))
            self._dump_recon(self._success_stats(
                source_url, started_at, ended_at, synced_at, result,
                source_errors=source_errors))
            return result

        err = RingManagerAgentError(
            'all ring-manager sources failed: %s' % '; '.join(
                '%s: %s' % (item['source'], item['error'])
                for item in source_errors))
        self._record_sync_failure(started_at, err, source_errors)
        raise err

    def sync_once(self):
        started_at = self._timestamp()
        stats_increment(self.logger, 'agent.sync.attempts')
        try:
            self._ensure_directory_durable(self.swift_dir)
            with lock_path(self.swift_dir, timeout=self.lock_timeout,
                           name=LOCK_NAME):
                return self._sync_once_locked(started_at)
        except Exception as err:
            if getattr(err, 'ring_manager_failure_recorded', False):
                raise
            self._record_sync_failure(started_at, err, [])
            raise

    def run_once(self, *args, **kwargs):
        if self.mode == 'observe':
            result = self.observe_once()
            self.logger.info(
                'Observed %(files_observed)d local ring files: '
                '%(files_valid)d valid, %(files_invalid)d invalid' % result)
            return result
        if self.mode == 'validate-only':
            result = self.validate_once()
            self.logger.info(
                'Validated ring-manager release %(release)s: '
                '%(files_matching)d matching, %(files_stale)d stale, '
                '%(files_missing)d missing, %(files_unknown)d unknown, '
                '%(files_extra)d extra, %(files_error)d errors' % result)
            return result
        result = self.sync_once()
        self.logger.info(
            'Synced desired ring-manager version %(desired_ring_version)s: '
            '%(files_downloaded)d files downloaded, '
            '%(files_unchanged)d unchanged' % result)
        return result

    def run_forever(self, *args, **kwargs):
        if self.jitter:
            self.sleep(self.random_func() * self.jitter)
        while True:
            started_at = self._timestamp()
            try:
                self.run_once(*args, **kwargs)
            except Exception:
                self.logger.exception('Error running ring-manager agent')
            elapsed = float(self._timestamp()) - float(started_at)
            delay = max(0.0, self.interval - elapsed)
            if self.jitter:
                delay += self.random_func() * self.jitter
            self.sleep(delay)


def main():
    conf_file, options = parse_options(once=True)
    run_daemon(RingManagerAgent, conf_file, 'ring-manager-agent', **options)


if __name__ == '__main__':
    sys.exit(main())
