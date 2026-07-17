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
import errno
import json
import optparse
import os
import re
import sys
import uuid

from urllib.parse import quote, urljoin, urlparse

from swift.common import exceptions as swift_exceptions
from swift.common.concurrency import socket, urllib_request
from swift.common.ring.builder import RingBuilder
from swift.common.recon import DEFAULT_RECON_CACHE_PATH, \
    RECON_RING_MANAGER_FILE
from swift.common.utils import NullLogger, config_true_value, \
    dump_recon_cache, fsync, fsync_dir, get_logger, list_from_csv, \
    lock_path, mkdirs, non_negative_float, readconf
from swift.common.utils import md5
from swift.ring_manager.common import DEFAULT_STATE_CHANGE_HOOK_TIMEOUT, \
    DEFAULT_RING_BUILDER_DIR, load_secret, NormalTimestamp, \
    RING_MANAGER_SYNC_JOURNAL, StateChangeHook, normal_timestamp, \
    stats_increment, stats_timing, validate_path_component, \
    validate_relative_api_url


USER_AGENT = 'swift-ring-manager-sync'
SYNC_CONF_SECTION = 'ring-manager-sync'


class RingManagerSyncError(Exception):
    pass


class RingManagerSyncLocalError(RingManagerSyncError):
    pass


class RingManagerSync(object):
    """
    Pull published ring-manager state and artifacts from a source server.

    The syncer writes the same directory layout consumed by
    ``RingManagerStore`` so read-only and standby ring-manager servers can
    serve already-published manifests and artifacts locally.
    """

    def __init__(self, source_url, state_dir, artifact_dir, admin_key=None,
                 admin_key_file=None, auth_token=None, read_key=None,
                 read_key_file=None, read_auth_token=None, timeout=30,
                 opener=None,
                 recon_cache_path=DEFAULT_RECON_CACHE_PATH, recon_dump=True,
                 logger=None, time_func=NormalTimestamp.now,
                 state_change_hook=None, state_change_hook_timeout=None,
                 builder_dir=None, sync_builder_files=False,
                 sync_lock_timeout=30):
        if not source_url:
            raise RingManagerSyncError('source_url is required')
        if not state_dir:
            raise RingManagerSyncError('ring_manager_state_dir is required')
        if not artifact_dir:
            raise RingManagerSyncError('ring_artifact_dir is required')
        self.source_urls = self._normalize_source_urls(source_url)
        self.source_url = self.source_urls[0]
        self.state_dir = state_dir
        self.artifact_dir = artifact_dir
        self.builder_dir = builder_dir
        self.sync_builder_files = sync_builder_files
        if self.sync_builder_files and not self.builder_dir:
            raise RingManagerSyncError(
                'ring_builder_dir is required when sync_builder_files is true')
        try:
            self.read_key = load_secret(
                read_key, 'read_key', read_key_file, 'read_key_file')
            self.admin_key = load_secret(
                admin_key, 'admin_key', admin_key_file, 'admin_key_file')
        except ValueError as err:
            raise RingManagerSyncError(str(err))
        self.read_auth_token = read_auth_token
        self.auth_token = auth_token
        if self.sync_builder_files and not (self.admin_key or
                                            self.auth_token):
            raise RingManagerSyncError(
                'admin credentials are required when sync_builder_files is '
                'true')
        self.timeout = timeout
        self.opener = opener or urllib_request.urlopen
        self.recon_cache_path = recon_cache_path or DEFAULT_RECON_CACHE_PATH
        self.recon_cache = os.path.join(
            self.recon_cache_path, RECON_RING_MANAGER_FILE)
        self.recon_dump = recon_dump
        self.logger = logger or NullLogger()
        self.time_func = time_func
        self.sync_lock_timeout = sync_lock_timeout
        self.state_change_hook = StateChangeHook(
            state_change_hook, state_dir=state_dir,
            timeout=state_change_hook_timeout, logger=self.logger)

    def _normalize_source_urls(self, source_url):
        return _validate_source_urls(source_url)

    def _source_urls(self):
        return list(self.source_urls)

    def _timestamp(self, timestamp=None):
        timestamp = self.time_func() if timestamp is None else timestamp
        return normal_timestamp(timestamp)

    def _safe_id(self, value):
        return quote(str(value), safe='')

    def _api_url(self, source_url, path_or_url):
        return urljoin(source_url + '/', path_or_url)

    def _headers(self, extra=None, admin=False):
        headers = {'User-Agent': USER_AGENT}
        if admin:
            if self.admin_key:
                headers['X-Ring-Manager-Admin-Key'] = self.admin_key
            elif self.auth_token:
                headers['X-Auth-Token'] = self.auth_token
            if extra:
                headers.update(extra)
            return headers
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

    def _request(self, source_url, path_or_url, headers=None, admin=False):
        url = self._api_url(source_url, path_or_url)
        req = urllib_request.Request(
            url, headers=self._headers(headers, admin=admin))
        try:
            resp = self.opener(req, timeout=self.timeout)
        except urllib_request.HTTPError as err:
            if err.code == 304:
                return 304, b'', err.headers
            body = err.read()
            raise RingManagerSyncError(
                'GET %s failed with HTTP %s: %s' % (
                    url, err.code, body.decode('utf-8', 'replace')))
        except (urllib_request.URLError, socket.timeout) as err:
            raise RingManagerSyncError('GET %s failed: %s' % (url, err))

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
            raise RingManagerSyncError(
                'GET %s failed with HTTP %s: %s' % (
                    url, status, body.decode('utf-8', 'replace')))
        return status, body, resp.info()

    def _json_request(self, source_url, path, admin=False):
        _status, body, _headers = self._request(
            source_url, path, admin=admin)
        try:
            value = json.loads(body.decode('utf-8'))
        except (TypeError, ValueError, UnicodeDecodeError) as err:
            raise RingManagerSyncError(
                'GET %s returned invalid JSON: %s' % (path, err))
        if not isinstance(value, dict):
            raise RingManagerSyncError(
                'GET %s returned JSON %s, not an object' % (
                    path, type(value).__name__))
        return value

    def _state_path(self, *parts):
        return os.path.join(self.state_dir, *parts)

    def _artifact_path(self, *parts):
        return os.path.join(self.artifact_dir, *parts)

    def _builder_path(self, file_name):
        file_name = validate_path_component(file_name, 'builder file name')
        root = os.path.realpath(self.builder_dir)
        path = os.path.realpath(os.path.join(root, file_name))
        try:
            common_path = os.path.commonpath([root, path])
        except ValueError:
            common_path = None
        if common_path != root:
            raise RingManagerSyncError(
                'builder file name escapes ring_builder_dir: %s' % file_name)
        return path

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
                fsync_dir(parent)

    def _write_file_atomic(self, path, body):
        temp_path = None
        try:
            self._mkdirs(path)
            temp_path = '%s.tmp-%s' % (path, uuid.uuid4().hex)
            with open(temp_path, 'wb') as fp:
                fp.write(body)
                fp.flush()
                fsync(fp.fileno())
            os.rename(temp_path, path)
            temp_path = None
            directory = os.path.dirname(path)
            if directory:
                fsync_dir(directory)
        except RingManagerSyncLocalError:
            raise
        except Exception as err:
            raise RingManagerSyncLocalError(
                'local ring-manager sync write failed for %s: %s' %
                (path, err))
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except OSError as err:
                    if err.errno != errno.ENOENT:
                        raise RingManagerSyncLocalError(
                            'local ring-manager sync cleanup failed for %s: '
                            '%s' % (temp_path, err))

    def _json_body(self, value):
        return json.dumps(value, sort_keys=True, indent=2).encode(
            'ascii') + b'\n'

    def _write_json_atomic(self, path, value):
        self._write_file_atomic(path, self._json_body(value))
        self.state_change_hook.run('write', path)

    def _stage_json_write(self, state_writes, path, value):
        state_writes.append({
            'path': path,
            'body': self._json_body(value),
        })

    def _backup_existing_path(self, path):
        if not os.path.exists(path):
            return None
        backup_path = '%s.sync-backup-%s' % (path, uuid.uuid4().hex)
        try:
            os.link(path, backup_path)
            directory = os.path.dirname(path)
            if directory:
                fsync_dir(directory)
        except OSError as err:
            raise RingManagerSyncLocalError(
                'local ring-manager sync backup failed for %s: %s' %
                (path, err))
        return backup_path

    def _sync_journal_path(self):
        return self._state_path(RING_MANAGER_SYNC_JOURNAL)

    def _default_sync_transaction_stats(self):
        return {
            'pending': False,
            'recovered': False,
            'action': 'none',
            'committed': False,
            'entries': 0,
            'staged_builders': 0,
        }

    def _sync_transaction_summary(self, journal):
        committed = bool(journal.get('committed'))
        return {
            'pending': True,
            'recovered': True,
            'action': 'cleanup' if committed else 'rollback',
            'committed': committed,
            'entries': len(journal.get('entries', [])),
            'staged_builders': len(journal.get('staged_builders', [])),
        }

    def _failed_sync_transaction_summary(self):
        return {
            'pending': os.path.exists(self._sync_journal_path()),
            'recovered': False,
            'action': 'failed',
            'committed': False,
            'entries': 0,
            'staged_builders': 0,
            'recovery_failed': True,
        }

    def _record_sync_transaction_metrics(self, sync_transaction):
        if not sync_transaction or not sync_transaction.get('pending'):
            return
        stats_increment(self.logger, 'sync.transaction.pending')
        if sync_transaction.get('recovery_failed'):
            stats_increment(self.logger, 'sync.transaction.recovery_failures')
            return
        if sync_transaction.get('recovered'):
            stats_increment(self.logger, 'sync.transaction.recoveries')
        if sync_transaction.get('committed'):
            stats_increment(
                self.logger, 'sync.transaction.committed_cleanups')
        else:
            stats_increment(self.logger, 'sync.transaction.rollbacks')

    def _metric_suffix(self, value):
        return re.sub(r'[^A-Za-z0-9_.-]+', '_', str(value))

    def _operator_attention_status(self, success=True, source_errors=None,
                                   sync_transaction=None):
        source_errors = list(source_errors or [])
        sync_transaction = (
            sync_transaction or self._default_sync_transaction_stats())
        reasons = []
        if not success:
            reasons.append('sync_failed')
        if source_errors:
            reasons.append('source_errors')
        if sync_transaction.get('recovery_failed'):
            reasons.append('sync_transaction_recovery_failed')
        elif sync_transaction.get('pending') and not \
                sync_transaction.get('recovered'):
            reasons.append('sync_transaction_pending')
        return {
            'needed': bool(reasons),
            'reasons': reasons,
            'source_errors': len(source_errors),
            'sync_transaction': {
                'pending': bool(sync_transaction.get('pending')),
                'recovery_failed': bool(
                    sync_transaction.get('recovery_failed')),
            },
        }

    def _emit_operator_attention_metrics(self, attention):
        if not attention.get('needed'):
            return
        stats_increment(self.logger, 'sync.operator_attention')
        for reason in attention.get('reasons') or []:
            stats_increment(
                self.logger,
                'sync.operator_attention.%s' % self._metric_suffix(reason))

    def _write_sync_journal(self, journal):
        self._write_file_atomic(
            self._sync_journal_path(), self._json_body(journal))

    def _remove_sync_journal(self):
        path = self._sync_journal_path()
        try:
            os.unlink(path)
            directory = os.path.dirname(path)
            if directory:
                fsync_dir(directory)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise RingManagerSyncLocalError(
                    'local ring-manager sync journal cleanup failed for %s: '
                    '%s' % (path, err))

    def _read_sync_journal(self):
        path = self._sync_journal_path()
        journal = self._read_json(path, None)
        if journal is None:
            return None
        if not isinstance(journal, dict):
            raise RingManagerSyncLocalError(
                'local ring-manager sync journal must be an object')
        entries = journal.get('entries', [])
        staged_builders = journal.get('staged_builders', [])
        if not isinstance(entries, list) or not isinstance(
                staged_builders, list):
            raise RingManagerSyncLocalError(
                'local ring-manager sync journal has invalid contents')
        return journal

    def _cleanup_staged_builder_paths(self, paths):
        errors = []
        for path in paths:
            if not path:
                continue
            try:
                os.unlink(path)
                directory = os.path.dirname(path)
                if directory:
                    fsync_dir(directory)
            except OSError as err:
                if err.errno != errno.ENOENT:
                    errors.append('%s: %s' % (path, err))
        if errors:
            raise RingManagerSyncLocalError(
                'local ring-manager builder sync cleanup failed: %s' %
                '; '.join(errors))

    def _rollback_journal_entry(self, entry):
        path = entry['path']
        backup_path = entry.get('backup_path')
        if backup_path:
            try:
                os.rename(backup_path, path)
            except OSError as err:
                if err.errno != errno.ENOENT:
                    raise
            else:
                directory = os.path.dirname(path)
                if directory:
                    fsync_dir(directory)
            return
        try:
            os.unlink(path)
            directory = os.path.dirname(path)
            if directory:
                fsync_dir(directory)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise

    def _rollback_sync_journal(self, journal):
        while journal.get('entries'):
            entry = journal['entries'][-1]
            try:
                self._rollback_journal_entry(entry)
            except OSError as err:
                raise RingManagerSyncLocalError(
                    'local ring-manager sync rollback failed for %s: %s' %
                    (entry.get('path'), err))
            journal['entries'].pop()
            self._write_sync_journal(journal)
        self._cleanup_staged_builder_paths(journal.get('staged_builders', []))
        journal['staged_builders'] = []
        self._write_sync_journal(journal)

    def _recover_sync_transaction(self):
        journal = self._read_sync_journal()
        if journal is None:
            return self._default_sync_transaction_stats()
        summary = self._sync_transaction_summary(journal)
        if journal.get('committed'):
            self._cleanup_backups(journal.get('entries', []))
            self._cleanup_staged_builder_paths(
                journal.get('staged_builders', []))
            self._remove_sync_journal()
            return summary
        self._rollback_sync_journal(journal)
        self._remove_sync_journal()
        return summary

    def _record_journal_entry(self, journal, entry):
        journal['entries'].append(entry)
        self._write_sync_journal(journal)

    def _commit_path_body(self, touched, journal, path, body, kind='state'):
        backup_path = self._backup_existing_path(path)
        entry = {
            'kind': kind,
            'path': path,
            'backup_path': backup_path,
        }
        self._record_journal_entry(journal, entry)
        touched.append(entry)
        self._write_file_atomic(path, body)

    def _cleanup_backups(self, touched):
        for entry in touched:
            backup_path = entry.get('backup_path')
            if not backup_path:
                continue
            try:
                os.unlink(backup_path)
                directory = os.path.dirname(backup_path)
                if directory:
                    fsync_dir(directory)
            except OSError as err:
                if err.errno != errno.ENOENT:
                    self.logger.warning(
                        'Unable to remove ring-manager sync backup %s: %s',
                        backup_path, err)

    def _cleanup_staged_builders(self, staged_builders):
        errors = []
        for builder in staged_builders:
            temp_path = builder.get('temp_path')
            if not temp_path:
                continue
            try:
                os.unlink(temp_path)
                builder['temp_path'] = None
                directory = os.path.dirname(temp_path)
                if directory:
                    fsync_dir(directory)
            except OSError as err:
                if err.errno != errno.ENOENT:
                    errors.append('%s: %s' % (temp_path, err))
        if errors:
            raise RingManagerSyncLocalError(
                'local ring-manager builder sync cleanup failed: %s' %
                '; '.join(errors))

    def _commit_staged_builder(self, touched, journal, builder):
        path = builder['path']
        temp_path = builder['temp_path']
        backup_path = self._backup_existing_path(path)
        entry = {
            'kind': 'builder',
            'path': path,
            'backup_path': backup_path,
        }
        self._record_journal_entry(journal, entry)
        touched.append(entry)
        try:
            os.rename(temp_path, path)
            builder['temp_path'] = None
            directory = os.path.dirname(path)
            if directory:
                fsync_dir(directory)
        except OSError as err:
            raise RingManagerSyncLocalError(
                'local ring-manager builder sync install failed for %s: %s' %
                (path, err))

    def _commit_sync_transaction(self, state_writes, staged_builders,
                                 index_write):
        journal = {
            'committed': False,
            'entries': [],
            'staged_builders': [
                builder['temp_path'] for builder in staged_builders
                if builder.get('temp_path')],
        }
        self._write_sync_journal(journal)
        touched = []
        state_paths = []
        try:
            for write in state_writes:
                self._commit_path_body(
                    touched, journal, write['path'], write['body'])
                state_paths.append(write['path'])
            for builder in staged_builders:
                self._commit_staged_builder(touched, journal, builder)
            self._commit_path_body(
                touched, journal, index_write['path'], index_write['body'])
            state_paths.append(index_write['path'])
            journal['committed'] = True
            self._write_sync_journal(journal)
        except Exception:
            rollback_err = None
            try:
                self._rollback_sync_journal(journal)
            except RingManagerSyncLocalError as err:
                rollback_err = err
            try:
                self._cleanup_staged_builders(staged_builders)
            except RingManagerSyncLocalError as err:
                if rollback_err is None:
                    rollback_err = err
            if rollback_err is not None:
                raise rollback_err
            self._remove_sync_journal()
            raise
        self._cleanup_backups(touched)
        self._remove_sync_journal()
        for path in state_paths:
            self.state_change_hook.run('write', path)

    def _stage_builder_file(self, path, body, ring_id):
        temp_path = None
        try:
            self._mkdirs(path)
            temp_path = '%s.sync-%s' % (path, uuid.uuid4().hex)
            with open(temp_path, 'wb') as fp:
                fp.write(body)
                fp.flush()
                fsync(fp.fileno())
            directory = os.path.dirname(temp_path)
            if directory:
                fsync_dir(directory)
            try:
                builder = RingBuilder.load(temp_path)
            except Exception as err:
                raise RingManagerSyncError(
                    'downloaded builder for ring %s could not be loaded: %s' %
                    (ring_id, err))
            record = {
                'path': path,
                'temp_path': temp_path,
                'ring_id': ring_id,
                'builder': builder,
            }
            temp_path = None
            return record
        except RingManagerSyncError:
            raise
        except Exception as err:
            raise RingManagerSyncLocalError(
                'local ring-manager builder sync stage failed for %s: %s' %
                (path, err))
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except OSError as err:
                    if err.errno != errno.ENOENT:
                        raise RingManagerSyncLocalError(
                            'local ring-manager builder sync cleanup failed '
                            'for %s: %s' % (temp_path, err))

    def _read_json(self, path, default):
        try:
            with open(path, 'r') as fp:
                return json.load(fp)
        except IOError as err:
            if err.errno == errno.ENOENT:
                return default
            raise RingManagerSyncLocalError(
                'local ring-manager sync read failed for %s: %s' %
                (path, err))
        except ValueError as err:
            raise RingManagerSyncLocalError(
                'local ring-manager sync JSON read failed for %s: %s' %
                (path, err))

    def _local_artifact_relpath(self, version, file_info):
        return os.path.join(
            self._safe_id(version), self._safe_id(file_info['name']))

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

    def _verified_artifact_etag(self, path, file_info):
        try:
            with open(path, 'rb') as fp:
                body = fp.read()
        except IOError:
            return None
        if not self._body_matches(body, file_info):
            return None
        return md5(body, usedforsecurity=False).hexdigest()

    def _verify_download(self, body, file_info, url):
        expected_bytes = file_info.get('bytes')
        if expected_bytes is not None and len(body) != expected_bytes:
            raise RingManagerSyncError(
                '%s returned %d bytes, expected %d' % (
                    url, len(body), expected_bytes))
        expected_sha256 = file_info.get('sha256')
        if expected_sha256 is not None:
            actual = hashlib.sha256(body).hexdigest()
            if actual != expected_sha256:
                raise RingManagerSyncError(
                    '%s sha256 mismatch: got %s, expected %s' % (
                        url, actual, expected_sha256))

    def _response_header(self, headers, name):
        if not headers:
            return None
        for key, value in headers.items():
            if key.lower() == name.lower():
                return value
        return None

    def _default_builder_name(self, ring):
        ring_id = ring.get('id')
        ring_type = ring.get('ring_type') or ring.get('type')
        if ring_type == 'account' or ring_id == 'account':
            return 'account.builder'
        if ring_type == 'container' or ring_id == 'container':
            return 'container.builder'
        policy_index = ring.get('storage_policy_index')
        if policy_index in (None, '', 0, '0'):
            return 'object.builder'
        return 'object-%s.builder' % policy_index

    def _local_builder_file_name(self, ring):
        path = None
        files = ring.get('builder_files')
        if isinstance(files, list) and len(files) == 1 and files[0]:
            path = files[0]
        elif ring.get('builder_path'):
            path = ring['builder_path']
        if path:
            file_name = os.path.basename(str(path))
            if file_name:
                return validate_path_component(
                    file_name, 'builder file name')
        return validate_path_component(
            self._default_builder_name(ring), 'builder file name')

    def _ring_is_disabled(self, ring):
        return config_true_value(str(ring.get('disabled', False)))

    def _stage_local_builder_names(self, rings):
        seen = {}
        for ring in rings:
            file_name = self._local_builder_file_name(ring)
            other_ring_id = seen.get(file_name)
            if other_ring_id is not None:
                raise RingManagerSyncError(
                    'rings %s and %s map to the same local builder file %s' %
                    (other_ring_id, ring['id'], file_name))
            seen[file_name] = ring['id']
            ring.pop('builder_path', None)
            ring['builder_files'] = [file_name]

    def _file_url(self, file_info, default_url):
        url = file_info.get('url') or default_url
        try:
            return validate_relative_api_url(url, 'manifest artifact URL')
        except ValueError as err:
            raise RingManagerSyncError(str(err))

    def _download_file(self, source_url, file_info, default_url, local_path):
        headers = {}
        if file_info.get('sha256'):
            etag = self._verified_artifact_etag(local_path, file_info)
            if etag:
                headers['If-None-Match'] = etag
        url = self._file_url(file_info, default_url)
        status, body, _headers = self._request(
            source_url, url, headers=headers)
        if status == 304:
            return 'unchanged'
        self._verify_download(body, file_info, url)
        stats_increment(self.logger, 'sync.bytes_downloaded', len(body))
        self._write_file_atomic(local_path, body)
        return 'downloaded'

    def _localized_file_info(self, version, file_info):
        local_info = dict(file_info)
        local_info.pop('url', None)
        local_info['path'] = self._local_artifact_relpath(version, file_info)
        return local_info

    def _sync_manifest_files(self, source_url, manifest,
                             field_name='release manifest'):
        version = str(manifest.get('version', ''))
        if not version:
            raise RingManagerSyncError('%s has no version' % field_name)
        files = manifest.get('files', [])
        if not isinstance(files, list):
            raise RingManagerSyncError(
                '%s files must be a list' % field_name)

        downloaded = unchanged = 0
        localized_files = []
        for file_info in files:
            if not isinstance(file_info, dict) or not file_info.get('name'):
                raise RingManagerSyncError(
                    '%s files must contain objects with name' % field_name)
            relpath = self._local_artifact_relpath(version, file_info)
            local_path = self._artifact_path(relpath)
            default_url = '/api/v1/rings/releases/%s/files/%s' % (
                quote(version, safe=''),
                quote(file_info['name'], safe=''))
            result = self._download_file(
                source_url, file_info, default_url, local_path)
            if result == 'downloaded':
                downloaded += 1
            else:
                unchanged += 1
            localized_files.append(self._localized_file_info(
                version, file_info))

        local_manifest = dict(manifest)
        local_manifest.pop('latest', None)
        local_manifest.pop('desired', None)
        local_manifest.pop('resource_uri', None)
        local_manifest['version'] = version
        local_manifest['files'] = localized_files
        return version, local_manifest, downloaded, unchanged

    def _sync_rings(self, source_url):
        collection = self._json_request(source_url, '/api/v1/rings/')
        objects = collection.get('objects', [])
        if not isinstance(objects, list):
            raise RingManagerSyncError(
                'rings collection objects must be a list')
        local_rings = []
        for ring in objects:
            if not isinstance(ring, dict) or ring.get('id') in (None, ''):
                raise RingManagerSyncError(
                    'rings collection objects must contain ids')
            local_ring = dict(ring)
            local_ring.pop('resource_uri', None)
            local_rings.append(local_ring)
        if self.sync_builder_files:
            self._stage_local_builder_names(local_rings)
        return len(local_rings), local_rings

    def _stage_rings(self, state_writes, rings):
        synced = 0
        for local_ring in rings:
            self._stage_json_write(
                state_writes,
                self._state_path(
                    'rings', '%s.json' % self._safe_id(local_ring['id'])),
                local_ring)
            synced += 1
        return synced

    def _verify_local_builder(self, path, file_info):
        return self._verified_local_builder_etag(path, file_info) is not None

    def _verified_local_builder_etag(self, path, file_info):
        try:
            with open(path, 'rb') as fp:
                body = fp.read()
        except IOError:
            return None
        try:
            RingBuilder.load(path)
        except Exception:
            return None
        if not self._body_matches(body, file_info):
            return None
        expected_version = file_info.get('builder_version')
        if expected_version not in (None, ''):
            try:
                builder = RingBuilder.load(path)
            except Exception:
                return None
            if str(builder.version) != str(expected_version):
                return None
        return md5(body, usedforsecurity=False).hexdigest()

    def _download_builder_file(self, source_url, ring, metadata, local_path,
                               staged_builders):
        ring_id = str(ring['id'])
        file_info = metadata.get('file')
        if not isinstance(file_info, dict):
            raise RingManagerSyncError(
                'ring %s builder metadata file must be an object' % ring_id)
        expected_url = '/api/v1/rings/%s/builder/file/' % quote(
            ring_id, safe='')
        url = self._file_url(file_info, expected_url)
        if file_info.get('url') != expected_url or url != expected_url:
            raise RingManagerSyncError(
                'ring %s builder metadata file URL must be %s' %
                (ring_id, expected_url))
        missing = [key for key in ('bytes', 'sha256')
                   if file_info.get(key) in (None, '')]
        if metadata.get('builder_version') in (None, ''):
            missing.append('builder_version')
        if missing:
            raise RingManagerSyncError(
                'ring %s builder metadata missing required field(s): %s' %
                (ring_id, ', '.join(missing)))
        file_info = dict(file_info)
        file_info['builder_version'] = metadata['builder_version']
        headers = {}
        if file_info.get('sha256'):
            etag = self._verified_local_builder_etag(local_path, file_info)
            if etag:
                headers['If-None-Match'] = etag
        status, body, response_headers = self._request(
            source_url, url, headers=headers, admin=True)
        if status == 304:
            return 'unchanged'
        expected_sha256 = file_info.get('sha256') or self._response_header(
            response_headers, 'X-Checksum-Sha256')
        if expected_sha256 is not None:
            file_info = dict(file_info)
            file_info['sha256'] = expected_sha256
        self._verify_download(body, file_info, url)
        staged_builder = self._stage_builder_file(local_path, body, ring_id)
        builder = staged_builder['builder']
        expected_version = metadata.get('builder_version')
        if expected_version not in (None, '') and \
                str(builder.version) != str(expected_version):
            self._cleanup_staged_builders([staged_builder])
            raise RingManagerSyncError(
                'downloaded builder for ring %s version %s did not match '
                'metadata version %s' % (
                    ring_id, builder.version, expected_version))
        stats_increment(self.logger, 'sync.bytes_downloaded', len(body))
        staged_builders.append(staged_builder)
        return 'downloaded'

    def _sync_builder_files(self, source_url, rings, staged_builders):
        stats = {
            'builder_files_synced': 0,
            'builder_files_downloaded': 0,
            'builder_files_unchanged': 0,
            'builder_files_skipped_disabled': 0,
        }
        if not self.sync_builder_files:
            return stats
        for ring in rings:
            if self._ring_is_disabled(ring):
                stats['builder_files_skipped_disabled'] += 1
                continue
            ring_id = str(ring['id'])
            metadata = self._json_request(
                source_url, '/api/v1/rings/%s/builder/' %
                quote(ring_id, safe=''), admin=True)
            if str(metadata.get('ring_id')) != ring_id:
                raise RingManagerSyncError(
                    'ring %s builder metadata returned ring_id %r' % (
                        ring_id, metadata.get('ring_id')))
            file_name = self._local_builder_file_name(ring)
            local_path = self._builder_path(file_name)
            result = self._download_builder_file(
                source_url, ring, metadata, local_path, staged_builders)
            stats['builder_files_synced'] += 1
            if result == 'downloaded':
                stats['builder_files_downloaded'] += 1
            else:
                stats['builder_files_unchanged'] += 1
        return stats

    def _sync_ring_artifact_versions(self, source_url, manifest_version,
                                     manifest):
        synced = 0
        downloaded = unchanged = 0
        ring_versions = []
        for ring in manifest.get('rings', []):
            if not isinstance(ring, dict):
                continue
            ring_id = ring.get('ring_id')
            version_id = ring.get('swift_ring_version') or ring.get('version')
            if ring_id in (None, '') or version_id in (None, ''):
                continue
            version = self._json_request(
                source_url,
                '/api/v1/rings/%s/versions/%s/' % (
                    quote(str(ring_id), safe=''),
                    quote(str(version_id), safe='')))
            files = version.get('files', [])
            if not isinstance(files, list):
                raise RingManagerSyncError(
                    'ring %s version %s files must be a list' % (
                        ring_id, version_id))
            localized_files = []
            for file_info in files:
                if (not isinstance(file_info, dict) or
                        not file_info.get('name')):
                    raise RingManagerSyncError(
                        'ring %s version %s files must contain objects with '
                        'name' % (ring_id, version_id))
                relpath = self._local_artifact_relpath(
                    manifest_version, file_info)
                local_path = self._artifact_path(relpath)
                default_url = '/api/v1/rings/%s/versions/%s/files/%s' % (
                    quote(str(ring_id), safe=''),
                    quote(str(version_id), safe=''),
                    quote(file_info['name'], safe=''))
                result = self._download_file(
                    source_url, file_info, default_url, local_path)
                if result == 'downloaded':
                    downloaded += 1
                else:
                    unchanged += 1
                localized_files.append(self._localized_file_info(
                    manifest_version, file_info))

            local_version = dict(version)
            local_version.pop('latest', None)
            local_version.pop('resource_uri', None)
            local_version['files'] = localized_files
            ring_versions.append({
                'ring_id': ring_id,
                'version_id': version_id,
                'body': local_version,
            })
            synced += 1
        return synced, downloaded, unchanged, ring_versions

    def _stage_ring_artifact_versions(self, state_writes, ring_versions):
        for version in ring_versions:
            self._stage_json_write(
                state_writes,
                self._state_path(
                    'ring-versions', self._safe_id(version['ring_id']),
                    '%s.json' % self._safe_id(version['version_id'])),
                version['body'])

    def _index_write(self, source_url, latest_version, desired_version,
                     desired_info, synced_at):
        index_path = self._state_path('index.json')
        index = self._read_json(index_path, {})
        if not isinstance(index, dict):
            raise RingManagerSyncLocalError(
                'local index.json must be an object')
        index['latest_ring_version'] = latest_version
        if desired_version is None:
            for key in ('desired_ring_version', 'desired_updated_at',
                        'desired_reason'):
                index.pop(key, None)
        else:
            index['desired_ring_version'] = desired_version
            if desired_info.get('desired_updated_at') is None:
                index.pop('desired_updated_at', None)
            else:
                index['desired_updated_at'] = \
                    desired_info['desired_updated_at']
            if desired_info.get('desired_reason') is None:
                index.pop('desired_reason', None)
            else:
                index['desired_reason'] = desired_info['desired_reason']
        index['ring_manager_sync'] = {
            'source': source_url,
            'latest_ring_version': latest_version,
            'desired_ring_version': desired_version,
            'synced_at': synced_at,
        }
        return {
            'path': index_path,
            'body': self._json_body(index),
        }

    def _dump_recon(self, sync_stats):
        if not self.recon_dump:
            return
        try:
            mkdirs(os.path.dirname(self.recon_cache))
        except Exception as err:
            self.logger.exception('Exception creating recon cache path: %s' %
                                  err)
            return
        dump_recon_cache({'ring_manager_sync': sync_stats},
                         self.recon_cache, self.logger)

    def _sync_stats(self, source_url, started_at, ended_at, synced_at, result,
                    source_errors=None, sync_transaction=None):
        stats = dict(result)
        stats['sync_transaction'] = (
            sync_transaction or self._default_sync_transaction_stats())
        stats.update({
            'source': source_url,
            'success': True,
            'sync_time': float(ended_at) - float(started_at),
            'last_attempt': ended_at.internal,
            'last_attempted_at': synced_at,
            'last_success': ended_at.internal,
            'last_synced_at': synced_at,
            'error': {},
        })
        if len(self.source_urls) > 1:
            stats['sources'] = list(self.source_urls)
        if source_errors:
            stats['source_errors'] = list(source_errors)
        stats['operator_attention'] = self._operator_attention_status(
            success=True, source_errors=source_errors,
            sync_transaction=stats['sync_transaction'])
        return stats

    def _failure_stats(self, started_at, err, source_url=None,
                       source_errors=None, sync_transaction=None):
        ended_at = self._timestamp()
        attempted_at = ended_at.internal
        stats = {
            'source': source_url or self.source_url,
            'success': False,
            'sync_time': float(ended_at) - float(started_at),
            'last_attempt': ended_at.internal,
            'last_attempted_at': attempted_at,
            'error': str(err),
            'sync_transaction': (
                sync_transaction or self._default_sync_transaction_stats()),
        }
        if len(self.source_urls) > 1:
            stats['sources'] = list(self.source_urls)
        if source_errors:
            stats['source_errors'] = list(source_errors)
        stats['operator_attention'] = self._operator_attention_status(
            success=False, source_errors=source_errors,
            sync_transaction=stats['sync_transaction'])
        return stats

    def _source_status(self, source_url):
        return self._json_request(source_url,
                                  '/api/v1/ring_manager/status/')

    def _validate_source(self, source_url):
        status = self._source_status(source_url)
        mode = status.get('mode')
        if 'desired_ring_version' not in status:
            raise RingManagerSyncError(
                'source %s status did not include desired_ring_version' %
                source_url)
        desired_version = status.get('desired_ring_version')
        if desired_version not in (None, ''):
            desired_version = str(desired_version)
        else:
            desired_version = None
        if mode == 'primary':
            return {
                'mode': mode,
                'latest_ring_version': status.get('latest_ring_version'),
                'desired_ring_version': desired_version,
                'sync_timestamp': None,
            }
        if mode in ('readonly', 'standby'):
            sync_status = status.get('ring_manager_sync')
            if not isinstance(sync_status, dict):
                raise RingManagerSyncError(
                    'source %s is %s but did not include ring_manager_sync '
                    'status' % (source_url, mode))
            blockers = []
            if sync_status.get('can_serve_published_reads') is not True:
                blockers.append('cannot_serve_published_reads')
            if sync_status.get('synced') is not True:
                blockers.append('not_synced')
            if sync_status.get('fresh') is not True:
                blockers.append('not_fresh')
            if sync_status.get('stale') is True:
                blockers.append('stale')
            if sync_status.get('latest_matches_local') is not True:
                blockers.append('latest_mismatch_or_unknown')
            if sync_status.get('desired_matches_local') is not True:
                blockers.append('desired_mismatch_or_unknown')
            if 'desired_ring_version' not in sync_status:
                blockers.append('missing_desired_ring_version')
            else:
                sync_desired = sync_status.get('desired_ring_version')
                if sync_desired in (None, ''):
                    sync_desired = None
                else:
                    sync_desired = str(sync_desired)
                if sync_desired != desired_version:
                    blockers.append('desired_status_mismatch')
            sync_latest = sync_status.get('latest_ring_version')
            if sync_latest in (None, ''):
                blockers.append('missing_latest_ring_version')
            synced_at = sync_status.get('last_synced_at') or \
                sync_status.get('synced_at')
            if synced_at in (None, ''):
                blockers.append('missing_last_synced_at')
            else:
                try:
                    synced_at = NormalTimestamp(synced_at).internal
                except (TypeError, ValueError, AssertionError):
                    blockers.append('invalid_last_synced_at')
            if blockers:
                reasons = sync_status.get('reasons')
                if isinstance(reasons, list) and reasons:
                    blockers.extend(str(reason) for reason in reasons)
                raise RingManagerSyncError(
                    'source %s is %s but cannot serve fresh published reads: '
                    '%s' % (source_url, mode, ', '.join(blockers)))
            return {
                'mode': mode,
                'latest_ring_version': str(sync_latest),
                'desired_ring_version': desired_version,
                'sync_timestamp': synced_at,
            }
        raise RingManagerSyncError(
            'source %s returned unsupported ring-manager mode %r' %
            (source_url, mode))

    def _sync_release(self, source_url, selector, expected_version):
        manifest = self._json_request(
            source_url, '/api/v1/rings/releases/%s/manifest/' % selector)
        version, local_manifest, downloaded, unchanged = \
            self._sync_manifest_files(
                source_url, manifest, field_name='%s manifest' % selector)
        if str(expected_version) != version:
            raise RingManagerSyncError(
                'source %s status %s version %s does not match %s manifest '
                'version %s' % (
                    source_url, selector, expected_version, selector,
                    version))
        per_ring, per_ring_downloaded, per_ring_unchanged, ring_versions = \
            self._sync_ring_artifact_versions(
                source_url, version, manifest)
        return {
            'version': version,
            'manifest': manifest,
            'local_manifest': local_manifest,
            'manifest_files_downloaded': downloaded,
            'manifest_files_unchanged': unchanged,
            'ring_versions_synced': per_ring,
            'ring_version_files_downloaded': per_ring_downloaded,
            'ring_version_files_unchanged': per_ring_unchanged,
            'ring_versions': ring_versions,
        }

    def _sync_from_source(self, source_url):
        staged_builders = []
        try:
            source_status = self._validate_source(source_url)
            source_latest = source_status.get('latest_ring_version')
            if source_latest in (None, ''):
                raise RingManagerSyncError(
                    'source %s status has no latest ring version' %
                    source_url)
            source_latest = str(source_latest)
            latest = self._sync_release(
                source_url, 'latest', source_latest)

            desired_version = source_status.get('desired_ring_version')
            desired_info = {}
            desired = None
            if desired_version is not None:
                desired_info = self._json_request(
                    source_url, '/api/v1/rings/releases/desired/')
                if str(desired_info.get('version')) != desired_version or \
                        desired_info.get('desired') is not True:
                    raise RingManagerSyncError(
                        'source %s desired release does not match status '
                        'version %s' % (source_url, desired_version))
                if desired_version == source_latest:
                    desired = latest
                else:
                    desired = self._sync_release(
                        source_url, 'desired', desired_version)

            _rings, ring_objects = self._sync_rings(source_url)
            builder_stats = self._sync_builder_files(
                source_url, ring_objects, staged_builders)

            state_writes = []
            rings = self._stage_rings(state_writes, ring_objects)
            self._stage_ring_artifact_versions(
                state_writes, latest['ring_versions'])
            if desired is not None and desired is not latest:
                self._stage_ring_artifact_versions(
                    state_writes, desired['ring_versions'])
            self._stage_json_write(
                state_writes,
                self._state_path(
                    'releases', self._safe_id(source_latest),
                    'manifest.json'),
                latest['local_manifest'])
            if desired is not None and desired is not latest:
                self._stage_json_write(
                    state_writes,
                    self._state_path(
                        'releases', self._safe_id(desired_version),
                        'manifest.json'),
                    desired['local_manifest'])
            commit_at = self._timestamp()
            synced_at = source_status.get('sync_timestamp') or \
                commit_at.internal
            index_write = self._index_write(
                source_url, source_latest, desired_version, desired_info,
                synced_at)
            self._commit_sync_transaction(
                state_writes, staged_builders, index_write)
        except Exception:
            self._cleanup_staged_builders(staged_builders)
            raise

        ended_at = self._timestamp()

        release_results = [latest]
        if desired is not None and desired is not latest:
            release_results.append(desired)
        return {
            'latest_ring_version': source_latest,
            'desired_ring_version': desired_version,
            'manifest_files_downloaded': sum(
                result['manifest_files_downloaded']
                for result in release_results),
            'manifest_files_unchanged': sum(
                result['manifest_files_unchanged']
                for result in release_results),
            'rings_synced': rings,
            'ring_versions_synced': sum(
                result['ring_versions_synced']
                for result in release_results),
            'ring_version_files_downloaded': sum(
                result['ring_version_files_downloaded']
                for result in release_results),
            'ring_version_files_unchanged': sum(
                result['ring_version_files_unchanged']
                for result in release_results),
            'builder_files_synced': builder_stats['builder_files_synced'],
            'builder_files_downloaded': builder_stats[
                'builder_files_downloaded'],
            'builder_files_unchanged': builder_stats[
                'builder_files_unchanged'],
            'builder_files_skipped_disabled': builder_stats[
                'builder_files_skipped_disabled'],
        }, synced_at, ended_at

    def _record_failure(self, started_at, err, source_url=None,
                        source_errors=None, sync_transaction=None):
        ended_at = self._timestamp()
        stats_increment(self.logger, 'sync.failures')
        stats_timing(
            self.logger, 'sync.timing',
            float(ended_at) - float(started_at))
        stats = self._failure_stats(
            started_at, err, source_url=source_url,
            source_errors=source_errors, sync_transaction=sync_transaction)
        self._emit_operator_attention_metrics(stats['operator_attention'])
        self._dump_recon(stats)

    def sync(self):
        started_at = self._timestamp()
        stats_increment(self.logger, 'sync.attempts')
        try:
            with lock_path(
                    self.state_dir, timeout=self.sync_lock_timeout,
                    name='ring-manager-sync'):
                try:
                    sync_transaction = self._recover_sync_transaction()
                except RingManagerSyncLocalError as err:
                    sync_transaction = \
                        self._failed_sync_transaction_summary()
                    self._record_sync_transaction_metrics(sync_transaction)
                    self._record_failure(
                        started_at, err,
                        sync_transaction=sync_transaction)
                    raise
                self._record_sync_transaction_metrics(sync_transaction)
                return self._sync_locked(started_at, sync_transaction)
        except swift_exceptions.LockTimeout as err:
            local_err = RingManagerSyncLocalError(
                'local ring-manager sync lock failed: %s' % err)
            self._record_failure(started_at, local_err)
            raise local_err

    def _sync_locked(self, started_at, sync_transaction):
        source_errors = []
        last_source = None
        last_error = None
        for source_url in self._source_urls():
            last_source = source_url
            try:
                result, synced_at, ended_at = self._sync_from_source(
                    source_url)
            except RingManagerSyncLocalError as err:
                self._record_failure(
                    started_at, err, source_url=source_url,
                    source_errors=source_errors,
                    sync_transaction=sync_transaction)
                raise
            except Exception as err:
                last_error = err
                stats_increment(self.logger, 'sync.source.failures')
                self.logger.warning(
                    'Unable to sync ring-manager state from %s: %s',
                    source_url, err)
                source_errors.append({
                    'source': source_url,
                    'error': str(err),
                })
                continue

            stats_increment(self.logger, 'sync.successes')
            stats_timing(
                self.logger, 'sync.timing',
                float(ended_at) - float(started_at))
            stats_increment(
                self.logger, 'sync.manifest_files.downloaded',
                result['manifest_files_downloaded'])
            stats_increment(
                self.logger, 'sync.manifest_files.unchanged',
                result['manifest_files_unchanged'])
            stats_increment(self.logger, 'sync.rings_synced',
                            result['rings_synced'])
            stats_increment(self.logger, 'sync.ring_versions_synced',
                            result['ring_versions_synced'])
            stats_increment(
                self.logger, 'sync.ring_version_files.downloaded',
                result['ring_version_files_downloaded'])
            stats_increment(
                self.logger, 'sync.ring_version_files.unchanged',
                result['ring_version_files_unchanged'])
            stats_increment(
                self.logger, 'sync.builder_files.synced',
                result['builder_files_synced'])
            stats_increment(
                self.logger, 'sync.builder_files.downloaded',
                result['builder_files_downloaded'])
            stats_increment(
                self.logger, 'sync.builder_files.unchanged',
                result['builder_files_unchanged'])
            stats_increment(
                self.logger, 'sync.builder_files.skipped_disabled',
                result['builder_files_skipped_disabled'])
            sync_stats = self._sync_stats(
                source_url, started_at, ended_at, synced_at, result,
                source_errors=source_errors,
                sync_transaction=sync_transaction)
            self._emit_operator_attention_metrics(
                sync_stats['operator_attention'])
            self._dump_recon(sync_stats)
            return result

        if len(self.source_urls) == 1 and last_error is not None:
            self._record_failure(
                started_at, last_error, source_url=last_source,
                source_errors=source_errors,
                sync_transaction=sync_transaction)
            raise last_error
        err = RingManagerSyncError(
            'all ring-manager sources failed: %s' % '; '.join(
                '%s: %s' % (item['source'], item['error'])
                for item in source_errors))
        self._record_failure(
            started_at, err, source_url=last_source,
            source_errors=source_errors, sync_transaction=sync_transaction)
        raise err


def _make_parser():
    parser = optparse.OptionParser(
        usage='%prog [SOURCE_URL ... | CONFIG] [options]',
        description='Sync published ring-manager state and artifacts from '
                    'SOURCE_URL into a local ring-manager state directory. '
                    'When CONFIG is provided, options are loaded from the '
                    '[ring-manager-sync] section.')
    parser.add_option(
        '--config', dest='conf_file',
        help='Config file with a [ring-manager-sync] section.')
    parser.add_option(
        '--ring-manager-state-dir', dest='state_dir',
        help='Local ring_manager_state_dir to write.')
    parser.add_option(
        '--ring-artifact-dir', dest='artifact_dir',
        help='Local ring_artifact_dir to write.')
    parser.add_option(
        '--ring-builder-dir', dest='builder_dir',
        help='Local ring_builder_dir to write builder files when '
             '--sync-builder-files is enabled. Default: %s' %
             DEFAULT_RING_BUILDER_DIR)
    parser.add_option(
        '--sync-builder-files', action='store_true',
        dest='sync_builder_files', default=None,
        help='Also sync enabled-ring Swift builder files. Requires admin '
             'credentials.')
    parser.add_option(
        '--no-sync-builder-files', action='store_false',
        dest='sync_builder_files',
        help='Do not sync Swift builder files.')
    parser.add_option(
        '--admin-key', dest='admin_key',
        help='Value for X-Ring-Manager-Admin-Key when fetching from source.')
    parser.add_option(
        '--admin-key-file', dest='admin_key_file',
        help='File containing X-Ring-Manager-Admin-Key value.')
    parser.add_option(
        '--read-key', dest='read_key',
        help='Value for X-Ring-Manager-Read-Key when fetching from source.')
    parser.add_option(
        '--read-key-file', dest='read_key_file',
        help='File containing X-Ring-Manager-Read-Key value.')
    parser.add_option(
        '--auth-token', dest='auth_token',
        help='Value for X-Auth-Token when fetching from source.')
    parser.add_option(
        '--read-auth-token', dest='read_auth_token',
        help='Read-only value for X-Auth-Token when fetching from source.')
    parser.add_option(
        '--timeout', dest='timeout', type='float',
        help='HTTP request timeout in seconds. Default: 30')
    parser.add_option(
        '--sync-lock-timeout', dest='sync_lock_timeout', type='float',
        help='Seconds to wait for the local ring-manager sync lock. '
             'Default: 30')
    parser.add_option(
        '--recon-cache-path', dest='recon_cache_path',
        help='Directory for ring-manager recon cache data. Default: %s' %
             DEFAULT_RECON_CACHE_PATH)
    parser.add_option(
        '--no-recon-dump', action='store_false', dest='recon_dump',
        default=None,
        help='Do not write ring-manager sync stats to recon cache.')
    parser.add_option(
        '--recon-dump', action='store_true', dest='recon_dump',
        help='Write ring-manager sync stats to recon cache.')
    parser.add_option(
        '--state-change-hook', dest='state_change_hook',
        help='Command to run after each ring-manager state JSON write.')
    parser.add_option(
        '--state-change-hook-timeout', dest='state_change_hook_timeout',
        type='float',
        help='Seconds to wait for --state-change-hook. Use 0 for no timeout. '
             'Default: %s' % DEFAULT_STATE_CHANGE_HOOK_TIMEOUT)
    parser.add_option(
        '--log-statsd-host', dest='log_statsd_host',
        help='StatsD host for sync metrics.')
    parser.add_option(
        '--log-statsd-port', dest='log_statsd_port', type='int',
        help='StatsD port for sync metrics. Default: 8125')
    parser.add_option(
        '--log-statsd-default-sample-rate',
        dest='log_statsd_default_sample_rate', type='float',
        help='Default StatsD sample rate. Default: 1.0')
    parser.add_option(
        '--log-statsd-sample-rate-factor',
        dest='log_statsd_sample_rate_factor', type='float',
        help='StatsD sample rate multiplier. Default: 1.0')
    parser.add_option(
        '--log-statsd-metric-prefix', dest='log_statsd_metric_prefix',
        help='Prefix to prepend to sync metric names.')
    parser.add_option(
        '-q', '--quiet', action='store_true', default=False,
        help='Do not print a successful sync summary.')
    return parser


def _is_url(value):
    parsed = urlparse(value)
    return parsed.scheme in ('http', 'https') and bool(parsed.netloc)


def _looks_like_path(value):
    return (os.path.sep in value or value.startswith('.') or
            value.endswith('.conf'))


def _normalize_source_urls(source_url):
    if source_url in (None, ''):
        return []
    values = source_url
    if isinstance(values, str):
        values = list_from_csv(values)
    urls = []
    for value in values:
        if isinstance(value, str):
            urls.extend(list_from_csv(value))
        else:
            urls.append(value)
    return [str(url).rstrip('/') for url in urls if url]


def _validate_source_urls(source_urls):
    urls = _normalize_source_urls(source_urls)
    if not urls:
        raise RingManagerSyncError('SOURCE_URL is required')
    for url in urls:
        if not _is_url(url):
            raise RingManagerSyncError(
                'source_url must be an http(s) URL: %s' % url)
    return urls


def _option_or_conf(options, option_name, conf, conf_names, default=None):
    value = getattr(options, option_name, None)
    if value not in (None, ''):
        return value
    for name in conf_names:
        value = conf.get(name)
        if value not in (None, ''):
            return value
    return default


def _option_pair_or_conf(options, option_names, conf, default=None):
    cli_values = [getattr(options, name, None) for name in option_names]
    if any(value not in (None, '') for value in cli_values):
        return cli_values
    return [
        conf.get(name) if conf.get(name) not in (None, '') else default
        for name in option_names
    ]


def _non_negative_float_option(value, name):
    try:
        return non_negative_float(value)
    except ValueError as err:
        raise RingManagerSyncError('%s: %s' % (name, err))


def _load_sync_conf(conf_file):
    try:
        return readconf(conf_file, SYNC_CONF_SECTION, log_name=USER_AGENT)
    except (IOError, ValueError) as err:
        raise RingManagerSyncError(str(err))


def _resolve_sync_conf(options, args):
    conf_file = options.conf_file
    source_args = list(args)
    if conf_file is None and len(source_args) == 1:
        arg = source_args[0]
        if not _is_url(arg):
            if os.path.exists(arg):
                conf_file = arg
                source_args = []
            elif _looks_like_path(arg):
                raise RingManagerSyncError('config file not found: %s' % arg)
    conf = _load_sync_conf(conf_file) if conf_file else {}

    source_urls = source_args
    if not source_urls:
        source_urls = _option_or_conf(
            options, 'source_urls', conf,
            ('source_urls', 'source_url', 'ring_manager_urls',
             'ring_manager_url'))
    source_urls = _validate_source_urls(source_urls)

    recon_dump = options.recon_dump
    if recon_dump is None:
        recon_dump = config_true_value(conf.get('recon_dump', 'true'))

    timeout = _option_or_conf(
        options, 'timeout', conf, ('request_timeout', 'timeout'), default=30)
    sync_lock_timeout = _option_or_conf(
        options, 'sync_lock_timeout', conf, ('sync_lock_timeout',),
        default=30)
    hook_timeout = _option_or_conf(
        options, 'state_change_hook_timeout', conf,
        ('state_change_hook_timeout',),
        default=DEFAULT_STATE_CHANGE_HOOK_TIMEOUT)
    admin_key, admin_key_file = _option_pair_or_conf(
        options, ('admin_key', 'admin_key_file'), conf)
    read_key, read_key_file = _option_pair_or_conf(
        options, ('read_key', 'read_key_file'), conf)
    auth_token, read_auth_token = _option_pair_or_conf(
        options, ('auth_token', 'read_auth_token'), conf)
    sync_builder_files = options.sync_builder_files
    if sync_builder_files is None:
        sync_builder_files = config_true_value(
            conf.get('sync_builder_files', 'false'))

    return {
        'conf': conf,
        'source_urls': source_urls,
        'state_dir': _option_or_conf(
            options, 'state_dir', conf,
            ('ring_manager_state_dir', 'state_dir')),
        'artifact_dir': _option_or_conf(
            options, 'artifact_dir', conf,
            ('ring_artifact_dir', 'artifact_dir')),
        'builder_dir': _option_or_conf(
            options, 'builder_dir', conf,
            ('ring_builder_dir', 'builder_dir'),
            default=DEFAULT_RING_BUILDER_DIR),
        'sync_builder_files': sync_builder_files,
        'admin_key': admin_key,
        'admin_key_file': admin_key_file,
        'auth_token': auth_token,
        'read_key': read_key,
        'read_key_file': read_key_file,
        'read_auth_token': read_auth_token,
        'timeout': _non_negative_float_option(timeout, 'timeout'),
        'sync_lock_timeout': _non_negative_float_option(
            sync_lock_timeout, 'sync_lock_timeout'),
        'recon_cache_path': _option_or_conf(
            options, 'recon_cache_path', conf, ('recon_cache_path',),
            default=DEFAULT_RECON_CACHE_PATH),
        'recon_dump': recon_dump,
        'state_change_hook': _option_or_conf(
            options, 'state_change_hook', conf, ('state_change_hook',)),
        'state_change_hook_timeout': _non_negative_float_option(
            hook_timeout, 'state_change_hook_timeout'),
    }


def _logger_conf(options, conf):
    logger_conf = dict(
        (key, value) for key, value in conf.items()
        if value not in (None, ''))
    for name in (
            'log_statsd_host', 'log_statsd_port',
            'log_statsd_default_sample_rate',
            'log_statsd_sample_rate_factor',
            'log_statsd_metric_prefix'):
        value = getattr(options, name)
        if value not in (None, ''):
            logger_conf[name] = value
    return logger_conf


def main(argv=None):
    parser = _make_parser()
    options, args = parser.parse_args(argv)
    try:
        sync_conf = _resolve_sync_conf(options, args)
        try:
            logger = get_logger(
                _logger_conf(options, sync_conf['conf']),
                log_route=USER_AGENT,
                statsd_tail_prefix=USER_AGENT)
        except (TypeError, ValueError) as err:
            raise RingManagerSyncError('invalid logger config: %s' % err)
        syncer = RingManagerSync(
            sync_conf['source_urls'],
            sync_conf['state_dir'],
            sync_conf['artifact_dir'],
            admin_key=sync_conf['admin_key'],
            admin_key_file=sync_conf['admin_key_file'],
            auth_token=sync_conf['auth_token'],
            read_key=sync_conf['read_key'],
            read_key_file=sync_conf['read_key_file'],
            read_auth_token=sync_conf['read_auth_token'],
            timeout=sync_conf['timeout'],
            sync_lock_timeout=sync_conf['sync_lock_timeout'],
            recon_cache_path=sync_conf['recon_cache_path'],
            recon_dump=sync_conf['recon_dump'],
            state_change_hook=sync_conf['state_change_hook'],
            state_change_hook_timeout=sync_conf[
                'state_change_hook_timeout'],
            builder_dir=sync_conf['builder_dir'],
            sync_builder_files=sync_conf['sync_builder_files'],
            logger=logger)
        result = syncer.sync()
    except RingManagerSyncError as err:
        print('ERROR: %s' % err, file=sys.stderr)
        return 1

    if not options.quiet:
        print('Synced ring-manager version %(latest_ring_version)s: '
              '%(rings_synced)d rings, %(ring_versions_synced)d per-ring '
              'versions, %(manifest_files_downloaded)d manifest files '
              'downloaded, %(manifest_files_unchanged)d unchanged, '
              '%(builder_files_downloaded)d builder files downloaded, '
              '%(builder_files_unchanged)d unchanged' % result)
    return 0


if __name__ == '__main__':
    sys.exit(main())
