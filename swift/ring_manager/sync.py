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
import sys
import uuid

from urllib.parse import quote, urljoin

from swift.common.concurrency import socket, urllib_request
from swift.common.recon import DEFAULT_RECON_CACHE_PATH, \
    RECON_RING_MANAGER_FILE
from swift.common.utils import NullLogger, dump_recon_cache, get_logger, \
    list_from_csv, mkdirs
from swift.common.utils import md5
from swift.ring_manager.common import DEFAULT_STATE_CHANGE_HOOK_TIMEOUT, \
    load_secret, NormalTimestamp, StateChangeHook, normal_timestamp, \
    stats_increment, stats_timing, validate_relative_api_url


USER_AGENT = 'swift-ring-manager-sync'


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
                 state_change_hook=None, state_change_hook_timeout=None):
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
        try:
            self.read_key = load_secret(
                read_key, 'read_key', read_key_file, 'read_key_file')
            self.admin_key = load_secret(
                admin_key, 'admin_key', admin_key_file, 'admin_key_file')
        except ValueError as err:
            raise RingManagerSyncError(str(err))
        self.read_auth_token = read_auth_token
        self.auth_token = auth_token
        self.timeout = timeout
        self.opener = opener or urllib_request.urlopen
        self.recon_cache_path = recon_cache_path or DEFAULT_RECON_CACHE_PATH
        self.recon_cache = os.path.join(
            self.recon_cache_path, RECON_RING_MANAGER_FILE)
        self.recon_dump = recon_dump
        self.logger = logger or NullLogger()
        self.time_func = time_func
        self.state_change_hook = StateChangeHook(
            state_change_hook, state_dir=state_dir,
            timeout=state_change_hook_timeout, logger=self.logger)

    def _normalize_source_urls(self, source_url):
        values = source_url
        if isinstance(values, str):
            values = list_from_csv(values)
        urls = []
        for value in values:
            if isinstance(value, str):
                urls.extend(list_from_csv(value))
            else:
                urls.append(value)
        urls = [str(url).rstrip('/') for url in urls if url]
        if not urls:
            raise RingManagerSyncError('source_url is required')
        return urls

    def _source_urls(self):
        return list(self.source_urls)

    def _timestamp(self, timestamp=None):
        timestamp = self.time_func() if timestamp is None else timestamp
        return normal_timestamp(timestamp)

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

    def _request(self, source_url, path_or_url, headers=None):
        url = self._api_url(source_url, path_or_url)
        req = urllib_request.Request(url, headers=self._headers(headers))
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

    def _json_request(self, source_url, path):
        _status, body, _headers = self._request(source_url, path)
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

    def _mkdirs(self, path):
        directory = os.path.dirname(path)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory)

    def _write_file_atomic(self, path, body):
        temp_path = None
        try:
            self._mkdirs(path)
            temp_path = '%s.tmp-%s' % (path, uuid.uuid4().hex)
            with open(temp_path, 'wb') as fp:
                fp.write(body)
            os.rename(temp_path, path)
            temp_path = None
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

    def _write_json_atomic(self, path, value):
        body = json.dumps(value, sort_keys=True, indent=2).encode('ascii')
        self._write_file_atomic(path, body + b'\n')
        self.state_change_hook.run('write', path)

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

    def _sync_manifest_files(self, source_url, manifest):
        version = str(manifest.get('version', ''))
        if not version:
            raise RingManagerSyncError('latest manifest has no version')
        files = manifest.get('files', [])
        if not isinstance(files, list):
            raise RingManagerSyncError('latest manifest files must be a list')

        downloaded = unchanged = 0
        localized_files = []
        for file_info in files:
            if not isinstance(file_info, dict) or not file_info.get('name'):
                raise RingManagerSyncError(
                    'latest manifest files must contain objects with name')
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
        synced = 0
        for ring in objects:
            if not isinstance(ring, dict) or ring.get('id') in (None, ''):
                raise RingManagerSyncError(
                    'rings collection objects must contain ids')
            local_ring = dict(ring)
            local_ring.pop('resource_uri', None)
            self._write_json_atomic(
                self._state_path(
                    'rings', '%s.json' % self._safe_id(local_ring['id'])),
                local_ring)
            synced += 1
        return synced

    def _sync_ring_artifact_versions(self, source_url, manifest_version,
                                     manifest):
        synced = 0
        downloaded = unchanged = 0
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
            self._write_json_atomic(
                self._state_path(
                    'ring-versions', self._safe_id(ring_id),
                    '%s.json' % self._safe_id(version_id)),
                local_version)
            synced += 1
        return synced, downloaded, unchanged

    def _write_latest(self, source_url, version, synced_at):
        index_path = self._state_path('index.json')
        index = self._read_json(index_path, {})
        if not isinstance(index, dict):
            raise RingManagerSyncLocalError(
                'local index.json must be an object')
        index['latest_ring_version'] = version
        index['ring_manager_sync'] = {
            'source': source_url,
            'latest_ring_version': version,
            'synced_at': synced_at,
        }
        self._write_json_atomic(index_path, index)

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
                    source_errors=None):
        stats = dict(result)
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
        return stats

    def _failure_stats(self, started_at, err, source_url=None,
                       source_errors=None):
        ended_at = self._timestamp()
        attempted_at = ended_at.internal
        stats = {
            'source': source_url or self.source_url,
            'success': False,
            'sync_time': float(ended_at) - float(started_at),
            'last_attempt': ended_at.internal,
            'last_attempted_at': attempted_at,
            'error': str(err),
        }
        if len(self.source_urls) > 1:
            stats['sources'] = list(self.source_urls)
        if source_errors:
            stats['source_errors'] = list(source_errors)
        return stats

    def _source_status(self, source_url):
        return self._json_request(source_url,
                                  '/api/v1/ring_manager/status/')

    def _validate_source(self, source_url):
        status = self._source_status(source_url)
        mode = status.get('mode')
        if mode == 'primary':
            return {
                'mode': mode,
                'latest_ring_version': status.get('latest_ring_version'),
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
                'sync_timestamp': synced_at,
            }
        raise RingManagerSyncError(
            'source %s returned unsupported ring-manager mode %r' %
            (source_url, mode))

    def _sync_from_source(self, source_url):
        source_status = self._validate_source(source_url)
        manifest = self._json_request(
            source_url, '/api/v1/rings/releases/latest/manifest/')
        version, local_manifest, downloaded, unchanged = \
            self._sync_manifest_files(source_url, manifest)
        source_latest = source_status.get('latest_ring_version')
        if source_latest not in (None, '') and str(source_latest) != version:
            raise RingManagerSyncError(
                'source %s status latest version %s does not match latest '
                'manifest version %s' % (source_url, source_latest, version))
        rings = self._sync_rings(source_url)
        per_ring, per_ring_downloaded, per_ring_unchanged = \
            self._sync_ring_artifact_versions(source_url, version, manifest)

        self._write_json_atomic(
            self._state_path(
                'releases', self._safe_id(version), 'manifest.json'),
            local_manifest)
        ended_at = self._timestamp()
        synced_at = source_status.get('sync_timestamp') or ended_at.internal
        self._write_latest(source_url, version, synced_at)

        return {
            'latest_ring_version': version,
            'manifest_files_downloaded': downloaded,
            'manifest_files_unchanged': unchanged,
            'rings_synced': rings,
            'ring_versions_synced': per_ring,
            'ring_version_files_downloaded': per_ring_downloaded,
            'ring_version_files_unchanged': per_ring_unchanged,
        }, synced_at, ended_at

    def _record_failure(self, started_at, err, source_url=None,
                        source_errors=None):
        ended_at = self._timestamp()
        stats_increment(self.logger, 'sync.failures')
        stats_timing(
            self.logger, 'sync.timing',
            float(ended_at) - float(started_at))
        self._dump_recon(self._failure_stats(
            started_at, err, source_url=source_url,
            source_errors=source_errors))

    def sync(self):
        started_at = self._timestamp()
        stats_increment(self.logger, 'sync.attempts')
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
                    source_errors=source_errors)
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
            self._dump_recon(
                self._sync_stats(
                    source_url, started_at, ended_at, synced_at, result,
                    source_errors=source_errors))
            return result

        if len(self.source_urls) == 1 and last_error is not None:
            self._record_failure(
                started_at, last_error, source_url=last_source,
                source_errors=source_errors)
            raise last_error
        err = RingManagerSyncError(
            'all ring-manager sources failed: %s' % '; '.join(
                '%s: %s' % (item['source'], item['error'])
                for item in source_errors))
        self._record_failure(
            started_at, err, source_url=last_source,
            source_errors=source_errors)
        raise err


def _make_parser():
    parser = optparse.OptionParser(
        usage='%prog SOURCE_URL [SOURCE_URL ...] [options]',
        description='Sync published ring-manager state and artifacts from '
                    'SOURCE_URL into a local ring-manager state directory.')
    parser.add_option(
        '--ring-manager-state-dir', dest='state_dir',
        help='Local ring_manager_state_dir to write.')
    parser.add_option(
        '--ring-artifact-dir', dest='artifact_dir',
        help='Local ring_artifact_dir to write.')
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
        '--timeout', dest='timeout', type='float', default=30,
        help='HTTP request timeout in seconds. Default: 30')
    parser.add_option(
        '--recon-cache-path', dest='recon_cache_path',
        default=DEFAULT_RECON_CACHE_PATH,
        help='Directory for ring-manager recon cache data. Default: %default')
    parser.add_option(
        '--no-recon-dump', action='store_false', dest='recon_dump',
        default=True,
        help='Do not write ring-manager sync stats to recon cache.')
    parser.add_option(
        '--state-change-hook', dest='state_change_hook',
        help='Command to run after each ring-manager state JSON write.')
    parser.add_option(
        '--state-change-hook-timeout', dest='state_change_hook_timeout',
        type='float', default=DEFAULT_STATE_CHANGE_HOOK_TIMEOUT,
        help='Seconds to wait for --state-change-hook. Use 0 for no timeout. '
             'Default: %default')
    parser.add_option(
        '--log-statsd-host', dest='log_statsd_host',
        help='StatsD host for sync metrics.')
    parser.add_option(
        '--log-statsd-port', dest='log_statsd_port', type='int',
        default=8125,
        help='StatsD port for sync metrics. Default: %default')
    parser.add_option(
        '--log-statsd-default-sample-rate',
        dest='log_statsd_default_sample_rate', type='float', default=1.0,
        help='Default StatsD sample rate. Default: %default')
    parser.add_option(
        '--log-statsd-sample-rate-factor',
        dest='log_statsd_sample_rate_factor', type='float', default=1.0,
        help='StatsD sample rate multiplier. Default: %default')
    parser.add_option(
        '--log-statsd-metric-prefix', dest='log_statsd_metric_prefix',
        help='Prefix to prepend to sync metric names.')
    parser.add_option(
        '-q', '--quiet', action='store_true', default=False,
        help='Do not print a successful sync summary.')
    return parser


def main(argv=None):
    parser = _make_parser()
    options, args = parser.parse_args(argv)
    if len(args) < 1:
        parser.print_usage()
        print('Error: SOURCE_URL is required')
        return 1
    logger_conf = {}
    if options.log_statsd_host:
        logger_conf.update({
            'log_statsd_host': options.log_statsd_host,
            'log_statsd_port': options.log_statsd_port,
            'log_statsd_default_sample_rate':
                options.log_statsd_default_sample_rate,
            'log_statsd_sample_rate_factor':
                options.log_statsd_sample_rate_factor,
        })
    if options.log_statsd_metric_prefix:
        logger_conf['log_statsd_metric_prefix'] = \
            options.log_statsd_metric_prefix
    try:
        syncer = RingManagerSync(
            args,
            options.state_dir,
            options.artifact_dir,
            admin_key=options.admin_key,
            admin_key_file=options.admin_key_file,
            auth_token=options.auth_token,
            read_key=options.read_key,
            read_key_file=options.read_key_file,
            read_auth_token=options.read_auth_token,
            timeout=options.timeout,
            recon_cache_path=options.recon_cache_path,
            recon_dump=options.recon_dump,
            state_change_hook=options.state_change_hook,
            state_change_hook_timeout=options.state_change_hook_timeout,
            logger=get_logger(
                logger_conf, log_route=USER_AGENT,
                statsd_tail_prefix=USER_AGENT))
        result = syncer.sync()
    except RingManagerSyncError as err:
        print('ERROR: %s' % err, file=sys.stderr)
        return 1

    if not options.quiet:
        print('Synced ring-manager version %(latest_ring_version)s: '
              '%(rings_synced)d rings, %(ring_versions_synced)d per-ring '
              'versions, %(manifest_files_downloaded)d manifest files '
              'downloaded, %(manifest_files_unchanged)d unchanged' % result)
    return 0


if __name__ == '__main__':
    sys.exit(main())
