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
from swift.common.utils import NullLogger, dump_recon_cache, get_logger, mkdirs
from swift.common.utils import md5
from swift.ring_manager.common import DEFAULT_STATE_CHANGE_HOOK_TIMEOUT, \
    NormalTimestamp, StateChangeHook, normal_timestamp, stats_increment, \
    stats_timing


USER_AGENT = 'swift-ring-manager-sync'


class RingManagerSyncError(Exception):
    pass


class RingManagerSync(object):
    """
    Pull published ring-manager state and artifacts from a source server.

    The syncer writes the same directory layout consumed by
    ``RingManagerStore`` so read-only and standby ring-manager servers can
    serve already-published manifests and artifacts locally.
    """

    def __init__(self, source_url, state_dir, artifact_dir, admin_key=None,
                 auth_token=None, timeout=30, opener=None,
                 recon_cache_path=DEFAULT_RECON_CACHE_PATH, recon_dump=True,
                 logger=None, time_func=NormalTimestamp.now,
                 state_change_hook=None, state_change_hook_timeout=None):
        if not source_url:
            raise RingManagerSyncError('source_url is required')
        if not state_dir:
            raise RingManagerSyncError('ring_manager_state_dir is required')
        if not artifact_dir:
            raise RingManagerSyncError('ring_artifact_dir is required')
        self.source_url = source_url.rstrip('/')
        self.state_dir = state_dir
        self.artifact_dir = artifact_dir
        self.admin_key = admin_key
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

    def _timestamp(self, timestamp=None):
        timestamp = self.time_func() if timestamp is None else timestamp
        return normal_timestamp(timestamp)

    def _safe_id(self, value):
        return quote(str(value), safe='')

    def _api_url(self, path_or_url):
        return urljoin(self.source_url + '/', path_or_url)

    def _headers(self, extra=None):
        headers = {'User-Agent': USER_AGENT}
        if self.admin_key:
            headers['X-Ring-Manager-Admin-Key'] = self.admin_key
        if self.auth_token:
            headers['X-Auth-Token'] = self.auth_token
        if extra:
            headers.update(extra)
        return headers

    def _request(self, path_or_url, headers=None):
        url = self._api_url(path_or_url)
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

    def _json_request(self, path):
        _status, body, _headers = self._request(path)
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
        self._mkdirs(path)
        temp_path = '%s.tmp-%s' % (path, uuid.uuid4().hex)
        with open(temp_path, 'wb') as fp:
            fp.write(body)
        os.rename(temp_path, path)

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
            raise

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
        return file_info.get('url') or default_url

    def _download_file(self, file_info, default_url, local_path):
        headers = {}
        if file_info.get('sha256'):
            etag = self._verified_artifact_etag(local_path, file_info)
            if etag:
                headers['If-None-Match'] = etag
        url = self._file_url(file_info, default_url)
        status, body, _headers = self._request(url, headers=headers)
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

    def _sync_manifest_files(self, manifest):
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
            result = self._download_file(file_info, default_url, local_path)
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

    def _sync_rings(self):
        collection = self._json_request('/api/v1/rings/')
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

    def _sync_ring_artifact_versions(self, manifest_version, manifest):
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
                    file_info, default_url, local_path)
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

    def _write_latest(self, version, synced_at):
        index_path = self._state_path('index.json')
        index = self._read_json(index_path, {})
        if not isinstance(index, dict):
            raise RingManagerSyncError('local index.json must be an object')
        index['latest_ring_version'] = version
        index['ring_manager_sync'] = {
            'source': self.source_url,
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

    def _sync_stats(self, started_at, ended_at, synced_at, result):
        stats = dict(result)
        stats.update({
            'source': self.source_url,
            'success': True,
            'sync_time': float(ended_at) - float(started_at),
            'last_attempt': ended_at.internal,
            'last_attempted_at': synced_at,
            'last_success': ended_at.internal,
            'last_synced_at': synced_at,
            'error': {},
        })
        return stats

    def _failure_stats(self, started_at, err):
        ended_at = self._timestamp()
        attempted_at = ended_at.internal
        return {
            'source': self.source_url,
            'success': False,
            'sync_time': float(ended_at) - float(started_at),
            'last_attempt': ended_at.internal,
            'last_attempted_at': attempted_at,
            'error': str(err),
        }

    def sync(self):
        started_at = self._timestamp()
        stats_increment(self.logger, 'sync.attempts')
        try:
            manifest = self._json_request(
                '/api/v1/rings/releases/latest/manifest/')
            version, local_manifest, downloaded, unchanged = \
                self._sync_manifest_files(manifest)
            rings = self._sync_rings()
            per_ring, per_ring_downloaded, per_ring_unchanged = \
                self._sync_ring_artifact_versions(version, manifest)

            self._write_json_atomic(
                self._state_path(
                    'releases', self._safe_id(version), 'manifest.json'),
                local_manifest)
            ended_at = self._timestamp()
            synced_at = ended_at.internal
            self._write_latest(version, synced_at)

            result = {
                'latest_ring_version': version,
                'manifest_files_downloaded': downloaded,
                'manifest_files_unchanged': unchanged,
                'rings_synced': rings,
                'ring_versions_synced': per_ring,
                'ring_version_files_downloaded': per_ring_downloaded,
                'ring_version_files_unchanged': per_ring_unchanged,
            }
            stats_increment(self.logger, 'sync.successes')
            stats_timing(
                self.logger, 'sync.timing',
                float(ended_at) - float(started_at))
            stats_increment(
                self.logger, 'sync.manifest_files.downloaded', downloaded)
            stats_increment(
                self.logger, 'sync.manifest_files.unchanged', unchanged)
            stats_increment(self.logger, 'sync.rings_synced', rings)
            stats_increment(self.logger, 'sync.ring_versions_synced', per_ring)
            stats_increment(
                self.logger, 'sync.ring_version_files.downloaded',
                per_ring_downloaded)
            stats_increment(
                self.logger, 'sync.ring_version_files.unchanged',
                per_ring_unchanged)
            self._dump_recon(
                self._sync_stats(started_at, ended_at, synced_at, result))
            return result
        except Exception as err:
            ended_at = self._timestamp()
            stats_increment(self.logger, 'sync.failures')
            stats_timing(
                self.logger, 'sync.timing',
                float(ended_at) - float(started_at))
            self._dump_recon(self._failure_stats(started_at, err))
            raise


def _make_parser():
    parser = optparse.OptionParser(
        usage='%prog SOURCE_URL [options]',
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
        '--auth-token', dest='auth_token',
        help='Value for X-Auth-Token when fetching from source.')
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
    if len(args) != 1:
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
            args[0],
            options.state_dir,
            options.artifact_dir,
            admin_key=options.admin_key,
            auth_token=options.auth_token,
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
