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

import copy
import errno
import json
import math
import os
import tempfile
import uuid

from contextlib import contextmanager
from urllib.parse import quote, unquote

from swift.common.utils import config_true_value, fsync, fsync_dir, lock_path
from swift.ring_manager.common import DEFAULT_BUILD_JOB_LEASE_TIMEOUT, \
    RESERVED_ARTIFACT_VERSION_IDS, StateChangeHook, normal_timestamp_float, \
    normal_timestamp_internal, resolve_artifact_path, \
    validate_artifact_version_id, validate_path_component


class RingNotFound(KeyError):
    pass


class RingAlreadyExists(KeyError):
    pass


class RingVersionNotFound(KeyError):
    pass


class RingVersionFileNotFound(KeyError):
    pass


class RingVersionReserved(Exception):
    def __init__(self, version):
        self.version = version
        super(RingVersionReserved, self).__init__(
            'published ring version %s is reserved by tombstone' % version)


class RingArtifactVersionReserved(Exception):
    def __init__(self, ring_id, version):
        self.ring_id = ring_id
        self.version = version
        super(RingArtifactVersionReserved, self).__init__(
            'ring %s artifact version %s is reserved by tombstone' % (
                ring_id, version))


class RingBuildNotFound(KeyError):
    pass


class RingBuildPublishedVersionConflict(Exception):
    def __init__(self, version, reserved=False):
        self.version = version
        self.reserved = reserved
        if reserved:
            message = 'published ring version %s is reserved by tombstone' % \
                version
        else:
            message = 'published ring version %s already exists' % version
        super(RingBuildPublishedVersionConflict, self).__init__(
            message)


class RingBuildVersionConflict(Exception):
    def __init__(self, version, build):
        self.version = version
        self.build = build
        super(RingBuildVersionConflict, self).__init__(
            'active ring build %s in state %s already exists for '
            'version %s' % (
                build.get('id'), build.get('state', 'unknown'), version))


class RingBuildStateConflict(Exception):
    pass


class RingDesiredVersionConflict(Exception):
    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual
        super(RingDesiredVersionConflict, self).__init__(
            'desired ring version changed from %r to %r' % (
                expected, actual))


TERMINAL_RING_BUILD_STATES = ('completed', 'failed', 'cancelled')
READY_RING_BUILD_STATES = ('queued', 'deferred')
ACTIVE_RING_BUILD_STATES = READY_RING_BUILD_STATES + ('building',)
CLEANUP_BLOCKING_WARNING_TYPES = (
    'artifact_scan_failed',
    'artifact_stat_failed',
    'invalid_artifact_path',
    'invalid_manifest_ring',
    'missing_artifact_dir',
    'missing_artifact_file',
    'missing_latest_ring_artifact',
    'missing_latest_ring_version',
    'missing_ring_artifact_version',
    'missing_ring_id',
    'ring_artifact_version_ring_id_mismatch',
    'tombstoned_manifest_record',
    'tombstoned_ring_artifact_version_record',
)
REPAIRABLE_CLEANUP_WARNING_TYPES = (
    'tombstoned_manifest_record',
    'tombstoned_ring_artifact_version_record',
)
ARTIFACT_FILE_CLEANUP_BLOCKER_SAMPLE_LIMIT = 10


def _add_reason(reasons, key, reason):
    reasons.setdefault(key, [])
    if reason not in reasons[key]:
        reasons[key].append(reason)


class RingManagerStore(object):
    """Directory-backed durable state for the ring-manager service."""

    BUILDER_OWNED_RING_FIELDS = (
        'part_power', 'num_replicas', 'min_part_hours', 'overload')

    def __init__(self, state_dir=None, ring_artifact_dir=None,
                 state_change_hook=None, state_change_hook_timeout=None,
                 logger=None, state_change_hook_background_runner=None):
        self.state_dir = state_dir
        self.ring_artifact_dir = ring_artifact_dir
        self.state_change_hook = StateChangeHook(
            state_change_hook, state_dir=state_dir,
            timeout=state_change_hook_timeout, logger=logger,
            background_runner=state_change_hook_background_runner)

    def _safe_id(self, object_id):
        return quote(str(object_id), safe='')

    def _path_exists(self, path):
        if path is None:
            return False
        return os.path.exists(path)

    def _ring_version_tombstone_file(self, version_id):
        return self._state_dir_path(
            'tombstones', 'versions',
            '%s.json' % self._safe_id(version_id))

    def _ring_artifact_version_tombstone_file(self, ring_id, version_id):
        return self._state_dir_path(
            'tombstones', 'ring-versions', self._safe_id(ring_id),
            '%s.json' % self._safe_id(version_id))

    def _read_json_file(self, path, default=None):
        if path is None:
            return copy.deepcopy(default)
        try:
            with open(path, 'r') as fp:
                value = json.load(fp)
        except IOError as err:
            if err.errno == errno.ENOENT:
                return copy.deepcopy(default)
            raise
        return value

    def _write_json_file(self, path, value):
        body = json.dumps(
            value, sort_keys=True, indent=2).encode('ascii') + b'\n'
        self._write_file_durable(path, body)
        self.state_change_hook.run('write', path)

    def _temporary_state_file(self, path):
        directory = os.path.dirname(path)
        return tempfile.mkstemp(
            prefix='.%s.' % os.path.basename(path),
            suffix='.tmp', dir=directory)

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

    def _write_file_durable(self, path, body):
        if path is None:
            raise ValueError(
                'ring_manager_state_dir is required for mutating requests')
        directory = os.path.dirname(path)
        self._ensure_directory_durable(directory)
        fd, temp_path = self._temporary_state_file(path)
        try:
            with os.fdopen(fd, 'wb') as fp:
                fp.write(body)
                fp.flush()
                fsync(fp.fileno())
            os.rename(temp_path, path)
            temp_path = None
            if directory:
                fsync_dir(directory)
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except OSError as err:
                    if err.errno != errno.ENOENT:
                        raise

    def _delete_state_file(self, path, missing_ok=False,
                           run_state_change_hook=True):
        if path is None:
            raise ValueError(
                'ring_manager_state_dir is required for mutating requests')
        try:
            os.unlink(path)
        except OSError as err:
            if err.errno == errno.ENOENT and missing_ok:
                return False
            raise
        directory = os.path.dirname(path)
        if directory:
            fsync_dir(directory)
        if run_state_change_hook:
            self.state_change_hook.run('delete', path)
        return True

    def _state_dir_path(self, *parts):
        if not self.state_dir:
            return None
        return os.path.join(self.state_dir, *parts)

    @contextmanager
    def published_state_lock(self):
        if not self.state_dir:
            raise ValueError(
                'ring_manager_state_dir is required for mutating requests')
        with lock_path(self.state_dir, name='ring-manager-published-state'):
            yield

    def _state_index(self):
        index = self._read_json_file(
            self._state_dir_path('index.json'), {})
        if not isinstance(index, dict):
            raise ValueError('Ring manager state index must be an object')
        return index

    def get_state_index(self):
        return copy.deepcopy(self._state_index())

    def _save_state_index(self, index):
        self._write_json_file(self._state_dir_path('index.json'), index)

    def _mutate_state_index(self, mutate):
        if not self.state_dir:
            raise ValueError(
                'ring_manager_state_dir is required for mutating requests')
        with lock_path(self.state_dir, name='ring-manager-index'):
            index = self._state_index()
            changed, result = mutate(index)
            if changed:
                self._save_state_index(index)
            return result

    def _collection_dir(self, collection):
        return self._state_dir_path(collection)

    def _collection_file(self, collection, object_id):
        directory = self._collection_dir(collection)
        if directory is None:
            return None
        return os.path.join(
            directory, '%s.json' % self._safe_id(object_id))

    def _load_dir_object(self, collection, object_id, not_found):
        if object_id is None:
            raise not_found(object_id)
        path = self._collection_file(collection, object_id)
        obj = self._read_json_file(path)
        if obj is not None:
            if not isinstance(obj, dict):
                raise ValueError('%s object must be a JSON object' %
                                 collection)
            return obj
        for obj in self._list_dir_objects(collection):
            if self._object_id_matches(obj, 'id', object_id):
                return obj
        raise not_found(object_id)

    def _list_dir_objects(self, collection):
        directory = self._collection_dir(collection)
        if directory is None:
            return []
        try:
            names = sorted(os.listdir(directory))
        except OSError as err:
            if err.errno == errno.ENOENT:
                return []
            raise
        objects = []
        for name in names:
            if not name.endswith('.json'):
                continue
            obj = self._read_json_file(os.path.join(directory, name))
            if obj is None:
                continue
            if not isinstance(obj, dict):
                raise ValueError('%s/%s must be a JSON object' %
                                 (collection, name))
            objects.append(obj)
        return objects

    def _save_dir_object(self, collection, obj):
        object_id = obj.get('id')
        if object_id in (None, ''):
            raise ValueError('%s objects require an id' % collection)
        self._write_json_file(self._collection_file(collection, object_id),
                              obj)

    def _delete_dir_object(self, collection, object_id, not_found):
        path = self._collection_file(collection, object_id)
        try:
            self._delete_state_file(path)
        except OSError as err:
            if err.errno == errno.ENOENT:
                raise not_found(object_id)
            raise

    def _object_id_matches(self, obj, key, value):
        if value is None:
            return True
        expected = str(value)
        actual = obj.get(key)
        if actual is None:
            return False
        if str(actual) == expected:
            return True
        return str(actual).rstrip('/').endswith('/%s' % expected)

    def _matches_cluster(self, obj, cluster_id):
        return (
            self._object_id_matches(obj, 'cluster_id', cluster_id) or
            self._object_id_matches(obj, 'cluster', cluster_id)
        )

    def _artifact_relpath(self, path):
        if not self.ring_artifact_dir:
            return None
        root = os.path.realpath(self.ring_artifact_dir)
        resolved = os.path.realpath(path)
        try:
            common_path = os.path.commonpath([root, resolved])
        except ValueError:
            return None
        if common_path != root:
            return None
        return os.path.relpath(resolved, root).replace(os.sep, '/')

    def _artifact_file_relpath(self, version, file_info, warnings,
                               owner_type, owner_id):
        try:
            path = self._resolve_artifact_path(version, file_info)
        except (RingVersionFileNotFound, ValueError) as err:
            warnings.append({
                'type': 'invalid_artifact_path',
                'owner_type': owner_type,
                'owner_id': str(owner_id),
                'file': copy.deepcopy(file_info),
                'error': str(err),
            })
            return None
        relpath = self._artifact_relpath(path)
        if relpath is None:
            warnings.append({
                'type': 'invalid_artifact_path',
                'owner_type': owner_type,
                'owner_id': str(owner_id),
                'file': copy.deepcopy(file_info),
                'error': 'artifact path escapes ring_artifact_dir',
            })
        return relpath

    def _with_resource_uri(self, obj, collection):
        obj = copy.deepcopy(obj)
        if 'id' in obj and 'resource_uri' not in obj:
            obj['resource_uri'] = '/api/v1/%s/%s/' % (
                collection, self._safe_id(obj['id']))
        if collection == 'rings' and 'id' in obj:
            obj = self._strip_builder_owned_ring_fields(obj)
            obj['disabled'] = self.ring_is_disabled(obj)
            obj.setdefault(
                'devices_url',
                '/api/v1/rings/%s/devices/' % self._safe_id(obj['id']))
        return obj

    def _ring_version_id(self, version):
        return str(version.get('version', version.get('id')))

    def _ring_version_uri(self, version):
        return '/api/v1/rings/releases/%s/' % self._safe_id(
            self._ring_version_id(version))

    def _ring_version_file_url(self, version, file_info):
        return '%sfiles/%s' % (
            self._ring_version_uri(version),
            self._safe_id(file_info['name']))

    def _public_ring_version_file(self, version, file_info):
        public_file = copy.deepcopy(file_info)
        validate_path_component(public_file.get('name'), 'artifact file name')
        for key in ('path', 'artifact_dir', '_artifact_root'):
            public_file.pop(key, None)
        public_file['url'] = self._ring_version_file_url(
            version, file_info)
        return public_file

    def _public_ring_version(self, version, latest=False, desired=False):
        public_version = copy.deepcopy(version)
        public_version.pop('_artifact_root', None)
        public_version.pop('_manifest_path', None)
        public_version.pop('artifact_dir', None)
        public_version.pop('manifest', None)
        public_version['version'] = self._ring_version_id(version)
        public_version['resource_uri'] = self._ring_version_uri(version)
        public_version['latest'] = latest or bool(version.get('latest'))
        public_version['desired'] = bool(desired)
        public_version['files'] = [
            self._public_ring_version_file(version, file_info)
            for file_info in version.get('files', [])]
        return public_version

    def _manifest_for_ring_version(self, version, latest=False,
                                   desired=False):
        manifest = copy.deepcopy(version.get('manifest', {}))
        for key in ('path', 'artifact_dir', '_artifact_root',
                    '_manifest_path'):
            manifest.pop(key, None)
        manifest['version'] = self._ring_version_id(version)
        manifest['resource_uri'] = self._ring_version_uri(version)
        manifest['latest'] = latest or bool(version.get('latest'))
        manifest['desired'] = bool(desired)
        for key in ('cluster_id', 'cluster', 'state', 'created_at', 'rings'):
            if key in version and key not in manifest:
                manifest[key] = version[key]
        manifest['files'] = [
            self._public_ring_version_file(version, file_info)
            for file_info in version.get('files', [])]
        return manifest

    def _resolve_artifact_path(self, version, file_info):
        path = file_info.get('path', file_info.get('name'))
        root = self.ring_artifact_dir or version.get('artifact_dir')
        root = root or version.get('_artifact_root')
        try:
            return resolve_artifact_path(root, path, 'artifact path')
        except ValueError:
            raise RingVersionFileNotFound(file_info.get('name', path))

    def _strip_builder_owned_ring_fields(self, ring):
        ring = copy.deepcopy(ring)
        ring.pop('devices', None)
        for key in self.BUILDER_OWNED_RING_FIELDS:
            ring.pop(key, None)
        return ring

    def ring_is_disabled(self, ring):
        return config_true_value(str(ring.get('disabled', False)))

    def _normalize_ring_for_save(self, ring):
        ring = self._strip_builder_owned_ring_fields(ring)
        ring['disabled'] = self.ring_is_disabled(ring)
        return ring

    def _next_id(self, objects):
        next_id = 1
        for obj in objects:
            try:
                next_id = max(next_id, int(obj.get('id')) + 1)
            except (TypeError, ValueError):
                continue
        return next_id

    def _default_ring_id(self, ring):
        ring_type = ring.get('ring_type') or ring.get('type')
        if ring_type in ('account', 'container'):
            return ring_type
        if (ring_type == 'object' or
                ring.get('storage_policy_index') is not None):
            policy_index = ring.get('storage_policy_index')
            if policy_index in (None, ''):
                policy_index = 0
            return 'object-%s' % policy_index
        return 'ring-%s' % self._next_id(self._list_dir_objects('rings'))

    def list_rings(self, cluster_id=None):
        rings = self._list_dir_objects('rings')
        return [
            self._with_resource_uri(ring, 'rings')
            for ring in rings
            if self._matches_cluster(ring, cluster_id)
        ]

    def get_ring(self, ring_id):
        ring = self._load_dir_object('rings', ring_id, RingNotFound)
        return self._with_resource_uri(ring, 'rings')

    def create_ring(self, ring):
        ring = copy.deepcopy(ring)
        ring.pop('resource_uri', None)
        ring = self._normalize_ring_for_save(ring)
        ring.setdefault('id', self._default_ring_id(ring))
        try:
            self._load_dir_object('rings', ring['id'], RingNotFound)
        except RingNotFound:
            pass
        else:
            raise RingAlreadyExists(ring['id'])
        self._save_dir_object('rings', ring)
        return self._with_resource_uri(ring, 'rings')

    def update_ring(self, ring_id, updates, replace=False):
        existing = self._load_dir_object('rings', ring_id, RingNotFound)
        if 'id' in updates and str(updates['id']) != str(ring_id):
            raise ValueError('Request body id does not match ring id')
        if replace:
            ring = copy.deepcopy(updates)
        else:
            ring = copy.deepcopy(existing)
            ring.update(updates)
        ring['id'] = existing['id']
        ring.pop('resource_uri', None)
        ring = self._normalize_ring_for_save(ring)
        self._save_dir_object('rings', ring)
        return self._with_resource_uri(ring, 'rings')

    def delete_ring(self, ring_id):
        ring = self._load_dir_object('rings', ring_id, RingNotFound)
        self._delete_dir_object('rings', ring['id'], RingNotFound)
        return self._with_resource_uri(ring, 'rings')

    def _ring_build_sort_key(self, build):
        try:
            return (0, int(build.get('sequence')))
        except (TypeError, ValueError):
            return (
                1, str(build.get('created_at', '')),
                str(build.get('id', '')))

    def _ring_build_scope(self, build):
        request = build.get('request') or {}
        if config_true_value(str(build.get(
                'artifact_only', request.get('artifact_only', 'false')))):
            ring_id = build.get('ring_id', request.get('ring_id'))
            if ring_id not in (None, ''):
                return set([str(ring_id)])
        rings = build.get('rings', request.get('rings'))
        if rings is None:
            return None
        scope = set()
        for ring in rings:
            ring_id = ring.get('id', ring.get('ring_id')) \
                if isinstance(ring, dict) else ring
            if ring_id not in (None, ''):
                scope.add(str(ring_id))
        return scope or None

    def _ring_build_scopes_overlap(self, first, second):
        if first is None or second is None:
            return True
        return bool(first & second)

    def _ring_build_is_ready(self, build, timestamp):
        state = build.get('state')
        if state == 'queued':
            return True
        if state != 'deferred':
            return False
        deferred_until = build.get('deferred_until')
        if deferred_until in (None, ''):
            return True
        try:
            return normal_timestamp_float(deferred_until) <= \
                normal_timestamp_float(timestamp)
        except (TypeError, ValueError):
            return False

    def active_ring_builds_for_ring(self, ring_id, timestamp=None):
        ring_scope = set([str(ring_id)])
        builds = self._list_dir_objects('ring_builds')
        builds.sort(key=self._ring_build_sort_key)
        return [
            copy.deepcopy(build) for build in builds
            if build.get('state') not in TERMINAL_RING_BUILD_STATES
            and self._ring_build_scopes_overlap(
                self._ring_build_scope(build), ring_scope)
        ]

    def _ring_build_uri(self, build):
        return '/api/v1/rings/builds/%s/' % self._safe_id(build['id'])

    def _public_ring_build(self, build):
        build = copy.deepcopy(build)
        if 'id' in build:
            build['resource_uri'] = self._ring_build_uri(build)
        return build

    def _ring_build_version(self, build):
        version = build.get('version')
        if version is not None:
            return str(version)
        request = build.get('request')
        if isinstance(request, dict) and request.get('version') is not None:
            return str(request['version'])
        return None

    def _ring_build_is_current(self, build):
        try:
            return int(build.get('sequence')) > 0 and isinstance(
                build.get('request'), dict)
        except (TypeError, ValueError):
            return False

    def _active_ring_build_for_version(self, builds, version):
        for build in builds:
            if build.get('state') in TERMINAL_RING_BUILD_STATES:
                continue
            if self._ring_build_version(build) == str(version):
                return build
        return None

    def _ring_build_lease_expires_at(self, timestamp, lease_timeout):
        return normal_timestamp_internal(
            normal_timestamp_float(timestamp) + float(lease_timeout))

    def _ring_build_lease_expired(self, build, timestamp,
                                  lease_timeout=None):
        if build.get('state') != 'building':
            return False
        timestamp = normal_timestamp_float(timestamp)
        lease_timeout = (DEFAULT_BUILD_JOB_LEASE_TIMEOUT
                         if lease_timeout is None else float(lease_timeout))
        lease_expires_at = build.get('lease_expires_at')
        if lease_expires_at is not None:
            try:
                return normal_timestamp_float(lease_expires_at) <= timestamp
            except (TypeError, ValueError):
                return True
        for key in ('claimed_at', 'started_at', 'updated_at'):
            claimed_at = build.get(key)
            if claimed_at is None:
                continue
            try:
                return normal_timestamp_float(
                    claimed_at) + lease_timeout <= timestamp
            except (TypeError, ValueError):
                return True
        return True

    def _recover_stale_ring_build(self, build, timestamp):
        stale_claims = copy.deepcopy(build.get('stale_claims', []))
        stale_claims.append({
            'builder_id': build.get('builder_id'),
            'claimed_at': build.get('claimed_at') or build.get('started_at'),
            'lease_expires_at': build.get('lease_expires_at'),
            'recovered_at': timestamp,
        })
        build.update({
            'state': 'queued',
            'updated_at': timestamp,
            'stale_claims': stale_claims,
            'stale_recovered_at': timestamp,
            'stale_recovered_count': int(
                build.get('stale_recovered_count', 0)) + 1,
        })
        for key in ('builder_id', 'claimed_at', 'started_at',
                    'lease_expires_at', 'lease_refreshed_at',
                    'lease_timeout'):
            build.pop(key, None)
        self._save_dir_object('ring_builds', build)

    def _ring_build_matches_filter(self, build, retry_of=None,
                                   retry_root=None):
        if retry_of is not None:
            value = build.get('retry_of')
            if value is None or str(value) != str(retry_of):
                return False
        if retry_root is not None:
            value = build.get('retry_root')
            if value is None or str(value) != str(retry_root):
                return False
        return True

    def list_ring_builds(self, retry_of=None, retry_root=None):
        builds = self._list_dir_objects('ring_builds')
        builds.sort(key=self._ring_build_sort_key)
        builds = [
            build for build in builds
            if self._ring_build_matches_filter(
                build, retry_of=retry_of, retry_root=retry_root)
        ]
        return [self._public_ring_build(build) for build in builds]

    def get_ring_build(self, build_id):
        build = self._load_dir_object(
            'ring_builds', build_id, RingBuildNotFound)
        return self._public_ring_build(build)

    def create_ring_build(self, request, timestamp, extra=None):
        directory = self._collection_dir('ring_builds')
        if directory is None:
            raise ValueError(
                'ring_manager_state_dir is required for build requests')
        request = copy.deepcopy(request)
        with lock_path(directory, name='ring-build-queue'):
            existing_builds = self._list_dir_objects('ring_builds')
            artifact_only = config_true_value(str(
                request.get('artifact_only', 'false')))
            if not artifact_only and request.get('version') is not None:
                version = str(request['version'])
                if self.ring_version_tombstoned(version):
                    raise RingBuildPublishedVersionConflict(
                        version, reserved=True)
                if self.ring_version_exists(version):
                    raise RingBuildPublishedVersionConflict(version)
                conflict = self._active_ring_build_for_version(
                    existing_builds, version)
                if conflict is not None:
                    raise RingBuildVersionConflict(
                        version, self._public_ring_build(conflict))
            sequences = []
            for existing in existing_builds:
                try:
                    sequences.append(int(existing.get('sequence')))
                except (TypeError, ValueError):
                    continue
            sequence = max(sequences or [0]) + 1
            build = {
                'id': '%016d-%s' % (sequence, uuid.uuid4().hex),
                'sequence': sequence,
                'state': 'queued',
                'created_at': normal_timestamp_internal(timestamp),
                'updated_at': normal_timestamp_internal(timestamp),
                'request': request,
            }
            if artifact_only:
                build['artifact_only'] = True
                build['ring_id'] = request.get('ring_id')
            if request.get('version') is not None:
                build['version'] = str(request['version'])
            if request.get('rings') is not None:
                build['rings'] = copy.deepcopy(request['rings'])
            if extra:
                extra = copy.deepcopy(extra)
                for key in ('id', 'sequence', 'state', 'created_at',
                            'updated_at', 'request', 'resource_uri'):
                    extra.pop(key, None)
                build.update(extra)
            self._save_dir_object('ring_builds', build)
            return self._public_ring_build(build)

    def _update_ring_build(self, build_id, updates, timestamp):
        existing = self._load_dir_object(
            'ring_builds', build_id, RingBuildNotFound)
        if 'id' in updates and str(updates['id']) != str(build_id):
            raise ValueError('Request body id does not match build id')
        build = copy.deepcopy(existing)
        build.update(copy.deepcopy(updates))
        build['id'] = existing['id']
        build['updated_at'] = normal_timestamp_internal(timestamp)
        build.pop('resource_uri', None)
        if build.get('state') != 'building':
            for key in ('lease_expires_at', 'lease_refreshed_at',
                        'lease_timeout'):
                build.pop(key, None)
        self._save_dir_object('ring_builds', build)
        return self._public_ring_build(build)

    def cancel_ring_build(self, build_id, timestamp, reason=None):
        timestamp = normal_timestamp_internal(timestamp)
        directory = self._collection_dir('ring_builds')
        if directory is None:
            raise ValueError(
                'ring_manager_state_dir is required for build requests')
        with lock_path(directory, name='ring-build-queue'):
            existing = self._load_dir_object(
                'ring_builds', build_id, RingBuildNotFound)
            state = existing.get('state')
            if state == 'cancelled':
                return self._public_ring_build(existing), False
            if state not in ('queued', 'deferred'):
                raise RingBuildStateConflict(
                    'build %s in state %s cannot be cancelled' %
                    (build_id, state))
            build = copy.deepcopy(existing)
            build['state'] = 'cancelled'
            build['cancelled_from'] = state
            build['cancelled_at'] = timestamp
            build['updated_at'] = timestamp
            if reason not in (None, ''):
                build['cancel_reason'] = str(reason)
            build.pop('resource_uri', None)
            self._save_dir_object('ring_builds', build)
            return self._public_ring_build(build), True

    def update_claimed_ring_build(self, build_id, builder_id, claimed_at,
                                  updates, timestamp):
        directory = self._collection_dir('ring_builds')
        if directory is None:
            raise ValueError(
                'ring_manager_state_dir is required for build requests')
        timestamp = normal_timestamp_internal(timestamp)
        try:
            claimed_at = normal_timestamp_internal(claimed_at)
        except (TypeError, ValueError):
            return None
        with lock_path(directory, name='ring-build-queue'):
            existing = self._load_dir_object(
                'ring_builds', build_id, RingBuildNotFound)
            if (existing.get('state') != 'building' or
                    str(existing.get('builder_id')) != str(builder_id) or
                    str(existing.get('claimed_at')) != str(claimed_at) or
                    self._ring_build_lease_expired(existing, timestamp)):
                return None
            return self._update_ring_build(build_id, updates, timestamp)

    def claim_ring_build(self, builder_id, timestamp, build_id=None,
                         lease_timeout=None):
        directory = self._collection_dir('ring_builds')
        if directory is None:
            raise ValueError(
                'ring_manager_state_dir is required for build requests')
        timestamp = normal_timestamp_internal(timestamp)
        lease_timeout = (DEFAULT_BUILD_JOB_LEASE_TIMEOUT
                         if lease_timeout is None else float(lease_timeout))
        with lock_path(directory, name='ring-build-queue'):
            builds = self._list_dir_objects('ring_builds')
            builds.sort(key=self._ring_build_sort_key)
            unclaimable_scopes = []
            for candidate in builds:
                state = candidate.get('state')
                if state in TERMINAL_RING_BUILD_STATES:
                    continue
                if state == 'building':
                    if self._ring_build_lease_expired(
                            candidate, timestamp,
                            lease_timeout=lease_timeout):
                        self._recover_stale_ring_build(candidate, timestamp)
                        state = candidate.get('state')
                    else:
                        return None
                if state not in READY_RING_BUILD_STATES:
                    candidate.update({
                        'state': 'failed',
                        'completed_at': timestamp,
                        'error': 'unsupported persisted ring build state',
                    })
                    self._save_dir_object('ring_builds', candidate)
                    continue
                if not self._ring_build_is_current(candidate):
                    candidate.update({
                        'state': 'failed',
                        'completed_at': timestamp,
                        'error': 'persisted ring build lacks a durable '
                                 'request or sequence',
                    })
                    self._save_dir_object('ring_builds', candidate)
                    continue
                candidate_scope = self._ring_build_scope(candidate)
                blocked = any(self._ring_build_scopes_overlap(
                    candidate_scope, scope) for scope in unclaimable_scopes)
                if (not self._ring_build_is_ready(candidate, timestamp) or
                        blocked):
                    unclaimable_scopes.append(candidate_scope)
                    if (build_id is not None and
                            str(candidate.get('id')) == str(build_id)):
                        return None
                    continue
                if (build_id is not None and
                        str(candidate.get('id')) != str(build_id)):
                    return None
                candidate.update({
                    'state': 'building',
                    'builder_id': builder_id,
                    'claimed_at': timestamp,
                    'started_at': timestamp,
                    'updated_at': timestamp,
                    'lease_expires_at': self._ring_build_lease_expires_at(
                        timestamp, lease_timeout),
                    'lease_timeout': lease_timeout,
                    'attempts': int(candidate.get('attempts', 0)) + 1,
                })
                for key in ('deferred_at', 'deferred_until', 'defer_reason',
                            'deferred_ring_id', 'error'):
                    candidate.pop(key, None)
                self._save_dir_object('ring_builds', candidate)
                return self._public_ring_build(candidate)
        return None

    def refresh_ring_build_lease(self, build_id, builder_id, claimed_at,
                                 timestamp, lease_timeout=None):
        directory = self._collection_dir('ring_builds')
        if directory is None:
            raise ValueError(
                'ring_manager_state_dir is required for build requests')
        timestamp = normal_timestamp_internal(timestamp)
        try:
            claimed_at = normal_timestamp_internal(claimed_at)
        except (TypeError, ValueError):
            return None
        lease_timeout = (DEFAULT_BUILD_JOB_LEASE_TIMEOUT
                         if lease_timeout is None else float(lease_timeout))
        with lock_path(directory, name='ring-build-queue'):
            build = self._load_dir_object(
                'ring_builds', build_id, RingBuildNotFound)
            if (build.get('state') != 'building' or
                    str(build.get('builder_id')) != str(builder_id) or
                    str(build.get('claimed_at')) != str(claimed_at) or
                    self._ring_build_lease_expired(build, timestamp)):
                return None
            return self._update_ring_build(build_id, {
                'lease_refreshed_at': timestamp,
                'lease_expires_at': self._ring_build_lease_expires_at(
                    timestamp, lease_timeout),
                'lease_timeout': lease_timeout,
            }, timestamp)

    def ring_build_queue_stats(self, timestamp=None, lease_timeout=None):
        timestamp = normal_timestamp_internal(timestamp)
        states = {}
        stale_building = 0
        stale_recovered = 0
        builds = self._list_dir_objects('ring_builds')
        for build in builds:
            state = str(build.get('state', 'unknown'))
            states[state] = states.get(state, 0) + 1
            if self._ring_build_lease_expired(
                    build, timestamp, lease_timeout=lease_timeout):
                stale_building += 1
            try:
                stale_recovered += int(build.get('stale_recovered_count', 0))
            except (TypeError, ValueError):
                continue
        return {
            'total': len(builds),
            'states': states,
            'stale_building': stale_building,
            'stale_recovered': stale_recovered,
        }

    def _manifest_retention_sets(self, versions, latest_id, retention_age,
                                 retain_versions, cutoff):
        newest = []
        for version in versions:
            newest.append((
                self._timestamp_float(version),
                self._ring_version_id(version),
            ))
        newest.sort(key=lambda item: (
            item[0] is None, -(item[0] or 0), item[1]))
        retained_newest = set()
        if retain_versions is not None:
            for _timestamp, version_id in newest[:retain_versions]:
                retained_newest.add(version_id)

        protected = set()
        candidates = set()
        reasons = {}
        no_policy = retention_age is None and retain_versions is None
        for version in versions:
            version_id = self._ring_version_id(version)
            if latest_id is not None and str(version_id) == str(latest_id):
                protected.add(version_id)
                _add_reason(reasons, version_id, 'latest')
            if retain_versions is not None and version_id in retained_newest:
                protected.add(version_id)
                _add_reason(reasons, version_id, 'retained_newest')
            if retention_age is not None:
                timestamp = self._timestamp_float(version)
                if timestamp is None:
                    protected.add(version_id)
                    _add_reason(reasons, version_id, 'missing_timestamp')
                elif timestamp > cutoff:
                    protected.add(version_id)
                    _add_reason(reasons, version_id, 'recent')
                else:
                    _add_reason(reasons, version_id, 'older_than_retention')
            if no_policy:
                protected.add(version_id)
                _add_reason(reasons, version_id, 'no_retention_policy')
            if version_id not in protected:
                candidates.add(version_id)
                if retain_versions is not None:
                    _add_reason(reasons, version_id,
                                'outside_retained_newest')
                if not reasons.get(version_id):
                    _add_reason(reasons, version_id, 'unprotected')
        return protected, candidates, reasons

    def _active_build_artifact_namespaces(self, builds):
        namespaces = {}
        unknown_namespaces = []
        unknown_states = []
        for build in builds:
            state = build.get('state')
            if state in TERMINAL_RING_BUILD_STATES:
                continue
            if state not in ACTIVE_RING_BUILD_STATES:
                unknown_states.append({
                    'id': str(build.get('id', '')),
                    'state': None if state is None else str(state),
                })
                continue
            request = build.get('request') or {}
            namespace = None
            if config_true_value(str(request.get('artifact_only', 'false'))):
                namespace = request.get('artifact_namespace')
            else:
                namespace = request.get('version') or build.get('version')
            if namespace in (None, ''):
                unknown_namespaces.append(str(build.get('id', '')))
                continue
            namespaces.setdefault(str(namespace), [])
            namespaces[str(namespace)].append(str(build.get('id', '')))
        return namespaces, unknown_namespaces, unknown_states

    def _artifact_files(self, warnings):
        root = self.ring_artifact_dir
        if not root:
            warnings.append({
                'type': 'missing_artifact_dir',
                'error': 'ring_artifact_dir is required',
            })
            return []

        def onerror(err):
            path = getattr(err, 'filename', None)
            warning_type = 'artifact_scan_failed'
            if err.errno == errno.ENOENT and path is not None:
                if os.path.realpath(path) == os.path.realpath(root):
                    warning_type = 'missing_artifact_dir'
            warning = {
                'type': warning_type,
                'error': str(err),
            }
            if path:
                relpath = self._artifact_relpath(path)
                warning['path'] = relpath if relpath is not None else path
            warnings.append(warning)
        try:
            names = list(os.walk(root, onerror=onerror))
        except OSError as err:
            if err.errno == errno.ENOENT:
                return []
            raise
        files = []
        for directory, _dirs, filenames in names:
            for filename in sorted(filenames):
                path = os.path.join(directory, filename)
                relpath = self._artifact_relpath(path)
                if relpath is None:
                    continue
                try:
                    stat_result = os.stat(path)
                except OSError as err:
                    warnings.append({
                        'type': 'artifact_stat_failed',
                        'path': relpath,
                        'error': str(err),
                    })
                    files.append({
                        'path': relpath,
                        'bytes': None,
                        'mtime': None,
                    })
                    continue
                files.append({
                    'path': relpath,
                    'bytes': stat_result.st_size,
                    'mtime': stat_result.st_mtime,
                })
        files.sort(key=lambda item: item['path'])
        return files

    def _record_artifact_file_reference(self, version, file_info, reasons,
                                        warnings, owner_type, owner_id,
                                        reason):
        relpath = self._artifact_file_relpath(
            version, file_info, warnings, owner_type, owner_id)
        if relpath is None:
            return
        _add_reason(reasons, relpath, reason)

    def _artifact_cleanup_blockers(self, warnings, active_namespaces,
                                   active_unknown_namespaces,
                                   unknown_state_builds):
        blockers = []
        for warning in warnings:
            warning_type = warning.get('type')
            if warning_type not in CLEANUP_BLOCKING_WARNING_TYPES:
                continue
            blocker = copy.deepcopy(warning)
            blocker['source'] = 'warning'
            blockers.append(blocker)
        for namespace, build_ids in sorted(active_namespaces.items()):
            for build_id in sorted(build_ids):
                blockers.append({
                    'type': 'active_build_namespace',
                    'source': 'active_build',
                    'namespace': namespace,
                    'build_id': build_id,
                })
        for build_id in active_unknown_namespaces:
            blockers.append({
                'type': 'active_build_unknown_namespace',
                'source': 'active_build',
                'build_id': build_id,
            })
        for build in unknown_state_builds:
            blockers.append({
                'type': 'unknown_ring_build_state',
                'source': 'ring_build',
                'build_id': build.get('id'),
                'state': build.get('state'),
            })
        return blockers

    def _tombstoned_live_record_warnings(self):
        warnings = []
        for version in self._ring_versions(include_tombstoned=True):
            version_id = self._ring_version_id(version)
            if self.ring_version_tombstoned(version_id):
                warnings.append({
                    'type': 'tombstoned_manifest_record',
                    'version': version_id,
                    'action': 'treat_record_as_deleted',
                })
        for version in self._all_ring_artifact_versions(
                include_tombstoned=True):
            ring_id = version.get('ring_id')
            if ring_id in (None, ''):
                continue
            version_id = self._ring_artifact_version_id(version)
            if self.ring_artifact_version_tombstoned(ring_id, version_id):
                warnings.append({
                    'type': 'tombstoned_ring_artifact_version_record',
                    'ring_id': str(ring_id),
                    'version': version_id,
                    'action': 'treat_record_as_deleted',
                })
        return warnings

    def plan_artifact_cleanup(self, retention_age=None, retain_versions=None,
                              timestamp=None, include_details=True):
        """
        Build a dry-run published-state retention graph.

        This only reports protected and candidate manifests, per-ring artifact
        version records, and artifact files. It does not create tombstones or
        delete any state.
        """
        if retention_age is not None:
            retention_age = float(retention_age)
            if not math.isfinite(retention_age) or retention_age < 0:
                raise ValueError(
                    'retention_age must be a finite non-negative number')
        if retain_versions is not None:
            retain_versions = int(retain_versions)
            if retain_versions < 0:
                raise ValueError('retain_versions must be non-negative')
        timestamp = normal_timestamp_internal(timestamp)
        now = normal_timestamp_float(timestamp)
        cutoff = None if retention_age is None else now - retention_age
        warnings = self._tombstoned_live_record_warnings()
        versions = self._ring_versions()
        try:
            latest_id = self.get_latest_ring_version_id()
        except RingVersionNotFound:
            latest_id = None
        version_ids = set(self._ring_version_id(version)
                          for version in versions)
        protected_manifests, candidate_manifests, manifest_reasons = \
            self._manifest_retention_sets(
                versions, latest_id, retention_age, retain_versions, cutoff)
        if (latest_id is None and version_ids) or (
                latest_id is not None and str(latest_id) not in version_ids):
            warnings.append({
                'type': 'missing_latest_ring_version',
                'version': None if latest_id is None else str(latest_id),
                'action': 'protected_all_manifests',
            })
            for version_id in version_ids:
                protected_manifests.add(version_id)
                candidate_manifests.discard(version_id)
                _add_reason(
                    manifest_reasons, version_id,
                    'missing_latest_ring_version_fail_closed')

        protected_records = {}
        file_reasons = {}
        for version in versions:
            version_id = self._ring_version_id(version)
            if version_id not in protected_manifests:
                continue
            for file_info in version.get('files', []):
                self._record_artifact_file_reference(
                    version, file_info, file_reasons, warnings,
                    'manifest', version_id,
                    'manifest:%s' % version_id)
            for ring in version.get('rings', []):
                if not isinstance(ring, dict):
                    warnings.append({
                        'type': 'invalid_manifest_ring',
                        'manifest': version_id,
                        'ring': copy.deepcopy(ring),
                    })
                    continue
                ring_id = ring.get('ring_id')
                swift_ring_version = ring.get(
                    'swift_ring_version', ring.get('version'))
                if ring_id in (None, '') or swift_ring_version in (None, ''):
                    warnings.append({
                        'type': 'invalid_manifest_ring',
                        'manifest': version_id,
                        'ring': copy.deepcopy(ring),
                    })
                    continue
                key = (str(ring_id), str(swift_ring_version))
                _add_reason(
                    protected_records, key,
                    'manifest:%s' % version_id)

        rings = self._list_dir_objects('rings')
        for ring in rings:
            ring_id = ring.get('id')
            if ring_id in (None, ''):
                continue
            try:
                version_id = self._latest_ring_artifact_version_id_for_ring(
                    ring)
            except RingVersionNotFound:
                warnings.append({
                    'type': 'missing_latest_ring_artifact',
                    'ring_id': str(ring_id),
                })
                continue
            if version_id is None:
                for key_name in ('latest_swift_ring_version',
                                 'latest_version'):
                    if ring.get(key_name) is not None:
                        warnings.append({
                            'type': 'missing_latest_ring_artifact',
                            'ring_id': str(ring_id),
                            'version': str(ring[key_name]),
                            'field': key_name,
                        })
                        break
                continue
            key = (str(ring_id), str(version_id))
            _add_reason(protected_records, key,
                        'ring_latest:%s' % ring_id)

        builds = self._list_dir_objects('ring_builds')
        active_namespaces, active_unknown_namespaces, unknown_state_builds = \
            self._active_build_artifact_namespaces(builds)

        record_rows = []
        ring_ids = set(str(ring.get('id')) for ring in rings
                       if ring.get('id') not in (None, ''))
        all_records = {}
        for version in self._all_ring_artifact_versions():
            ring_id = version.get('ring_id')
            if ring_id in (None, ''):
                warnings.append({
                    'type': 'missing_ring_id',
                    'version': self._ring_artifact_version_id(version),
                })
                continue
            version_id = self._ring_artifact_version_id(version)
            key = (str(ring_id), str(version_id))
            if version.get('_ring_id_mismatch') is not None:
                warnings.append({
                    'type': 'ring_artifact_version_ring_id_mismatch',
                    'ring_id': str(ring_id),
                    'version': str(version_id),
                    'record_ring_id': version['_ring_id_mismatch'],
                })
            if str(ring_id) not in ring_ids:
                version.setdefault('_missing_ring_metadata', True)
            all_records[key] = version
        for key, reasons in sorted(protected_records.items()):
            if key not in all_records:
                ring_id, version_id = key
                warnings.append({
                    'type': 'missing_ring_artifact_version',
                    'ring_id': ring_id,
                    'version': version_id,
                    'reasons': reasons,
                })
        for key, version in sorted(all_records.items()):
            ring_id, version_id = key
            reasons = protected_records.get(key, [])
            candidate = False
            if not reasons:
                for build_id in active_unknown_namespaces:
                    reason = 'active_build_unknown_namespace:%s' % build_id
                    if reason not in reasons:
                        reasons.append(reason)
                if reasons:
                    pass
                elif retention_age is None:
                    reasons = ['no_retention_policy']
                else:
                    version_timestamp = self._timestamp_float(version)
                    if version_timestamp is None:
                        reasons = ['missing_timestamp']
                    elif version_timestamp > cutoff:
                        reasons = ['recent']
                    else:
                        candidate = True
                        reasons = ['unreferenced', 'older_than_retention']
            if reasons and not candidate:
                for file_info in version.get('files', []):
                    self._record_artifact_file_reference(
                        version, file_info, file_reasons, warnings,
                        'ring_artifact_version',
                        '%s/%s' % (ring_id, version_id),
                        'ring_artifact_version:%s/%s' % (
                            ring_id, version_id))
            record_rows.append({
                'ring_id': ring_id,
                'version': version_id,
                'candidate': candidate,
                'protected': not candidate,
                'reasons': reasons,
                'created_at': version.get('created_at'),
                'files': len(version.get('files', [])),
                'missing_ring_metadata': bool(
                    version.get('_missing_ring_metadata')),
            })

        artifact_file_rows = []
        artifact_files = self._artifact_files(warnings)
        artifact_file_paths = set(item['path'] for item in artifact_files)
        for relpath, reasons in sorted(file_reasons.items()):
            if relpath not in artifact_file_paths:
                warnings.append({
                    'type': 'missing_artifact_file',
                    'path': relpath,
                    'reasons': reasons,
                })
        for file_info in artifact_files:
            relpath = file_info['path']
            reasons = list(file_reasons.get(relpath, []))
            for namespace, build_ids in active_namespaces.items():
                prefix = namespace.rstrip('/') + '/'
                if relpath == namespace or relpath.startswith(prefix):
                    for build_id in build_ids:
                        reason = 'active_build:%s' % build_id
                        if reason not in reasons:
                            reasons.append(reason)
            candidate = False
            if not reasons:
                for build_id in active_unknown_namespaces:
                    reason = 'active_build_unknown_namespace:%s' % build_id
                    if reason not in reasons:
                        reasons.append(reason)
            if not reasons:
                if retention_age is None:
                    reasons = ['no_retention_policy']
                elif file_info.get('mtime') is None:
                    reasons = ['missing_timestamp']
                elif file_info['mtime'] > cutoff:
                    reasons = ['recent']
                else:
                    candidate = True
                    reasons = ['unreferenced', 'older_than_retention']
            artifact_file_rows.append({
                'path': relpath,
                'candidate': candidate,
                'protected': not candidate,
                'reasons': reasons,
                'bytes': file_info.get('bytes'),
            })

        manifest_rows = []
        for version in versions:
            version_id = self._ring_version_id(version)
            candidate = version_id in candidate_manifests
            manifest_rows.append({
                'version': version_id,
                'candidate': candidate,
                'protected': not candidate,
                'reasons': manifest_reasons.get(version_id, []),
                'created_at': version.get('created_at'),
                'files': len(version.get('files', [])),
                'rings': len(version.get('rings', [])),
                'latest': latest_id is not None and
                str(version_id) == str(latest_id),
                'resource_uri': self._ring_version_uri(version),
            })
        manifest_rows.sort(key=lambda item: item['version'])

        def count_candidates(rows):
            return len([item for item in rows if item.get('candidate')])

        candidate_bytes = sum(
            item.get('bytes') or 0 for item in artifact_file_rows
            if item.get('candidate'))
        cleanup_blockers = self._artifact_cleanup_blockers(
            warnings, active_namespaces, active_unknown_namespaces,
            unknown_state_builds)
        plan = {
            'dry_run': True,
            'delete_allowed': False,
            'cleanup_safe': not cleanup_blockers,
            'cleanup_blockers': cleanup_blockers,
            'generated_at': timestamp,
            'retention_age': retention_age,
            'retain_versions': retain_versions,
            'latest_ring_version': latest_id,
            'active_build_namespaces': active_namespaces,
            'active_builds_with_unknown_namespaces':
                active_unknown_namespaces,
            'ring_builds_with_unknown_states': unknown_state_builds,
            'summary': {
                'manifests': {
                    'total': len(manifest_rows),
                    'protected': len(manifest_rows) -
                    count_candidates(manifest_rows),
                    'candidates': count_candidates(manifest_rows),
                },
                'ring_artifact_versions': {
                    'total': len(record_rows),
                    'protected': len(record_rows) -
                    count_candidates(record_rows),
                    'candidates': count_candidates(record_rows),
                },
                'artifact_files': {
                    'total': len(artifact_file_rows),
                    'protected': len(artifact_file_rows) -
                    count_candidates(artifact_file_rows),
                    'candidates': count_candidates(artifact_file_rows),
                    'candidate_bytes': candidate_bytes,
                },
                'warnings': len(warnings),
            },
            'warnings': warnings,
            'requires_tombstones': bool(
                count_candidates(manifest_rows) or count_candidates(
                    record_rows)),
        }
        if include_details:
            plan.update({
                'manifests': manifest_rows,
                'ring_artifact_versions': record_rows,
                'artifact_files': artifact_file_rows,
            })
        return plan

    def _artifact_metadata_cleanup_result(self, plan, timestamp):
        def candidate_rows(key):
            return [row for row in plan.get(key) or []
                    if row.get('candidate')]

        def metadata_summary(rows):
            return {
                'candidates': len(rows),
                'tombstones_written': 0,
                'already_tombstoned': 0,
                'records_deleted': 0,
                'already_deleted': 0,
                'skipped': 0,
            }

        artifact_file_summary = (
            plan.get('summary', {}).get('artifact_files', {}))
        artifact_files = candidate_rows('artifact_files')
        return {
            'dry_run': False,
            'delete_allowed': False,
            'metadata_only': True,
            'cleanup_safe': plan.get('cleanup_safe'),
            'cleanup_blockers': copy.deepcopy(
                plan.get('cleanup_blockers') or []),
            'generated_at': timestamp,
            'started_at': timestamp,
            'completed_at': None,
            'retention_age': plan.get('retention_age'),
            'retain_versions': plan.get('retain_versions'),
            'latest_ring_version': plan.get('latest_ring_version'),
            'summary': {
                'manifests': metadata_summary(
                    candidate_rows('manifests')),
                'ring_artifact_versions': metadata_summary(
                    candidate_rows('ring_artifact_versions')),
                'artifact_files': {
                    'candidates': len(artifact_files),
                    'left_untouched': len(artifact_files),
                    'candidate_bytes': artifact_file_summary.get(
                        'candidate_bytes', 0),
                },
                'repairs': {
                    'tombstoned_live_records': 0,
                },
                'warnings': len(plan.get('warnings') or []),
            },
            'warnings': copy.deepcopy(plan.get('warnings') or []),
            'manifests': [],
            'ring_artifact_versions': [],
            'artifact_files': [
                {
                    'path': row.get('path'),
                    'bytes': row.get('bytes'),
                    'reasons': copy.deepcopy(row.get('reasons') or []),
                    'action': 'left_untouched',
                } for row in artifact_files
            ],
            'repairs': [],
            'errors': [],
            'skipped': [],
        }

    def _artifact_metadata_cleanup_status(self, result):
        status = copy.deepcopy(result)
        for key in ('manifests', 'ring_artifact_versions',
                    'artifact_files', 'warnings', 'repairs', 'skipped'):
            status.pop(key, None)
        status['details_omitted'] = True
        return status

    def _record_artifact_metadata_cleanup(self, result):
        status = self._artifact_metadata_cleanup_status(result)

        def mutate(index):
            index['artifact_cleanup_metadata'] = status
            return True, None
        self._mutate_state_index(mutate)

    def _artifact_file_cleanup_result(self, plan, timestamp):
        artifact_file_summary = (
            plan.get('summary', {}).get('artifact_files', {}))
        artifact_files = [
            row for row in plan.get('artifact_files') or []
            if row.get('candidate')]
        return {
            'dry_run': False,
            'delete_allowed': False,
            'artifact_files_only': True,
            'cleanup_safe': plan.get('cleanup_safe'),
            'cleanup_blockers': copy.deepcopy(
                plan.get('cleanup_blockers') or []),
            'generated_at': timestamp,
            'started_at': timestamp,
            'completed_at': None,
            'retention_age': plan.get('retention_age'),
            'retain_versions': plan.get('retain_versions'),
            'latest_ring_version': plan.get('latest_ring_version'),
            'summary': {
                'artifact_files': {
                    'candidates': len(artifact_files),
                    'files_deleted': 0,
                    'already_deleted': 0,
                    'bytes_deleted': 0,
                    'candidate_bytes': artifact_file_summary.get(
                        'candidate_bytes', 0),
                    'skipped': 0,
                },
                'warnings': len(plan.get('warnings') or []),
            },
            'warnings': copy.deepcopy(plan.get('warnings') or []),
            'artifact_files': [],
            'errors': [],
            'skipped': [],
        }

    def _artifact_file_cleanup_status(self, result):
        status = copy.deepcopy(result)
        for key in ('artifact_files', 'warnings', 'skipped'):
            status.pop(key, None)
        status['details_omitted'] = True
        return status

    def _record_artifact_file_cleanup(self, result):
        status = self._artifact_file_cleanup_status(result)

        def mutate(index):
            index['artifact_cleanup_files'] = status
            return True, None
        self._mutate_state_index(mutate)

    def _record_cleanup_skip(self, result, section, entry, reason):
        entry['record'] = 'skipped'
        entry['reason'] = reason
        result[section].append(entry)
        result['summary'][section]['skipped'] += 1
        result['skipped'].append(copy.deepcopy(entry))

    def _metadata_cleanup_error(self, result, section, entry, err):
        entry['error'] = str(err)
        result[section].append(entry)
        result['summary'][section]['skipped'] += 1
        result['errors'].append(copy.deepcopy(entry))

    def _cleanup_manifest_metadata(self, row, version_paths, result,
                                   timestamp, deleted_by, reason):
        version_id = row.get('version')
        entry = {
            'version': version_id,
            'reasons': copy.deepcopy(row.get('reasons') or []),
        }
        if version_id in (None, ''):
            self._record_cleanup_skip(
                result, 'manifests', entry, 'missing_version')
            return
        version_id = str(version_id)
        entry['version'] = version_id

        tombstoned = self.ring_version_tombstoned(version_id)
        if tombstoned:
            entry['tombstone'] = 'already_exists'
            result['summary']['manifests']['already_tombstoned'] += 1
        else:
            try:
                self.create_ring_version_tombstone(
                    version_id, timestamp=timestamp, deleted_by=deleted_by,
                    reason=reason)
            except Exception as err:
                entry['tombstone'] = 'failed'
                self._metadata_cleanup_error(
                    result, 'manifests', entry, err)
                return
            entry['tombstone'] = 'written'
            result['summary']['manifests']['tombstones_written'] += 1

        path = version_paths.get(version_id)
        try:
            if path is None:
                entry['record'] = 'already_deleted'
                result['summary']['manifests']['already_deleted'] += 1
            elif self._delete_state_file(path, missing_ok=True):
                entry['record'] = 'deleted'
                result['summary']['manifests']['records_deleted'] += 1
            else:
                entry['record'] = 'already_deleted'
                result['summary']['manifests']['already_deleted'] += 1
        except Exception as err:
            entry['record'] = 'delete_failed'
            self._metadata_cleanup_error(result, 'manifests', entry, err)
            return
        result['manifests'].append(entry)

    def _cleanup_ring_artifact_version_metadata(
            self, row, record_paths, result, timestamp, deleted_by, reason):
        ring_id = row.get('ring_id')
        version_id = row.get('version')
        entry = {
            'ring_id': ring_id,
            'version': version_id,
            'reasons': copy.deepcopy(row.get('reasons') or []),
        }
        if ring_id in (None, '') or version_id in (None, ''):
            self._record_cleanup_skip(
                result, 'ring_artifact_versions', entry,
                'missing_ring_id_or_version')
            return
        ring_id = str(ring_id)
        version_id = str(version_id)
        entry['ring_id'] = ring_id
        entry['version'] = version_id

        tombstoned = self.ring_artifact_version_tombstoned(
            ring_id, version_id)
        if tombstoned:
            entry['tombstone'] = 'already_exists'
            result['summary']['ring_artifact_versions'][
                'already_tombstoned'] += 1
        else:
            try:
                self.create_ring_artifact_version_tombstone(
                    ring_id, version_id, timestamp=timestamp,
                    deleted_by=deleted_by, reason=reason)
            except Exception as err:
                entry['tombstone'] = 'failed'
                self._metadata_cleanup_error(
                    result, 'ring_artifact_versions', entry, err)
                return
            entry['tombstone'] = 'written'
            result['summary']['ring_artifact_versions'][
                'tombstones_written'] += 1

        path = record_paths.get((ring_id, version_id))
        try:
            if path is None:
                entry['record'] = 'already_deleted'
                result['summary']['ring_artifact_versions'][
                    'already_deleted'] += 1
            elif self._delete_state_file(path, missing_ok=True):
                entry['record'] = 'deleted'
                result['summary']['ring_artifact_versions'][
                    'records_deleted'] += 1
            else:
                entry['record'] = 'already_deleted'
                result['summary']['ring_artifact_versions'][
                    'already_deleted'] += 1
        except Exception as err:
            entry['record'] = 'delete_failed'
            self._metadata_cleanup_error(
                result, 'ring_artifact_versions', entry, err)
            return
        result['ring_artifact_versions'].append(entry)

    def _active_build_cleanup_plan(self, builds, timestamp, retention_age,
                                   retain_versions):
        active_namespaces, active_unknown_namespaces, unknown_state_builds = \
            self._active_build_artifact_namespaces(builds)
        cleanup_blockers = self._artifact_cleanup_blockers(
            [], active_namespaces, active_unknown_namespaces,
            unknown_state_builds)
        if not cleanup_blockers:
            return None
        try:
            latest_id = self.get_latest_ring_version_id()
        except RingVersionNotFound:
            latest_id = None
        return {
            'dry_run': True,
            'delete_allowed': False,
            'cleanup_safe': False,
            'cleanup_blockers': cleanup_blockers,
            'generated_at': timestamp,
            'retention_age': retention_age,
            'retain_versions': retain_versions,
            'latest_ring_version': latest_id,
            'active_build_namespaces': active_namespaces,
            'active_builds_with_unknown_namespaces':
                active_unknown_namespaces,
            'ring_builds_with_unknown_states': unknown_state_builds,
            'summary': {
                'artifact_files': {
                    'candidate_bytes': 0,
                },
            },
            'warnings': [],
            'manifests': [],
            'ring_artifact_versions': [],
            'artifact_files': [],
        }

    def _active_build_cleanup_result(self, builds, timestamp, retention_age,
                                     retain_versions):
        plan = self._active_build_cleanup_plan(
            builds, timestamp, retention_age, retain_versions)
        if plan is None:
            return None
        return self._artifact_metadata_cleanup_result(plan, timestamp)

    def _repairable_cleanup_blockers(self, blockers):
        return blockers and all(
            blocker.get('type') in REPAIRABLE_CLEANUP_WARNING_TYPES
            for blocker in blockers)

    def _tombstoned_live_record_paths(self):
        manifests = {}
        for version in self._ring_versions(include_tombstoned=True):
            version_id = self._ring_version_id(version)
            if self.ring_version_tombstoned(version_id):
                manifests[version_id] = version.get('_manifest_path')
        ring_versions = {}
        for version in self._all_ring_artifact_versions(
                include_tombstoned=True):
            ring_id = version.get('ring_id')
            if ring_id in (None, ''):
                continue
            version_id = self._ring_artifact_version_id(version)
            if self.ring_artifact_version_tombstoned(ring_id, version_id):
                ring_versions[(str(ring_id), version_id)] = \
                    version.get('_record_path')
        return manifests, ring_versions

    def _repair_tombstoned_live_records(self, blockers, result):
        manifest_paths, ring_version_paths = \
            self._tombstoned_live_record_paths()
        for blocker in blockers:
            blocker_type = blocker.get('type')
            repair = copy.deepcopy(blocker)
            try:
                if blocker_type == 'tombstoned_manifest_record':
                    version_id = str(blocker.get('version'))
                    path = manifest_paths.get(version_id)
                    if path is None:
                        repair['record'] = 'already_deleted'
                    elif self._delete_state_file(path, missing_ok=True):
                        repair['record'] = 'deleted'
                    else:
                        repair['record'] = 'already_deleted'
                elif blocker_type == 'tombstoned_ring_artifact_version_record':
                    ring_id = str(blocker.get('ring_id'))
                    version_id = str(blocker.get('version'))
                    path = ring_version_paths.get((ring_id, version_id))
                    if path is None:
                        repair['record'] = 'already_deleted'
                    elif self._delete_state_file(path, missing_ok=True):
                        repair['record'] = 'deleted'
                    else:
                        repair['record'] = 'already_deleted'
                else:
                    repair['record'] = 'skipped'
            except Exception as err:
                repair['record'] = 'delete_failed'
                repair['error'] = str(err)
                result['errors'].append(copy.deepcopy(repair))
                result['repairs'].append(repair)
                return False
            result['repairs'].append(repair)
            result['summary']['repairs']['tombstoned_live_records'] += 1
        return True

    def cleanup_artifact_metadata(self, retention_age=None,
                                  retain_versions=None, timestamp=None,
                                  deleted_by='artifact_cleanup',
                                  reason='retention_policy'):
        """
        Tombstone and delete cleanup-candidate metadata records.

        This intentionally prunes only served JSON metadata:
        ``releases/<id>/manifest.json`` and
        ``ring-versions/<ring-id>/<swift-ring-version>.json``. Ring artifact
        files are left untouched for a later cleanup phase.
        """
        if not self.state_dir:
            raise ValueError(
                'ring_manager_state_dir is required for cleanup requests')
        timestamp = normal_timestamp_internal(timestamp)
        builds_dir = self._collection_dir('ring_builds')
        with lock_path(builds_dir, name='ring-build-queue'):
            with self.published_state_lock():
                builds = self._list_dir_objects('ring_builds')
                active_result = self._active_build_cleanup_result(
                    builds, timestamp, retention_age, retain_versions)
                if active_result is not None:
                    return active_result

                plan = self.plan_artifact_cleanup(
                    retention_age=retention_age,
                    retain_versions=retain_versions,
                    timestamp=timestamp,
                    include_details=True)
                result = self._artifact_metadata_cleanup_result(
                    plan, timestamp)
                if not plan.get('cleanup_safe'):
                    blockers = plan.get('cleanup_blockers') or []
                    if not self._repairable_cleanup_blockers(blockers):
                        return result
                    if not self._repair_tombstoned_live_records(
                            blockers, result):
                        result['completed_at'] = normal_timestamp_internal()
                        self._record_artifact_metadata_cleanup(result)
                        return result
                    plan = self.plan_artifact_cleanup(
                        retention_age=retention_age,
                        retain_versions=retain_versions,
                        timestamp=timestamp,
                        include_details=True)
                    repaired_result = self._artifact_metadata_cleanup_result(
                        plan, timestamp)
                    repaired_result['repairs'] = result['repairs']
                    repaired_result['summary']['repairs'] = \
                        result['summary']['repairs']
                    result = repaired_result
                    if not plan.get('cleanup_safe'):
                        result['completed_at'] = normal_timestamp_internal()
                        self._record_artifact_metadata_cleanup(result)
                        return result

                result['delete_allowed'] = True
                version_paths = dict(
                    (self._ring_version_id(version),
                     version.get('_manifest_path'))
                    for version in self._ring_versions())
                record_paths = dict(
                    ((str(version.get('ring_id')),
                      self._ring_artifact_version_id(version)),
                     version.get('_record_path'))
                    for version in self._all_ring_artifact_versions())

                for row in plan.get('manifests') or []:
                    if row.get('candidate'):
                        self._cleanup_manifest_metadata(
                            row, version_paths, result, timestamp,
                            deleted_by, reason)
                        if result['errors']:
                            break
                for row in plan.get('ring_artifact_versions') or []:
                    if result['errors']:
                        break
                    if row.get('candidate'):
                        self._cleanup_ring_artifact_version_metadata(
                            row, record_paths, result, timestamp,
                            deleted_by, reason)

                result['completed_at'] = normal_timestamp_internal()
                self._record_artifact_metadata_cleanup(result)
                return result

    def artifact_cleanup_metadata_stats(self):
        try:
            cleanup = self._state_index().get(
                'artifact_cleanup_metadata', {})
        except (IOError, ValueError):
            return {}
        if not isinstance(cleanup, dict):
            return {}
        return copy.deepcopy(cleanup)

    def _delete_artifact_file(self, relpath, missing_ok=False):
        root = os.path.realpath(self.ring_artifact_dir)
        relpath = str(relpath)
        if os.path.isabs(relpath):
            path = os.path.abspath(relpath)
        else:
            path = os.path.abspath(os.path.join(root, relpath))
        try:
            common_path = os.path.commonpath([root, path])
        except ValueError:
            common_path = None
        if common_path != root:
            raise ValueError(
                'artifact cleanup path escapes ring_artifact_dir')
        directory = os.path.dirname(path)
        if os.path.realpath(directory) != os.path.abspath(directory):
            raise ValueError(
                'artifact cleanup path contains a symbolic directory')
        try:
            os.unlink(path)
        except OSError as err:
            if err.errno == errno.ENOENT and missing_ok:
                return False
            raise
        if directory:
            fsync_dir(directory)
        return True

    def _record_artifact_file_cleanup_skip(self, result, entry, reason):
        entry['file'] = 'skipped'
        entry['reason'] = reason
        result['artifact_files'].append(entry)
        result['summary']['artifact_files']['skipped'] += 1
        result['skipped'].append(copy.deepcopy(entry))

    def _artifact_file_cleanup_error(self, result, entry, err):
        entry['file'] = 'delete_failed'
        entry['error'] = str(err)
        result['artifact_files'].append(entry)
        result['summary']['artifact_files']['skipped'] += 1
        result['errors'].append(copy.deepcopy(entry))

    def _cleanup_artifact_file(self, row, result):
        relpath = row.get('path')
        entry = {
            'path': relpath,
            'bytes': row.get('bytes'),
            'reasons': copy.deepcopy(row.get('reasons') or []),
        }
        if relpath in (None, ''):
            self._record_artifact_file_cleanup_skip(
                result, entry, 'missing_path')
            return
        relpath = str(relpath)
        entry['path'] = relpath
        try:
            if self._delete_artifact_file(relpath, missing_ok=True):
                entry['file'] = 'deleted'
                result['summary']['artifact_files']['files_deleted'] += 1
                result['summary']['artifact_files']['bytes_deleted'] += (
                    row.get('bytes') or 0)
            else:
                entry['file'] = 'already_deleted'
                result['summary']['artifact_files']['already_deleted'] += 1
        except Exception as err:
            self._artifact_file_cleanup_error(result, entry, err)
            return
        result['artifact_files'].append(entry)

    def _artifact_file_cleanup_metadata_blockers(self, plan):
        manifest_versions = [
            row.get('version') for row in plan.get('manifests') or []
            if row.get('candidate')]
        ring_versions = [
            {
                'ring_id': row.get('ring_id'),
                'version': row.get('version'),
            } for row in plan.get('ring_artifact_versions') or []
            if row.get('candidate')]
        blockers = []
        if manifest_versions:
            blockers.append({
                'type': 'metadata_cleanup_required',
                'source': 'manifest',
                'candidates': len(manifest_versions),
                'versions': manifest_versions[
                    :ARTIFACT_FILE_CLEANUP_BLOCKER_SAMPLE_LIMIT],
                'details_truncated': (
                    len(manifest_versions) >
                    ARTIFACT_FILE_CLEANUP_BLOCKER_SAMPLE_LIMIT),
            })
        if ring_versions:
            blockers.append({
                'type': 'metadata_cleanup_required',
                'source': 'ring_artifact_version',
                'candidates': len(ring_versions),
                'ring_versions': ring_versions[
                    :ARTIFACT_FILE_CLEANUP_BLOCKER_SAMPLE_LIMIT],
                'details_truncated': (
                    len(ring_versions) >
                    ARTIFACT_FILE_CLEANUP_BLOCKER_SAMPLE_LIMIT),
            })
        return blockers

    def cleanup_artifact_files(self, retention_age=None, retain_versions=None,
                               timestamp=None):
        """
        Delete cleanup-candidate ring artifact files.

        This phase only unlinks artifact files from ``ring_artifact_dir``. It
        does not write tombstones, delete manifests, or delete per-ring
        artifact-version JSON metadata.
        """
        if not self.state_dir:
            raise ValueError(
                'ring_manager_state_dir is required for cleanup requests')
        if not self.ring_artifact_dir:
            raise ValueError(
                'ring_artifact_dir is required for cleanup requests')
        timestamp = normal_timestamp_internal(timestamp)
        builds_dir = self._collection_dir('ring_builds')
        with lock_path(builds_dir, name='ring-build-queue'):
            with self.published_state_lock():
                builds = self._list_dir_objects('ring_builds')
                active_plan = self._active_build_cleanup_plan(
                    builds, timestamp, retention_age, retain_versions)
                if active_plan is not None:
                    return self._artifact_file_cleanup_result(
                        active_plan, timestamp)

                plan = self.plan_artifact_cleanup(
                    retention_age=retention_age,
                    retain_versions=retain_versions,
                    timestamp=timestamp,
                    include_details=True)
                result = self._artifact_file_cleanup_result(plan, timestamp)
                if not plan.get('cleanup_safe'):
                    return result
                metadata_blockers = \
                    self._artifact_file_cleanup_metadata_blockers(plan)
                if metadata_blockers:
                    result['cleanup_safe'] = False
                    result['cleanup_blockers'] = metadata_blockers
                    return result

                result['delete_allowed'] = True
                for row in plan.get('artifact_files') or []:
                    if row.get('candidate'):
                        self._cleanup_artifact_file(row, result)
                        if result['errors']:
                            break

                result['completed_at'] = normal_timestamp_internal()
                self._record_artifact_file_cleanup(result)
                return result

    def artifact_cleanup_file_stats(self):
        try:
            cleanup = self._state_index().get(
                'artifact_cleanup_files', {})
        except (IOError, ValueError):
            return {}
        if not isinstance(cleanup, dict):
            return {}
        return copy.deepcopy(cleanup)

    def _ring_artifact_version_id(self, version):
        return str(version.get(
            'swift_ring_version', version.get('version', version.get('id'))))

    def _ring_artifact_version_uri(self, ring_id, version):
        return '/api/v1/rings/%s/versions/%s/' % (
            self._safe_id(ring_id),
            self._safe_id(self._ring_artifact_version_id(version)))

    def _ring_artifact_version_file_url(self, ring_id, version, file_info):
        return '%sfiles/%s' % (
            self._ring_artifact_version_uri(ring_id, version),
            self._safe_id(file_info['name']))

    def _public_ring_artifact_version_file(self, ring_id, version,
                                           file_info):
        public_file = copy.deepcopy(file_info)
        validate_path_component(public_file.get('name'), 'artifact file name')
        for key in ('path', 'artifact_dir', '_artifact_root'):
            public_file.pop(key, None)
        public_file['url'] = self._ring_artifact_version_file_url(
            ring_id, version, file_info)
        return public_file

    def _public_ring_artifact_version(self, ring_id, version, latest=False):
        public_version = copy.deepcopy(version)
        for key in ('_artifact_root', '_record_path', 'artifact_dir'):
            public_version.pop(key, None)
        public_version['ring_id'] = ring_id
        public_version['version'] = self._ring_artifact_version_id(version)
        public_version.setdefault(
            'swift_ring_version', public_version['version'])
        public_version['resource_uri'] = self._ring_artifact_version_uri(
            ring_id, version)
        public_version['latest'] = latest or bool(version.get('latest'))
        public_version['files'] = [
            self._public_ring_artifact_version_file(
                ring_id, version, file_info)
            for file_info in version.get('files', [])]
        return public_version

    def _ring_artifact_versions_dir(self, ring_id):
        return self._state_dir_path('ring-versions', self._safe_id(ring_id))

    def _ring_artifact_version_file(self, ring_id, version_id):
        directory = self._ring_artifact_versions_dir(ring_id)
        if directory is None:
            return None
        return os.path.join(directory, '%s.json' % self._safe_id(version_id))

    def _ring_artifact_versions(self, ring_id, include_tombstoned=False):
        directory = self._ring_artifact_versions_dir(ring_id)
        if directory is None:
            return []
        try:
            names = sorted(os.listdir(directory))
        except OSError as err:
            if err.errno == errno.ENOENT:
                return []
            raise
        versions = []
        for name in names:
            if not name.endswith('.json'):
                continue
            path = os.path.join(directory, name)
            version = self._read_json_file(path)
            if version is None:
                continue
            if not isinstance(version, dict):
                raise ValueError('%s must be a JSON object' % path)
            version.setdefault('ring_id', ring_id)
            if str(version['ring_id']) != str(ring_id):
                raise ValueError(
                    'ring artifact version ring_id %r does not match %r' % (
                        version['ring_id'], ring_id))
            version_id = validate_artifact_version_id(
                self._ring_artifact_version_id(version),
                'ring artifact version')
            path_id = unquote(name[:-5])
            if version_id != path_id:
                raise ValueError(
                    'ring artifact version %r does not match state path %r' %
                    (version_id, path_id))
            if (not include_tombstoned and
                    self.ring_artifact_version_tombstoned(
                        ring_id, version_id)):
                continue
            version['_record_path'] = path
            version['_artifact_root'] = directory
            versions.append(version)
        return versions

    def _all_ring_artifact_versions(self, include_tombstoned=False):
        root = self._state_dir_path('ring-versions')
        if root is None:
            return []
        try:
            ring_dirs = sorted(os.listdir(root))
        except OSError as err:
            if err.errno == errno.ENOENT:
                return []
            raise
        versions = []
        for ring_dir in ring_dirs:
            directory = os.path.join(root, ring_dir)
            if not os.path.isdir(directory):
                continue
            try:
                names = sorted(os.listdir(directory))
            except OSError as err:
                if err.errno == errno.ENOENT:
                    continue
                raise
            for name in names:
                if not name.endswith('.json'):
                    continue
                path = os.path.join(directory, name)
                version = self._read_json_file(path)
                if version is None:
                    continue
                if not isinstance(version, dict):
                    raise ValueError('%s must be a JSON object' % path)
                ring_id = unquote(ring_dir)
                if (version.get('ring_id') is not None and
                        str(version.get('ring_id')) != str(ring_id)):
                    version['_ring_id_mismatch'] = str(version.get('ring_id'))
                version['ring_id'] = ring_id
                version_id = validate_artifact_version_id(
                    self._ring_artifact_version_id(version),
                    'ring artifact version')
                path_id = unquote(name[:-5])
                if version_id != path_id:
                    raise ValueError(
                        'ring artifact version %r does not match state path '
                        '%r' % (version_id, path_id))
                if (not include_tombstoned and
                        self.ring_artifact_version_tombstoned(
                            ring_id, version_id)):
                    continue
                version['_record_path'] = path
                version['_artifact_root'] = directory
                versions.append(version)
        return versions

    def _find_ring_artifact_version(self, ring_id, version_id):
        if version_id == 'latest':
            return self._find_latest_ring_artifact_version(ring_id)
        version_id = str(version_id)
        if self.ring_artifact_version_tombstoned(ring_id, version_id):
            raise RingVersionNotFound(version_id)
        for version in self._ring_artifact_versions(ring_id):
            if self._ring_artifact_version_id(version) == version_id:
                return version
        raise RingVersionNotFound(version_id)

    def _find_latest_ring_artifact_version(self, ring_id):
        ring = self.get_ring(ring_id)
        for key in ('latest_swift_ring_version', 'latest_version'):
            if ring.get(key) is not None:
                return self._find_ring_artifact_version(ring_id, ring[key])
        for version in self._ring_artifact_versions(ring_id):
            if version.get('latest'):
                return version
        raise RingVersionNotFound('latest')

    def _latest_ring_artifact_version_id_for_ring(self, ring):
        ring_id = ring.get('id')
        if ring_id in (None, ''):
            return None
        for key in ('latest_swift_ring_version', 'latest_version'):
            if ring.get(key) is not None:
                self._find_ring_artifact_version(ring_id, ring[key])
                return str(ring[key])
        for version in self._ring_artifact_versions(ring_id):
            if version.get('latest'):
                return self._ring_artifact_version_id(version)
        return None

    def list_ring_artifact_versions(self, ring_id):
        self.get_ring(ring_id)
        try:
            latest_id = self._ring_artifact_version_id(
                self._find_latest_ring_artifact_version(ring_id))
        except RingVersionNotFound:
            latest_id = None
        return [
            self._public_ring_artifact_version(
                ring_id, version,
                latest=self._ring_artifact_version_id(version) == latest_id)
            for version in self._ring_artifact_versions(ring_id)]

    def get_ring_artifact_version(self, ring_id, version_id):
        latest = version_id == 'latest'
        version = self._find_ring_artifact_version(ring_id, version_id)
        if not latest:
            try:
                latest = self._ring_artifact_version_id(version) == \
                    self._ring_artifact_version_id(
                        self._find_latest_ring_artifact_version(ring_id))
            except RingVersionNotFound:
                latest = False
        return self._public_ring_artifact_version(
            ring_id, version, latest=latest)

    def get_ring_artifact_version_record(self, ring_id, version_id):
        version = copy.deepcopy(
            self._find_ring_artifact_version(ring_id, version_id))
        version.pop('_artifact_root', None)
        version.pop('_record_path', None)
        return version

    def get_concrete_ring_artifact_version_id(self, ring_id, version_id):
        return self._ring_artifact_version_id(
            self._find_ring_artifact_version(ring_id, version_id))

    def ring_artifact_version_exists(self, ring_id, version_id):
        try:
            self._find_ring_artifact_version(ring_id, version_id)
        except RingVersionNotFound:
            return False
        return True

    def ring_artifact_version_tombstoned(self, ring_id, version_id):
        return self._path_exists(
            self._ring_artifact_version_tombstone_file(ring_id, version_id))

    def ring_artifact_version_reserved(self, ring_id, version_id):
        return self.ring_artifact_version_exists(
            ring_id, version_id) or self.ring_artifact_version_tombstoned(
                ring_id, version_id)

    def create_ring_artifact_version_tombstone(
            self, ring_id, version_id, timestamp=None,
            deleted_by='artifact_cleanup', reason='retention_policy'):
        timestamp = normal_timestamp_internal(timestamp)
        tombstone = {
            'schema_version': 1,
            'type': 'ring_artifact_version',
            'ring_id': str(ring_id),
            'version': str(version_id),
            'deleted_at': timestamp,
            'deleted_by': deleted_by,
            'reason': reason,
        }
        self._write_json_file(
            self._ring_artifact_version_tombstone_file(ring_id, version_id),
            tombstone)
        return copy.deepcopy(tombstone)

    def _list_ring_artifact_version_tombstones(self):
        root = self._state_dir_path('tombstones', 'ring-versions')
        if root is None:
            return []
        try:
            ring_dirs = sorted(os.listdir(root))
        except OSError as err:
            if err.errno == errno.ENOENT:
                return []
            raise
        tombstones = []
        for ring_dir in ring_dirs:
            directory = os.path.join(root, ring_dir)
            if not os.path.isdir(directory):
                continue
            try:
                names = sorted(os.listdir(directory))
            except OSError as err:
                if err.errno == errno.ENOENT:
                    continue
                raise
            for name in names:
                if not name.endswith('.json'):
                    continue
                path = os.path.join(directory, name)
                tombstone = self._read_json_file(path)
                if tombstone is None:
                    continue
                if not isinstance(tombstone, dict):
                    raise ValueError('%s must be a JSON object' % path)
                tombstone = copy.deepcopy(tombstone)
                tombstone.setdefault('schema_version', 1)
                tombstone.setdefault('type', 'ring_artifact_version')
                tombstone.setdefault('ring_id', unquote(ring_dir))
                tombstone.setdefault('version', unquote(name[:-5]))
                tombstones.append(tombstone)
        return tombstones

    def get_ring_artifact_version_file(self, ring_id, version_id, file_name):
        version = self._find_ring_artifact_version(ring_id, version_id)
        for file_info in version.get('files', []):
            if file_info.get('name') == file_name:
                file_info = copy.deepcopy(file_info)
                file_info['path'] = self._resolve_artifact_path(
                    version, file_info)
                return version, file_info
        raise RingVersionFileNotFound(file_name)

    def save_ring_artifact_version(self, ring_id, version):
        self.get_ring(ring_id)
        version = copy.deepcopy(version)
        version.setdefault('ring_id', ring_id)
        version_id = validate_artifact_version_id(
            self._ring_artifact_version_id(version),
            'ring artifact version')
        if self.ring_artifact_version_tombstoned(ring_id, version_id):
            raise RingArtifactVersionReserved(ring_id, version_id)
        if self.ring_artifact_version_exists(ring_id, version_id):
            raise ValueError(
                'ring %s artifact version %s already exists' % (
                    ring_id, version_id))
        self._write_json_file(
            self._ring_artifact_version_file(ring_id, version_id), version)
        return self._public_ring_artifact_version(ring_id, version)

    def _ring_versions(self, include_tombstoned=False):
        releases_dir = self._state_dir_path('releases')
        if releases_dir is None:
            return []
        try:
            names = sorted(os.listdir(releases_dir))
        except OSError as err:
            if err.errno == errno.ENOENT:
                return []
            raise
        versions = []
        for name in names:
            path = os.path.join(releases_dir, name)
            if os.path.isdir(path):
                manifest_path = os.path.join(path, 'manifest.json')
                artifact_root = path
            elif name.endswith('.json'):
                manifest_path = path
                artifact_root = releases_dir
            else:
                continue
            version = self._read_json_file(manifest_path)
            if version is None:
                continue
            if not isinstance(version, dict):
                raise ValueError('%s must be a JSON object' % manifest_path)
            version.setdefault(
                'version', name[:-5] if name.endswith('.json') else name)
            version_id = validate_artifact_version_id(
                self._ring_version_id(version), 'release version')
            path_id = unquote(
                name[:-5] if name.endswith('.json') else name)
            if version_id != path_id:
                raise ValueError(
                    'release version %r does not match state path %r' %
                    (version_id, path_id))
            if (not include_tombstoned and
                    self.ring_version_tombstoned(version_id)):
                continue
            version['_manifest_path'] = manifest_path
            version['_artifact_root'] = artifact_root
            versions.append(version)
        return versions

    def list_ring_versions(self, cluster_id=None):
        latest_id = self.get_latest_ring_version_id()
        desired_id = self.get_desired_ring_version_id()
        return [
            self._public_ring_version(
                version,
                latest=self._ring_version_id(version) == latest_id,
                desired=self._ring_version_id(version) == desired_id)
            for version in self._ring_versions()
            if self._matches_cluster(version, cluster_id)
        ]

    def _find_ring_version(self, version_id):
        version_id = str(version_id)
        if self.ring_version_tombstoned(version_id):
            raise RingVersionNotFound(version_id)
        for version in self._ring_versions():
            if self._ring_version_id(version) == version_id:
                return version
            if self._object_id_matches(version, 'id', version_id):
                return version
            if self._object_id_matches(
                    version, 'resource_uri', version_id):
                return version
        raise RingVersionNotFound(version_id)

    def _latest_ring_version_id(self):
        index = self._state_index()
        for key in ('latest_ring_version', 'latest_version'):
            if index.get(key):
                return str(index[key])
        return None

    def _find_latest_ring_version(self):
        latest_id = self._latest_ring_version_id()
        if latest_id:
            return self._find_ring_version(latest_id)
        for version in self._ring_versions():
            if version.get('latest'):
                return version
        raise RingVersionNotFound('latest')

    def get_latest_ring_version_id(self):
        latest_id = self._latest_ring_version_id()
        if latest_id:
            return latest_id
        try:
            return self._ring_version_id(self._find_latest_ring_version())
        except RingVersionNotFound:
            return None

    def _desired_ring_version_id(self):
        desired_id = self._state_index().get('desired_ring_version')
        if desired_id in (None, ''):
            return None
        return str(desired_id)

    def _find_desired_ring_version(self):
        desired_id = self._desired_ring_version_id()
        if desired_id is None:
            raise RingVersionNotFound('desired')
        return self._find_ring_version(desired_id)

    def get_desired_ring_version_id(self):
        return self._desired_ring_version_id()

    def get_desired_ring_version(self):
        index = self._state_index()
        desired_id = index.get('desired_ring_version')
        if desired_id in (None, ''):
            raise RingVersionNotFound('desired')
        version = self._find_ring_version(desired_id)
        public_version = self._public_ring_version(
            version,
            latest=self._ring_version_id(version) ==
            self.get_latest_ring_version_id(),
            desired=True)
        if index.get('desired_updated_at') is not None:
            public_version['desired_updated_at'] = \
                index['desired_updated_at']
        if index.get('desired_reason') is not None:
            public_version['desired_reason'] = index['desired_reason']
        return public_version

    def get_ring_version(self, version_id):
        if version_id == 'latest':
            version = self._find_latest_ring_version()
            return self._public_ring_version(
                version, latest=True,
                desired=self._ring_version_id(version) ==
                self.get_desired_ring_version_id())
        if version_id == 'desired':
            return self.get_desired_ring_version()
        version = self._find_ring_version(version_id)
        return self._public_ring_version(
            version,
            latest=self._ring_version_id(version) ==
            self.get_latest_ring_version_id(),
            desired=self._ring_version_id(version) ==
            self.get_desired_ring_version_id())

    def get_ring_version_manifest(self, version_id):
        if version_id == 'latest':
            version = self._find_latest_ring_version()
            return self._manifest_for_ring_version(
                version, latest=True,
                desired=self._ring_version_id(version) ==
                self.get_desired_ring_version_id())
        if version_id == 'desired':
            version = self._find_desired_ring_version()
            return self._manifest_for_ring_version(
                version,
                latest=self._ring_version_id(version) ==
                self.get_latest_ring_version_id(), desired=True)
        version = self._find_ring_version(version_id)
        return self._manifest_for_ring_version(
            version,
            latest=self._ring_version_id(version) ==
            self.get_latest_ring_version_id(),
            desired=self._ring_version_id(version) ==
            self.get_desired_ring_version_id())

    def get_concrete_ring_version_id(self, version_id):
        if version_id == 'latest':
            return self._ring_version_id(self._find_latest_ring_version())
        if version_id == 'desired':
            return self._ring_version_id(self._find_desired_ring_version())
        return self._ring_version_id(self._find_ring_version(version_id))

    def get_ring_version_file(self, version_id, file_name):
        version = self._find_ring_version(version_id)
        for file_info in version.get('files', []):
            if file_info.get('name') == file_name:
                file_info = copy.deepcopy(file_info)
                file_info['path'] = self._resolve_artifact_path(
                    version, file_info)
                return version, file_info
        raise RingVersionFileNotFound(file_name)

    def ring_version_exists(self, version_id):
        try:
            self._find_ring_version(version_id)
        except RingVersionNotFound:
            return False
        return True

    def ring_version_tombstoned(self, version_id):
        return self._path_exists(self._ring_version_tombstone_file(version_id))

    def ring_version_reserved(self, version_id):
        return self.ring_version_exists(version_id) or \
            self.ring_version_tombstoned(version_id)

    def create_ring_version_tombstone(
            self, version_id, timestamp=None, deleted_by='artifact_cleanup',
            reason='retention_policy'):
        timestamp = normal_timestamp_internal(timestamp)
        tombstone = {
            'schema_version': 1,
            'type': 'ring_version',
            'id': str(version_id),
            'version': str(version_id),
            'deleted_at': timestamp,
            'deleted_by': deleted_by,
            'reason': reason,
        }
        self._write_json_file(
            self._ring_version_tombstone_file(version_id), tombstone)
        return copy.deepcopy(tombstone)

    def _list_ring_version_tombstones(self):
        directory = self._state_dir_path('tombstones', 'versions')
        if directory is None:
            return []
        try:
            names = sorted(os.listdir(directory))
        except OSError as err:
            if err.errno == errno.ENOENT:
                return []
            raise
        tombstones = []
        for name in names:
            if not name.endswith('.json'):
                continue
            path = os.path.join(directory, name)
            tombstone = self._read_json_file(path)
            if tombstone is None:
                continue
            if not isinstance(tombstone, dict):
                raise ValueError('%s must be a JSON object' % path)
            tombstone = copy.deepcopy(tombstone)
            tombstone.setdefault('schema_version', 1)
            tombstone.setdefault('type', 'ring_version')
            tombstone.setdefault('id', unquote(name[:-5]))
            tombstone.setdefault('version', tombstone['id'])
            tombstones.append(tombstone)
        return tombstones

    def list_tombstones(self):
        return {
            'versions': self._list_ring_version_tombstones(),
            'ring_versions': self._list_ring_artifact_version_tombstones(),
        }

    def save_ring_version(self, version):
        version = copy.deepcopy(version)
        version_id = validate_artifact_version_id(
            self._ring_version_id(version), 'release version')
        if version_id in RESERVED_ARTIFACT_VERSION_IDS:
            raise ValueError(
                'published ring version %s is reserved for an API selector' %
                version_id)
        if self.ring_version_tombstoned(version_id):
            raise RingVersionReserved(version_id)
        if self.ring_version_exists(version_id):
            raise ValueError(
                'published ring version %s already exists' % version_id)
        self._write_json_file(
            self._state_dir_path(
                'releases', self._safe_id(version_id), 'manifest.json'),
            version)
        return self._public_ring_version(version)

    def set_latest_ring_version(self, version_id):
        def mutate(index):
            index['latest_ring_version'] = str(version_id)
            return True, copy.deepcopy(index)
        return self._mutate_state_index(mutate)

    def set_desired_ring_version(self, version_id, expected_desired,
                                 timestamp=None, reason=None):
        version_id = self._ring_version_id(
            self._find_ring_version(version_id))
        expected_desired = None if expected_desired is None else \
            str(expected_desired)
        timestamp = normal_timestamp_internal(timestamp)

        def mutate(index):
            current = index.get('desired_ring_version')
            current = None if current in (None, '') else str(current)
            if current != expected_desired:
                raise RingDesiredVersionConflict(expected_desired, current)
            if current == version_id:
                return False, {
                    'status': 'unchanged',
                    'version': version_id,
                    'previous_desired': current,
                    'updated_at': index.get('desired_updated_at'),
                    'reason': index.get('desired_reason'),
                }
            index['desired_ring_version'] = version_id
            index['desired_updated_at'] = timestamp
            if reason in (None, ''):
                index.pop('desired_reason', None)
            else:
                index['desired_reason'] = str(reason)
            return True, {
                'status': 'updated',
                'version': version_id,
                'previous_desired': current,
                'updated_at': timestamp,
                'reason': index.get('desired_reason'),
            }

        return self._mutate_state_index(mutate)

    def _timestamp_float(self, obj, keys=('created_at', 'updated_at')):
        for key in keys:
            value = obj.get(key)
            if value is None:
                continue
            try:
                return normal_timestamp_float(value)
            except (TypeError, ValueError):
                pass
        return None
