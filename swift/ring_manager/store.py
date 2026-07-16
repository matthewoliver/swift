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
import os
import tempfile

from urllib.parse import quote, unquote

from swift.common.utils import config_true_value, fsync, fsync_dir, lock_path
from swift.ring_manager.common import normal_timestamp_float, \
    resolve_artifact_path, validate_artifact_version_id, \
    validate_path_component


class RingNotFound(KeyError):
    pass


class RingAlreadyExists(KeyError):
    pass


class RingVersionNotFound(KeyError):
    pass


class RingVersionFileNotFound(KeyError):
    pass


TERMINAL_RING_BUILD_STATES = ('completed', 'failed', 'cancelled')


class RingManagerStore(object):
    """Directory-backed durable state for the ring-manager service."""

    BUILDER_OWNED_RING_FIELDS = (
        'part_power', 'num_replicas', 'min_part_hours', 'overload')

    def __init__(self, state_dir=None, ring_artifact_dir=None):
        self.state_dir = state_dir
        self.ring_artifact_dir = ring_artifact_dir

    def _safe_id(self, object_id):
        return quote(str(object_id), safe='')

    def _path_exists(self, path):
        if path is None:
            return False
        return os.path.exists(path)

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

    def _delete_state_file(self, path, missing_ok=False):
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
        return True

    def _state_dir_path(self, *parts):
        if not self.state_dir:
            return None
        return os.path.join(self.state_dir, *parts)

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

    def _public_ring_version(self, version, latest=False):
        public_version = copy.deepcopy(version)
        public_version.pop('_artifact_root', None)
        public_version.pop('_manifest_path', None)
        public_version.pop('artifact_dir', None)
        public_version.pop('manifest', None)
        public_version['version'] = self._ring_version_id(version)
        public_version['resource_uri'] = self._ring_version_uri(version)
        public_version['latest'] = latest or bool(version.get('latest'))
        public_version['files'] = [
            self._public_ring_version_file(version, file_info)
            for file_info in version.get('files', [])]
        return public_version

    def _manifest_for_ring_version(self, version, latest=False):
        manifest = copy.deepcopy(version.get('manifest', {}))
        for key in ('path', 'artifact_dir', '_artifact_root',
                    '_manifest_path'):
            manifest.pop(key, None)
        manifest['version'] = self._ring_version_id(version)
        manifest['resource_uri'] = self._ring_version_uri(version)
        manifest['latest'] = latest or bool(version.get('latest'))
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
        for key in ('_artifact_root', 'artifact_dir'):
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

    def _ring_artifact_versions(self, ring_id):
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
            version['_artifact_root'] = directory
            versions.append(version)
        return versions

    def _find_ring_artifact_version(self, ring_id, version_id):
        if version_id == 'latest':
            return self._find_latest_ring_artifact_version(ring_id)
        version_id = str(version_id)
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
        if self.ring_artifact_version_exists(ring_id, version_id):
            raise ValueError(
                'ring %s artifact version %s already exists' % (
                    ring_id, version_id))
        self._write_json_file(
            self._ring_artifact_version_file(ring_id, version_id), version)
        return self._public_ring_artifact_version(ring_id, version)

    def _ring_versions(self):
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
            version['_manifest_path'] = manifest_path
            version['_artifact_root'] = artifact_root
            versions.append(version)
        return versions

    def list_ring_versions(self, cluster_id=None):
        latest_id = self.get_latest_ring_version_id()
        return [
            self._public_ring_version(
                version,
                latest=self._ring_version_id(version) == latest_id)
            for version in self._ring_versions()
            if self._matches_cluster(version, cluster_id)
        ]

    def _find_ring_version(self, version_id):
        version_id = str(version_id)
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

    def get_ring_version(self, version_id):
        if version_id == 'latest':
            version = self._find_latest_ring_version()
            return self._public_ring_version(version, latest=True)
        version = self._find_ring_version(version_id)
        return self._public_ring_version(
            version,
            latest=self._ring_version_id(version) ==
            self.get_latest_ring_version_id())

    def get_ring_version_manifest(self, version_id):
        if version_id == 'latest':
            version = self._find_latest_ring_version()
            return self._manifest_for_ring_version(version, latest=True)
        version = self._find_ring_version(version_id)
        return self._manifest_for_ring_version(
            version,
            latest=self._ring_version_id(version) ==
            self.get_latest_ring_version_id())

    def get_concrete_ring_version_id(self, version_id):
        if version_id == 'latest':
            return self._ring_version_id(self._find_latest_ring_version())
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

    def save_ring_version(self, version):
        version = copy.deepcopy(version)
        version_id = validate_artifact_version_id(
            self._ring_version_id(version), 'release version')
        if self.ring_version_exists(version_id):
            raise ValueError(
                'published ring version %s already exists' % version_id)
        self._write_json_file(
            self._state_dir_path(
                'releases', self._safe_id(version_id), 'manifest.json'),
            version)
        return self._public_ring_version(version)

    def set_latest_ring_version(self, version_id):
        index = self._state_index()
        index['latest_ring_version'] = str(version_id)
        self._write_json_file(self._state_dir_path('index.json'), index)
        return copy.deepcopy(index)

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
