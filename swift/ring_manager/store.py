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

from urllib.parse import quote

from swift.common.utils import config_true_value, fsync, fsync_dir, lock_path
from swift.ring_manager.common import normal_timestamp_float


class RingNotFound(KeyError):
    pass


class RingAlreadyExists(KeyError):
    pass


TERMINAL_RING_BUILD_STATES = ('completed', 'failed', 'cancelled')


class RingManagerStore(object):
    """Directory-backed durable state for the ring-manager service."""

    BUILDER_OWNED_RING_FIELDS = (
        'part_power', 'num_replicas', 'min_part_hours', 'overload')

    def __init__(self, state_dir=None):
        self.state_dir = state_dir

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
            obj.setdefault(
                'devices_url',
                '/api/v1/rings/%s/devices/' % self._safe_id(obj['id']))
        return obj

    def _strip_builder_owned_ring_fields(self, ring):
        ring = copy.deepcopy(ring)
        ring.pop('devices', None)
        for key in self.BUILDER_OWNED_RING_FIELDS:
            ring.pop(key, None)
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
        ring = self._strip_builder_owned_ring_fields(ring)
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
        ring = self._strip_builder_owned_ring_fields(ring)
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
