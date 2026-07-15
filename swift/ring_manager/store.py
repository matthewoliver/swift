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

from swift.common.utils import fsync, fsync_dir, lock_path
from swift.ring_manager.common import normal_timestamp_float


class RingManagerStore(object):
    """Directory-backed durable state for the ring-manager service."""

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
