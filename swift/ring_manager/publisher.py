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

import hashlib
import os
import re

from swift.common import exceptions as swift_exceptions
from swift.common.ring.ring import DEFAULT_RING_FORMAT_VERSION
from swift.common.utils import config_true_value, lock_file, mkdirs
from swift.ring_manager.builder import RingBuilderManager, \
    RingBuilderManagerError, save_builder_durable
from swift.ring_manager.common import DEFAULT_BUILDER_LOCK_TIMEOUT, \
    NormalTimestamp, normal_timestamp, validate_artifact_version_id
from swift.ring_manager.store import RingVersionNotFound


class RingBuilderPublisherError(RingBuilderManagerError):
    pass


class RingBuilderPublisher(object):
    """Build immutable ring artifacts and complete release manifests."""

    def __init__(self, store, ring_builder_dir=None, ring_artifact_dir=None,
                 builder_manager=None,
                 builder_lock_timeout=DEFAULT_BUILDER_LOCK_TIMEOUT,
                 logger=None, time_func=NormalTimestamp.now):
        self.store = store
        self.ring_artifact_dir = ring_artifact_dir or store.ring_artifact_dir
        self.builder_lock_timeout = builder_lock_timeout
        self.logger = logger
        self.time_func = time_func
        self.builder_manager = builder_manager or RingBuilderManager(
            ring_builder_dir, builder_lock_timeout=builder_lock_timeout)

    def _timestamp_internal(self, timestamp=None):
        timestamp = self.time_func() if timestamp is None else timestamp
        return normal_timestamp(timestamp).internal

    def _artifact_name(self, ring):
        ring_id = ring.get('id')
        ring_type = ring.get('ring_type') or ring.get('type')
        if ring_type == 'account' or ring_id == 'account':
            return 'account.ring.gz'
        if ring_type == 'container' or ring_id == 'container':
            return 'container.ring.gz'
        policy_index = ring.get('storage_policy_index')
        if policy_index in (None, '', 0, '0'):
            return 'object.ring.gz'
        return 'object-%s.ring.gz' % policy_index

    def _ring_ids(self, payload):
        ring_ids = payload.get('rings')
        if ring_ids is None:
            return [
                ring['id'] for ring in self.store.list_rings()
                if not self.store.ring_is_disabled(ring)]
        if not isinstance(ring_ids, list) or not ring_ids:
            raise RingBuilderPublisherError('rings must be a non-empty list')
        return ring_ids

    def _manifest_ring_set(self, payload):
        return [
            ring for ring in self.store.list_rings(payload.get('cluster_id'))
            if not self.store.ring_is_disabled(ring)]

    def _artifact_only_ring_id(self, payload):
        ring_id = payload.get('ring_id')
        ring_ids = payload.get('rings')
        if ring_ids is not None:
            if not isinstance(ring_ids, list) or len(ring_ids) != 1:
                raise RingBuilderPublisherError(
                    'artifact-only builds require exactly one ring')
            if ring_id is None:
                ring_id = ring_ids[0]
            elif str(ring_id) != str(ring_ids[0]):
                raise RingBuilderPublisherError(
                    'ring_id must match the only rings entry')
        if ring_id in (None, ''):
            raise RingBuilderPublisherError(
                'artifact-only builds require ring_id')
        return str(ring_id)

    def _artifact_namespace(self, ring_id, created_at):
        safe_ring_id = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(ring_id))
        return 'artifact-%s-%s' % (safe_ring_id, created_at)

    def _artifact_path(self, publish_version, file_name):
        if not self.ring_artifact_dir:
            raise RingBuilderPublisherError('ring_artifact_dir is required')
        return os.path.join(self.ring_artifact_dir, publish_version, file_name)

    def _artifact_info(self, publish_version, file_name, path):
        checksum = hashlib.sha256()
        byte_count = 0
        with open(path, 'rb') as fp:
            while True:
                body = fp.read(1024 * 1024)
                if not body:
                    break
                byte_count += len(body)
                checksum.update(body)
        return {
            'name': file_name,
            'path': os.path.join(publish_version, file_name),
            'bytes': byte_count,
            'sha256': checksum.hexdigest(),
        }

    def _save_ring_data(self, builder, artifact_path, format_version):
        directory = os.path.dirname(artifact_path)
        if directory:
            mkdirs(directory)
        builder.get_ring().save(
            artifact_path, format_version=format_version)

    def _builder_lock_path(self, builder_path):
        return '%s.lock' % builder_path

    def _preflight_ring(self, ring):
        try:
            builder = self.builder_manager.load_builder(ring)[1]
        except swift_exceptions.FileNotFoundError:
            raise RingBuilderPublisherError(
                'ring %s builder does not exist' % ring.get('id'))
        if not self.builder_manager.active_device_count(builder):
            raise RingBuilderPublisherError(
                'ring %s has no builder devices to build' % ring.get('id'))

    def _latest_artifact_version(self, ring):
        ring_id = ring['id']
        try:
            version = self.store.get_ring_artifact_version_record(
                ring_id, 'latest')
        except RingVersionNotFound:
            raise RingBuilderPublisherError(
                'ring %s has no published artifact; include it in the '
                'publish request before publishing a complete manifest' %
                ring_id)
        if not version.get('files'):
            raise RingBuilderPublisherError(
                'ring %s latest artifact version has no files' % ring_id)
        return version

    def _carried_forward_files(self, version):
        files = []
        for file_info in version.get('files', []):
            file_info = dict(file_info)
            path = file_info.get('path', file_info.get('name'))
            if path and not os.path.isabs(path):
                artifact_dir = version.get('artifact_dir') or \
                    self.ring_artifact_dir
                if artifact_dir:
                    path = os.path.normpath(os.path.join(artifact_dir, path))
            if path:
                file_info['path'] = path
            files.append(file_info)
        return files

    def _carry_forward_ring(self, ring):
        version = self._latest_artifact_version(ring)
        swift_ring_version = version.get(
            'swift_ring_version', version.get('version'))
        return {
            'ring_id': ring['id'],
            'swift_ring_version': swift_ring_version,
            'source_version': str(version.get('version')),
            'files': self._carried_forward_files(version),
        }

    def _build_ring_locked(self, ring, publish_version, created_at, seed=None,
                           format_version=DEFAULT_RING_FORMAT_VERSION):
        try:
            builder_path, builder = self.builder_manager.load_builder(ring)
        except swift_exceptions.FileNotFoundError:
            raise RingBuilderPublisherError(
                'ring %s builder does not exist' % ring.get('id'))
        if not self.builder_manager.active_device_count(builder):
            raise RingBuilderPublisherError(
                'ring %s has no builder devices to build' % ring.get('id'))
        try:
            changed_parts, balance, removed_devs = builder.rebalance(
                seed=seed)
            builder.validate()
        except swift_exceptions.RingBuilderError as err:
            raise RingBuilderPublisherError(
                'ring %s build failed: %s' % (ring.get('id'), err))

        artifact_name = self._artifact_name(ring)
        artifact_path = self._artifact_path(publish_version, artifact_name)
        self._save_ring_data(builder, artifact_path, format_version)
        save_builder_durable(builder, builder_path)
        file_info = self._artifact_info(
            publish_version, artifact_name, artifact_path)
        ring_id = ring['id']
        swift_ring_version = builder.version
        ring_version = {
            'ring_id': ring_id,
            'version': swift_ring_version,
            'swift_ring_version': swift_ring_version,
            'state': 'published',
            'created_at': created_at,
            'builder_file': builder_path,
            'artifact_dir': self.ring_artifact_dir,
            'files': [file_info],
        }
        self.store.save_ring_artifact_version(ring_id, ring_version)
        updates = {
            'latest_swift_ring_version': swift_ring_version,
            'device_count': self.builder_manager.active_device_count(builder),
            'ever_pushed': True,
            'last_rebalance_time': created_at,
        }
        if not (ring.get('builder_files') or ring.get('builder_path')):
            updates['builder_files'] = [builder_path]
        self.store.update_ring(ring_id, updates)
        return {
            'ring_id': ring_id,
            'swift_ring_version': swift_ring_version,
            'builder_file': builder_path,
            'changed_parts': changed_parts,
            'balance': balance,
            'removed_devs': removed_devs,
            'files': [file_info],
        }

    def _build_ring(self, ring, publish_version, created_at, seed=None,
                    format_version=DEFAULT_RING_FORMAT_VERSION):
        builder_path = self.builder_manager.builder_path(ring)
        directory = os.path.dirname(builder_path)
        if directory:
            mkdirs(directory)
        try:
            with lock_file(
                    self._builder_lock_path(builder_path),
                    timeout=self.builder_lock_timeout, unlink=False):
                return self._build_ring_locked(
                    ring, publish_version, created_at, seed=seed,
                    format_version=format_version)
        except swift_exceptions.LockTimeout:
            raise RingBuilderPublisherError(
                'timed out waiting for builder lock for ring %s' %
                ring.get('id'))

    def publish_artifact(self, payload):
        payload = dict(payload or {})
        if payload.get('version') is not None:
            raise RingBuilderPublisherError(
                'artifact-only builds do not accept version; the Swift '
                'builder version identifies the resulting ring artifact')
        ring_id = self._artifact_only_ring_id(payload)
        ring = self.store.get_ring(ring_id)
        created_at = self._timestamp_internal()
        artifact_namespace = str(payload.get('artifact_namespace') or
                                 self._artifact_namespace(ring_id,
                                                          created_at))
        seed = payload.get('seed')
        format_version = int(payload.get(
            'format_version', DEFAULT_RING_FORMAT_VERSION))

        self._preflight_ring(ring)
        result = self._build_ring(
            ring, artifact_namespace, created_at, seed=seed,
            format_version=format_version)
        return self.store.get_ring_artifact_version(
            ring_id, result['swift_ring_version'])

    def publish(self, payload):
        payload = dict(payload or {})
        if config_true_value(str(payload.get('artifact_only', 'false'))):
            return self.publish_artifact(payload)
        publish_version = validate_artifact_version_id(
            payload.get('version') or
            ('release-%s' % self._timestamp_internal()), 'release version')
        if self.store.ring_version_exists(publish_version):
            raise RingBuilderPublisherError(
                'published ring version %s already exists' % publish_version)
        created_at = self._timestamp_internal()
        seed = payload.get('seed')
        format_version = int(payload.get(
            'format_version', DEFAULT_RING_FORMAT_VERSION))

        build_rings = [self.store.get_ring(ring_id)
                       for ring_id in self._ring_ids(payload)]
        disabled_build_rings = [
            ring for ring in build_rings if self.store.ring_is_disabled(ring)]
        if disabled_build_rings:
            raise RingBuilderPublisherError(
                'ring %s is disabled; enable it before publishing' %
                disabled_build_rings[0]['id'])
        build_ring_ids = set(ring['id'] for ring in build_rings)
        if len(build_ring_ids) != len(build_rings):
            raise RingBuilderPublisherError(
                'rings contains duplicate ring ids')
        manifest_ring_set = self._manifest_ring_set(payload)
        if not manifest_ring_set:
            raise RingBuilderPublisherError(
                'no enabled rings are available to publish')
        manifest_ring_ids = set(ring['id'] for ring in manifest_ring_set)
        missing_from_manifest = build_ring_ids - manifest_ring_ids
        if missing_from_manifest:
            raise RingBuilderPublisherError(
                'ring %s is not in the manifest ring set' %
                sorted(missing_from_manifest)[0])

        for ring in build_rings:
            self._preflight_ring(ring)
        for ring in manifest_ring_set:
            if ring['id'] not in build_ring_ids:
                self._latest_artifact_version(ring)

        build_results = []
        build_results_by_ring = {}
        for ring in build_rings:
            result = self._build_ring(
                ring, publish_version, created_at, seed=seed,
                format_version=format_version)
            build_results.append(result)
            build_results_by_ring[result['ring_id']] = result

        carried_forward = []
        manifest_rings = []
        manifest_files = []
        for ring in manifest_ring_set:
            if ring['id'] in build_ring_ids:
                result = build_results_by_ring[ring['id']]
            else:
                result = self._carry_forward_ring(ring)
                carried_forward.append({
                    'ring_id': result['ring_id'],
                    'swift_ring_version': result['swift_ring_version'],
                    'source_version': result['source_version'],
                })
            manifest_rings.append({
                'ring_id': result['ring_id'],
                'swift_ring_version': result['swift_ring_version'],
            })
            manifest_files.extend(result['files'])

        manifest = {
            'version': publish_version,
            'state': 'published',
            'created_at': created_at,
            'artifact_dir': self.ring_artifact_dir,
            'rings': manifest_rings,
            'files': manifest_files,
            'build_results': build_results,
            'carried_forward': carried_forward,
        }
        self.store.save_ring_version(manifest)
        self.store.set_latest_ring_version(publish_version)
        public_manifest = self.store.get_ring_version_manifest(
            publish_version)
        public_manifest['latest'] = True
        return public_manifest
