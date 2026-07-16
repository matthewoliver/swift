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

import contextlib
import hashlib
import os
import re

from swift.common import exceptions as swift_exceptions
from swift.common.ring.ring import DEFAULT_RING_FORMAT_VERSION, RING_CODECS
from swift.common.utils import config_true_value, lock_file, md5, mkdirs
from swift.ring_manager.builder import RingBuilderManager, \
    RingBuilderManagerError, save_builder_durable
from swift.ring_manager.common import DEFAULT_BUILDER_LOCK_TIMEOUT, \
    NormalTimestamp, normal_timestamp, validate_artifact_version_id
from swift.ring_manager.store import RingVersionNotFound


class RingBuilderPublisherError(RingBuilderManagerError):
    pass


class RingBuilderPublisherDeferred(RingBuilderPublisherError):
    def __init__(self, message, reason=None, retry_after=None, ring_id=None):
        super(RingBuilderPublisherDeferred, self).__init__(message)
        self.reason = reason
        self.retry_after = retry_after
        self.ring_id = ring_id


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
                ring['id'] for ring in self.store.list_rings(
                    payload.get('cluster_id'))
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
        etag = md5(usedforsecurity=False)
        byte_count = 0
        with open(path, 'rb') as fp:
            while True:
                body = fp.read(1024 * 1024)
                if not body:
                    break
                byte_count += len(body)
                checksum.update(body)
                etag.update(body)
        return {
            'name': file_name,
            'path': os.path.join(publish_version, file_name),
            'bytes': byte_count,
            'md5': etag.hexdigest(),
            'sha256': checksum.hexdigest(),
        }

    def _save_ring_data(self, builder, artifact_path, format_version):
        directory = os.path.dirname(artifact_path)
        if directory:
            mkdirs(directory)
        builder.get_ring().save(
            artifact_path, format_version=format_version)

    def _format_version_from_payload(self, payload):
        value = payload.get('format_version')
        if value is None:
            return None
        if isinstance(value, bool):
            raise RingBuilderPublisherError(
                'format_version must be one of %s' %
                ', '.join(str(version) for version in sorted(RING_CODECS)))
        if isinstance(value, int):
            format_version = value
        elif isinstance(value, str) and re.match(r'^[0-9]+$', value):
            try:
                format_version = int(value)
            except ValueError:
                raise RingBuilderPublisherError(
                    'format_version must be one of %s' %
                    ', '.join(
                        str(version) for version in sorted(RING_CODECS)))
        else:
            raise RingBuilderPublisherError(
                'format_version must be one of %s' %
                ', '.join(str(version) for version in sorted(RING_CODECS)))
        if format_version not in RING_CODECS:
            raise RingBuilderPublisherError(
                'format_version must be one of %s' %
                ', '.join(str(version) for version in sorted(RING_CODECS)))
        return format_version

    def _effective_format_version(self, ring, builder, format_version):
        if format_version is None:
            if builder.dev_id_bytes > 2:
                return 2
            return DEFAULT_RING_FORMAT_VERSION
        if format_version == 1 and builder.dev_id_bytes > 2:
            raise RingBuilderPublisherError(
                'ring %s requires ring format version 2 for %d-byte '
                'device ids' % (ring.get('id'), builder.dev_id_bytes))
        return format_version

    def _preflight_format_version(self, ring, format_version):
        if format_version is None:
            return
        try:
            builder = self.builder_manager.load_builder(ring)[1]
        except swift_exceptions.FileNotFoundError:
            raise RingBuilderPublisherError(
                'ring %s builder does not exist' % ring.get('id'))
        self._effective_format_version(ring, builder, format_version)

    def _builder_lock_path(self, builder_path):
        return '%s.lock' % builder_path

    @contextlib.contextmanager
    def _builder_locks(self, rings):
        lock_specs = {}
        for ring in rings:
            builder_path = self.builder_manager.builder_path(ring)
            lock_specs[self._builder_lock_path(builder_path)] = (
                builder_path, ring.get('id'))
        with contextlib.ExitStack() as stack:
            for lock_path in sorted(lock_specs):
                builder_path, ring_id = lock_specs[lock_path]
                directory = os.path.dirname(builder_path)
                if directory:
                    mkdirs(directory)
                try:
                    stack.enter_context(lock_file(
                        lock_path, timeout=self.builder_lock_timeout,
                        unlink=False))
                except swift_exceptions.LockTimeout:
                    raise RingBuilderPublisherError(
                        'timed out waiting for builder lock for ring %s' %
                        ring_id)
            yield

    def _builder_needs_rebalance(self, builder):
        if builder.devs_changed:
            return True
        # Removed devices may be reassigned even while movement is limited, so
        # only the real rebalance attempt can decide that case safely.
        if builder.dispersion is not None and builder.dispersion > 0:
            return True
        try:
            balance = builder.get_balance()
        except swift_exceptions.RingBuilderError:
            return False
        return balance > 5 and balance / 100.0 > builder.overload

    def _defer_if_min_part_hours(self, ring, builder, retry_after=None):
        if retry_after is None:
            retry_after = builder.min_part_seconds_left
        if retry_after <= 0 or not self._builder_needs_rebalance(builder):
            return
        ring_id = ring.get('id')
        raise RingBuilderPublisherDeferred(
            'ring %s build deferred until min_part_hours passes: '
            '%s seconds remaining' % (ring_id, retry_after),
            reason='min_part_hours', retry_after=retry_after,
            ring_id=ring_id)

    def _preflight_ring(self, ring):
        try:
            builder = self.builder_manager.load_builder(ring)[1]
        except swift_exceptions.FileNotFoundError:
            raise RingBuilderPublisherError(
                'ring %s builder does not exist' % ring.get('id'))
        if not self.builder_manager.active_device_count(builder):
            raise RingBuilderPublisherError(
                'ring %s has no builder devices to build' % ring.get('id'))
        self._defer_if_min_part_hours(ring, builder)

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
                           format_version=None):
        try:
            builder_path, builder = self.builder_manager.load_builder(ring)
        except swift_exceptions.FileNotFoundError:
            raise RingBuilderPublisherError(
                'ring %s builder does not exist' % ring.get('id'))
        if not self.builder_manager.active_device_count(builder):
            raise RingBuilderPublisherError(
                'ring %s has no builder devices to build' % ring.get('id'))
        needs_rebalance = self._builder_needs_rebalance(builder)
        min_part_seconds_left = builder.min_part_seconds_left
        try:
            changed_parts, balance, removed_devs = builder.rebalance(
                seed=seed)
            if (needs_rebalance and min_part_seconds_left > 0 and
                    not changed_parts and not removed_devs):
                self._defer_if_min_part_hours(
                    ring, builder, retry_after=min_part_seconds_left)
            builder.validate()
        except swift_exceptions.RingBuilderError as err:
            raise RingBuilderPublisherError(
                'ring %s build failed: %s' % (ring.get('id'), err))

        artifact_name = self._artifact_name(ring)
        artifact_path = self._artifact_path(publish_version, artifact_name)
        format_version = self._effective_format_version(
            ring, builder, format_version)
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
                    format_version=None, lock=True):
        builder_path = self.builder_manager.builder_path(ring)
        directory = os.path.dirname(builder_path)
        if directory:
            mkdirs(directory)
        if not lock:
            return self._build_ring_locked(
                ring, publish_version, created_at, seed=seed,
                format_version=format_version)
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
        format_version = self._format_version_from_payload(payload)

        self._preflight_format_version(ring, format_version)
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
        format_version = self._format_version_from_payload(payload)

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

        build_results = []
        build_results_by_ring = {}
        with self._builder_locks(build_rings):
            for ring in build_rings:
                self._preflight_format_version(ring, format_version)
            for ring in build_rings:
                self._preflight_ring(ring)
            for ring in manifest_ring_set:
                if ring['id'] not in build_ring_ids:
                    self._latest_artifact_version(ring)

            for ring in build_rings:
                result = self._build_ring(
                    ring, publish_version, created_at, seed=seed,
                    format_version=format_version, lock=False)
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
