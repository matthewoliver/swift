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
import hashlib
import os
import re
import shutil

from swift.common import exceptions as swift_exceptions
from swift.common.ring.ring import RingData
from swift.common.utils import config_true_value, lock_path, md5, mkdirs
from swift.ring_manager.builder import RingBuilderManager, \
    RingBuilderManagerError
from swift.ring_manager.common import NormalTimestamp, \
    RING_API_READONLY_FIELDS, normal_timestamp, resolve_artifact_path, \
    validate_artifact_version_id, validate_path_component
from swift.ring_manager.store import RingAlreadyExists, \
    RingArtifactVersionReserved, RingNotFound, RingVersionFileNotFound, \
    RingVersionReserved


class RingImporterError(RingBuilderManagerError):
    pass


class RingImportConflict(RingImporterError):
    pass


class RingImporter(object):
    """
    Enroll existing Swift builders and ring artifacts in ring-manager state.

    Import never rebalances, saves, or rewrites an existing builder. When a
    source ``.ring.gz`` is supplied it is copied into the ring-manager artifact
    store by default and published as an immutable baseline manifest.
    """

    CONTROL_FIELDS = set((
        'builder_file', 'builder_path', 'builder_files', 'copy_artifacts',
        'force', 'no_latest', 'ring_file', 'set_latest', 'version'))

    def __init__(self, store, ring_builder_dir=None, ring_artifact_dir=None,
                 logger=None, time_func=NormalTimestamp.now):
        self.store = store
        self.ring_builder_dir = ring_builder_dir
        self.ring_artifact_dir = ring_artifact_dir or store.ring_artifact_dir
        self.logger = logger
        self.time_func = time_func
        self.builder_manager = RingBuilderManager(ring_builder_dir)

    def _published_state_lock(self):
        if not self.store.state_dir:
            raise RingImporterError(
                'ring_manager_state_dir is required for ring import')
        return lock_path(self.store.state_dir, name='ring-manager-import')

    def _timestamp(self, timestamp=None):
        timestamp = self.time_func() if timestamp is None else timestamp
        return normal_timestamp(timestamp)

    def _timestamp_internal(self, timestamp=None):
        return self._timestamp(timestamp).internal

    def _bool(self, payload, key, default=False):
        if key not in payload:
            return default
        return config_true_value(str(payload[key]))

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
        raise RingImporterError('imported rings require an id or ring_type')

    def _infer_ring_metadata(self, metadata):
        ring_id = metadata.get('id')
        if ring_id == 'account':
            metadata.setdefault('ring_type', 'account')
        elif ring_id == 'container':
            metadata.setdefault('ring_type', 'container')
        elif ring_id in ('object', 'object-0'):
            metadata.setdefault('ring_type', 'object')
            metadata.setdefault('storage_policy_index', 0)
        elif ring_id:
            match = re.match(r'^object-(\d+)$', str(ring_id))
            if match:
                metadata.setdefault('ring_type', 'object')
                metadata.setdefault('storage_policy_index',
                                    int(match.group(1)))

        if metadata.get('id') in (None, ''):
            metadata['id'] = self._default_ring_id(metadata)

        if metadata.get('ring_type') == 'object':
            metadata.setdefault('policy_type', 'replication')
        return metadata

    def _builder_file(self, spec):
        if spec.get('builder_file'):
            return spec['builder_file']
        if spec.get('builder_path'):
            return spec['builder_path']
        files = spec.get('builder_files')
        if files:
            if not isinstance(files, list) or len(files) != 1:
                raise RingImporterError(
                    'imported rings require exactly one builder file')
            return files[0]
        raise RingImporterError('imported rings require builder_file')

    def _source_ring_path(self, spec):
        path = spec.get('ring_file')
        if path in (None, ''):
            return None
        if not os.path.isabs(path) and self.ring_builder_dir:
            path = os.path.join(self.ring_builder_dir, path)
        return os.path.abspath(path)

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

    def _artifact_name_for_spec(self, spec):
        metadata = copy.deepcopy(spec['metadata'])
        try:
            existing = self.store.get_ring(metadata['id'])
        except RingNotFound:
            pass
        else:
            existing.update(metadata)
            metadata = existing
        return self._artifact_name(metadata)

    def _artifact_path(self, publish_version, file_name):
        publish_version = validate_artifact_version_id(
            publish_version, 'artifact namespace')
        file_name = validate_path_component(file_name, 'artifact file name')
        return resolve_artifact_path(
            self.ring_artifact_dir,
            os.path.join(publish_version, file_name),
            'artifact path')

    def _artifact_info(self, file_name, path, stored_path):
        checksum = hashlib.sha256()
        etag = md5(usedforsecurity=False)
        byte_count = 0
        with open(path, 'rb') as fp:
            while True:
                chunk = fp.read(65536)
                if not chunk:
                    break
                byte_count += len(chunk)
                checksum.update(chunk)
                etag.update(chunk)
        return {
            'name': file_name,
            'path': stored_path,
            'bytes': byte_count,
            'md5': etag.hexdigest(),
            'sha256': checksum.hexdigest(),
        }

    def _file_import_identity(self, file_info):
        return (
            str(file_info['name']),
            int(file_info['bytes']),
            str(file_info['md5']),
            str(file_info['sha256']),
        )

    def _spec_import_identity(self, spec):
        files = ()
        if spec['source_ring_path'] is not None:
            file_name = self._artifact_name_for_spec(spec)
            file_info = self._artifact_info(
                file_name, spec['source_ring_path'], file_name)
            files = (self._file_import_identity(file_info),)
        return (
            str(spec['metadata']['id']),
            str(spec['builder'].version),
            files,
        )

    def _stored_import_identity(self, manifest):
        import_results = manifest.get('import_results')
        if not isinstance(import_results, list):
            return None
        try:
            identity = []
            for result in import_results:
                files = result.get('files', [])
                if not isinstance(files, list):
                    return None
                identity.append((
                    str(result['ring_id']),
                    str(result['swift_ring_version']),
                    tuple(sorted(
                        self._file_import_identity(file_info)
                        for file_info in files)),
                ))
            return tuple(sorted(identity))
        except (KeyError, TypeError, ValueError):
            return None

    def _stored_manifest_identity(self, manifest):
        rings = manifest.get('rings')
        files = manifest.get('files')
        if (not isinstance(rings, list) or not isinstance(files, list) or
                len(rings) != len(files)):
            return None
        try:
            return tuple(sorted(
                (
                    str(ring['ring_id']),
                    str(ring['swift_ring_version']),
                    (self._file_import_identity(file_info),),
                )
                for ring, file_info in zip(rings, files)))
        except (KeyError, TypeError, ValueError):
            return None

    def _idempotent_import(self, publish_version, specs, enabled_specs):
        if not self.store.ring_version_exists(publish_version):
            return None

        existing = self.store.get_ring_version(publish_version)
        if existing.get('state') != 'imported':
            raise RingImportConflict(
                'published ring version %s already exists and is not an '
                'imported release' % publish_version)
        spec_identities = [
            (id(spec), self._spec_import_identity(spec))
            for spec in specs]
        incoming = tuple(sorted(
            identity for _spec_id, identity in spec_identities))
        enabled_spec_ids = set(id(spec) for spec in enabled_specs)
        enabled_incoming = tuple(sorted(
            identity for spec_id, identity in spec_identities
            if spec_id in enabled_spec_ids))
        if (self._stored_import_identity(existing) != incoming or
                self._stored_manifest_identity(existing) !=
                enabled_incoming):
            raise RingImportConflict(
                'published ring version %s already exists with different '
                'import artifacts' % publish_version)

        result = self.store.get_ring_version_manifest(publish_version)
        result['import_status'] = 'unchanged'
        return result

    def _ring_assignments(self, ring_data):
        return [
            list(part2dev_id)
            for part2dev_id in ring_data._replica2part2dev_id]

    def _validate_ring_artifact(self, spec, source_path):
        ring_id = spec['metadata']['id']
        try:
            artifact_ring = RingData.load(source_path)
        except Exception as err:
            raise RingImporterError(
                'ring %s artifact could not be loaded: %s' %
                (ring_id, err))

        builder_ring = spec['builder'].get_ring()
        mismatches = []
        if artifact_ring.part_power != builder_ring.part_power:
            mismatches.append('part_power')
        if artifact_ring.replica_count != builder_ring.replica_count:
            mismatches.append('replica_count')
        if artifact_ring.next_part_power != builder_ring.next_part_power:
            mismatches.append('next_part_power')
        if artifact_ring.version != builder_ring.version:
            mismatches.append('version')
        if artifact_ring.devs != builder_ring.devs:
            mismatches.append('devices')
        if (self._ring_assignments(artifact_ring) !=
                self._ring_assignments(builder_ring)):
            mismatches.append('assignments')
        if mismatches:
            raise RingImporterError(
                'ring %s artifact does not match builder: %s' %
                (ring_id, ', '.join(mismatches)))

    def _copy_or_reference_ring_file(self, spec, publish_version, file_name,
                                     source_path):
        copy_artifacts = spec['copy_artifacts']
        if not copy_artifacts:
            raise RingImporterError(
                'copy_artifacts=false is not supported for published ring '
                'artifacts')
        artifact_path = self._artifact_path(publish_version, file_name)
        stored_path = os.path.join(
            validate_artifact_version_id(
                publish_version, 'artifact namespace'),
            validate_path_component(file_name, 'artifact file name'))
        if os.path.exists(artifact_path):
            try:
                stored_info = self._artifact_info(
                    file_name, artifact_path, stored_path)
            except (IOError, OSError) as err:
                raise RingImportConflict(
                    'artifact %s cannot be reused: %s' %
                    (artifact_path, err))
            source_info = self._artifact_info(
                file_name, source_path, stored_path)
            if (self._file_import_identity(stored_info) !=
                    self._file_import_identity(source_info)):
                raise RingImportConflict(
                    'artifact %s already exists with different bytes' %
                    artifact_path)
            return stored_info
        directory = os.path.dirname(artifact_path)
        if directory:
            mkdirs(directory)
        if os.path.abspath(source_path) != os.path.abspath(artifact_path):
            shutil.copyfile(source_path, artifact_path)
        return self._artifact_info(file_name, artifact_path, stored_path)

    def _existing_ring_artifact(self, spec, file_name):
        ring_id = spec['metadata']['id']
        swift_ring_version = spec['builder'].version
        if not self.store.ring_artifact_version_exists(
                ring_id, swift_ring_version):
            return None

        record = self.store.get_ring_artifact_version_record(
            ring_id, swift_ring_version)
        files = record.get('files')
        if (str(record.get('ring_id')) != str(ring_id) or
                str(record.get('swift_ring_version')) !=
                str(swift_ring_version) or
                not isinstance(files, list) or len(files) != 1):
            raise RingImportConflict(
                'ring %s artifact version %s has conflicting metadata' %
                (ring_id, swift_ring_version))

        stored_file = files[0]
        source_info = self._artifact_info(
            file_name, spec['source_ring_path'], file_name)
        try:
            if (self._file_import_identity(stored_file) !=
                    self._file_import_identity(source_info)):
                raise RingImportConflict(
                    'ring %s artifact version %s has different metadata' %
                    (ring_id, swift_ring_version))
            _version, resolved_file = \
                self.store.get_ring_artifact_version_file(
                    ring_id, swift_ring_version, file_name)
            actual_info = self._artifact_info(
                file_name, resolved_file['path'], stored_file['path'])
        except RingImportConflict:
            raise
        except (IOError, OSError, KeyError, TypeError, ValueError,
                RingVersionFileNotFound) as err:
            raise RingImportConflict(
                'ring %s artifact version %s cannot be reused: %s' %
                (ring_id, swift_ring_version, err))
        if (self._file_import_identity(actual_info) !=
                self._file_import_identity(source_info)):
            raise RingImportConflict(
                'ring %s artifact version %s has different stored bytes' %
                (ring_id, swift_ring_version))
        return copy.deepcopy(stored_file)

    def _normalize_spec(self, spec, payload):
        if not isinstance(spec, dict):
            raise RingImporterError('rings entries must be objects')
        metadata = copy.deepcopy(dict(
            (key, value) for key, value in spec.items()
            if (key not in self.CONTROL_FIELDS and
                key not in RING_API_READONLY_FIELDS)))
        builder_file = self._builder_file(spec)
        metadata['builder_files'] = [builder_file]
        metadata = self._infer_ring_metadata(metadata)
        source_ring_path = self._source_ring_path(spec)

        ring = copy.deepcopy(metadata)
        try:
            builder_path, builder = self.builder_manager.load_builder(ring)
        except swift_exceptions.FileNotFoundError:
            raise RingImporterError(
                'ring %s builder does not exist: %s' %
                (ring.get('id'), self.builder_manager.builder_path(ring)))

        if source_ring_path is not None and not os.path.isfile(
                source_ring_path):
            raise RingImporterError(
                'ring %s artifact does not exist: %s' %
                (ring.get('id'), source_ring_path))

        copy_artifacts = self._bool(
            spec, 'copy_artifacts',
            default=self._bool(payload, 'copy_artifacts', True))
        if source_ring_path is not None and not copy_artifacts:
            raise RingImporterError(
                'copy_artifacts=false is not supported for published ring '
                'artifacts')
        normalized_spec = {
            'metadata': metadata,
            'builder_path': builder_path,
            'builder': builder,
            'device_count': self.builder_manager.active_device_count(builder),
            'source_ring_path': source_ring_path,
            'copy_artifacts': copy_artifacts,
        }
        if source_ring_path is not None:
            self._validate_ring_artifact(normalized_spec, source_ring_path)
        return normalized_spec

    def _normalize_specs(self, payload):
        rings = payload.get('rings')
        if not isinstance(rings, list) or not rings:
            raise RingImporterError('rings must be a non-empty list')
        normalized = [
            self._normalize_spec(spec, payload) for spec in rings]
        artifact_names = set()
        for spec in normalized:
            if spec['source_ring_path'] is None:
                continue
            artifact_name = self._artifact_name_for_spec(spec)
            if artifact_name in artifact_names:
                raise RingImporterError(
                    'multiple imported rings would publish %s' %
                    artifact_name)
            artifact_names.add(artifact_name)
        return normalized

    def _preflight_artifact_namespace(self, specs, artifact_namespace):
        namespace_path = resolve_artifact_path(
            self.ring_artifact_dir, artifact_namespace,
            'artifact namespace')
        if not os.path.exists(namespace_path):
            return
        if not os.path.isdir(namespace_path):
            raise RingImportConflict(
                'artifact namespace %s is not a directory' %
                artifact_namespace)

        expected = {}
        for spec in specs:
            if spec['source_ring_path'] is None:
                continue
            expected[self._artifact_name_for_spec(spec)] = spec
        for file_name in os.listdir(namespace_path):
            spec = expected.get(file_name)
            if spec is None:
                raise RingImportConflict(
                    'artifact namespace %s contains unexpected artifact %s' %
                    (artifact_namespace, file_name))
            artifact_path = self._artifact_path(
                artifact_namespace, file_name)
            try:
                stored_info = self._artifact_info(
                    file_name, artifact_path, file_name)
            except (IOError, OSError) as err:
                raise RingImportConflict(
                    'artifact namespace %s contains unusable artifact %s: '
                    '%s' % (artifact_namespace, file_name, err))
            source_info = self._artifact_info(
                file_name, spec['source_ring_path'], file_name)
            if (self._file_import_identity(stored_info) !=
                    self._file_import_identity(source_info)):
                raise RingImportConflict(
                    'artifact namespace %s contains different artifact %s' %
                    (artifact_namespace, file_name))

    def _ring_import_metadata(self, spec, preserve_existing=False):
        metadata = copy.deepcopy(spec['metadata'])
        if preserve_existing:
            try:
                existing = self.store.get_ring(metadata['id'])
            except RingNotFound:
                pass
            else:
                existing.update(metadata)
                metadata = existing
        metadata['device_count'] = spec['device_count']
        metadata['disabled'] = self.store.ring_is_disabled(metadata)
        metadata.pop('resource_uri', None)
        return metadata

    def _ring_matches_import(self, ring, spec):
        if ring.get('imported_at') in (None, ''):
            return False
        expected = self._ring_import_metadata(spec)
        return all(ring.get(key) == value
                   for key, value in expected.items())

    def _check_ring_version_not_stale(self, ring, spec):
        latest = ring.get('latest_swift_ring_version')
        if latest in (None, ''):
            latest = ring.get('latest_version')
        if latest in (None, ''):
            return
        if isinstance(latest, bool):
            raise RingImportConflict(
                'ring %s has invalid latest artifact version %r' %
                (ring['id'], latest))
        try:
            latest = int(latest)
        except (TypeError, ValueError):
            raise RingImportConflict(
                'ring %s has invalid latest artifact version %r' %
                (ring['id'], latest))
        swift_ring_version = spec['builder'].version
        if swift_ring_version < latest:
            raise RingImportConflict(
                'ring %s artifact version %s is older than latest artifact '
                'version %s; select an older known release through desired '
                'for rollback' %
                (ring['id'], swift_ring_version, latest))

    def _preflight(self, specs, publish_version, force):
        if publish_version is not None:
            publish_version = validate_artifact_version_id(publish_version)
        if publish_version is not None:
            if self.store.ring_version_tombstoned(publish_version):
                raise RingImporterError(
                    'published ring version %s is reserved by tombstone' %
                    publish_version)
            if self.store.ring_version_exists(publish_version):
                raise RingImporterError(
                    'published ring version %s already exists' %
                    publish_version)
        seen_ring_ids = set()
        for spec in specs:
            ring_id = spec['metadata']['id']
            if ring_id in seen_ring_ids:
                raise RingImporterError(
                    'ring %s appears more than once in import request' %
                    ring_id)
            seen_ring_ids.add(ring_id)
            if (spec.get('source_ring_path') is not None and
                    self.store.ring_artifact_version_tombstoned(
                        ring_id, spec['builder'].version)):
                raise RingImporterError(
                    'ring %s artifact version %s is reserved by tombstone' %
                    (ring_id, spec['builder'].version))
            try:
                ring = self.store.get_ring(ring_id)
            except RingNotFound:
                continue
            if spec.get('source_ring_path') is not None:
                self._check_ring_version_not_stale(ring, spec)
            if not force and not self._ring_matches_import(ring, spec):
                raise RingImportConflict(
                    'ring %s already exists with different import metadata; '
                    'set force=true to update the ring record' % ring_id)

    def _save_ring_metadata(self, spec, created_at, force):
        metadata = self._ring_import_metadata(
            spec, preserve_existing=force)
        metadata['imported_at'] = created_at
        try:
            return self.store.create_ring(metadata)
        except RingAlreadyExists:
            if not force:
                ring = self.store.get_ring(metadata['id'])
                if self._ring_matches_import(ring, spec):
                    return ring
                raise
            return self.store.update_ring(metadata['id'], metadata)

    def _effective_ring_metadata(self, spec, force):
        metadata = copy.deepcopy(spec['metadata'])
        if not force:
            return metadata
        try:
            existing = self.store.get_ring(metadata['id'])
        except RingNotFound:
            return metadata
        existing.update(metadata)
        return existing

    def import_rings(self, payload):
        payload = dict(payload or {})
        if 'set_desired' in payload or 'expected_desired' in payload:
            raise RingImporterError(
                'imports cannot set the desired release; promote the '
                'imported release after validation')
        specs = self._normalize_specs(payload)
        force = self._bool(payload, 'force', False)
        specs_with_artifacts = [
            spec for spec in specs if spec['source_ring_path'] is not None]
        enabled_specs_with_artifacts = [
            spec for spec in specs_with_artifacts
            if not self.store.ring_is_disabled(
                self._effective_ring_metadata(spec, force))]
        created_at = self._timestamp_internal()
        if not enabled_specs_with_artifacts:
            if payload.get('version') is not None:
                raise RingImporterError(
                    'version requires at least one enabled ring_file')
            if self._bool(payload, 'set_latest', False):
                raise RingImporterError(
                    'set_latest requires at least one enabled ring_file')
            publish_version = None
        else:
            publish_version = validate_artifact_version_id(
                payload.get('version') or ('import-%s' % created_at))
        artifact_namespace = publish_version or 'import-%s' % created_at
        set_latest = not self._bool(payload, 'no_latest', False)
        if 'set_latest' in payload:
            set_latest = self._bool(payload, 'set_latest', False)
        with self._published_state_lock():
            if publish_version is not None:
                existing = self._idempotent_import(
                    publish_version, specs, enabled_specs_with_artifacts)
                if existing is not None:
                    return existing
            self._preflight(specs, publish_version, force)
            if publish_version is not None:
                self._preflight_artifact_namespace(
                    specs, artifact_namespace)

            import_results = []
            manifest_rings = []
            manifest_files = []
            for spec in specs:
                ring = self._save_ring_metadata(spec, created_at, force)
                ring_id = ring['id']
                swift_ring_version = spec['builder'].version
                result = {
                    'ring_id': ring_id,
                    'resource_uri': ring['resource_uri'],
                    'swift_ring_version': swift_ring_version,
                    'builder_file': spec['builder_path'],
                    'device_count': spec['device_count'],
                }
                if spec['source_ring_path'] is not None:
                    artifact_name = self._artifact_name(ring)
                    file_info = self._existing_ring_artifact(
                        spec, artifact_name)
                    reused_artifact = file_info is not None
                    if not reused_artifact:
                        file_info = self._copy_or_reference_ring_file(
                            spec, artifact_namespace, artifact_name,
                            spec['source_ring_path'])
                    ring_version = {
                        'ring_id': ring_id,
                        'version': swift_ring_version,
                        'swift_ring_version': swift_ring_version,
                        'state': 'imported',
                        'created_at': created_at,
                        'builder_file': spec['builder_path'],
                        'artifact_dir': self.ring_artifact_dir,
                        'files': [file_info],
                    }
                    if not reused_artifact:
                        try:
                            self.store.save_ring_artifact_version(
                                ring_id, ring_version)
                        except RingArtifactVersionReserved as err:
                            raise RingImporterError(str(err))
                    self.store.update_ring(ring_id, {
                        'latest_swift_ring_version': swift_ring_version,
                        'device_count': spec['device_count'],
                        'ever_pushed': True,
                        'imported_at': created_at,
                    })
                    public_file_info = copy.deepcopy(file_info)
                    public_file_info.pop('path', None)
                    result['files'] = [public_file_info]
                    if not self.store.ring_is_disabled(ring):
                        manifest_rings.append({
                            'ring_id': ring_id,
                            'swift_ring_version': swift_ring_version,
                        })
                        manifest_files.append(file_info)
                import_results.append(result)

            if publish_version is None:
                return {
                    'import_status': 'created',
                    'state': 'imported',
                    'created_at': created_at,
                    'rings': import_results,
                }

            manifest = {
                'version': publish_version,
                'state': 'imported',
                'created_at': created_at,
                'artifact_dir': self.ring_artifact_dir,
                'rings': manifest_rings,
                'files': manifest_files,
                'import_results': import_results,
            }
            try:
                self.store.save_ring_version(manifest)
            except RingVersionReserved as err:
                raise RingImporterError(str(err))
            if set_latest:
                self.store.set_latest_ring_version(publish_version)
            public_manifest = self.store.get_ring_version_manifest(
                publish_version)
            if set_latest:
                public_manifest['latest'] = True
            public_manifest['import_status'] = 'created'
            return public_manifest
