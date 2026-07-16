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
import hashlib
import os
import tempfile

from urllib.parse import quote, unquote

from swift.common import exceptions as swift_exceptions
from swift.common.ring.builder import RingBuilder
from swift.common.swob import HTTPBadRequest, HTTPConflict, HTTPNoContent, \
    HTTPNotFound, HTTPNotImplemented, HTTPTemporaryRedirect
from swift.common.utils import config_true_value, md5, mkdirs
from swift.ring_manager import http
from swift.ring_manager.analysis import RingBuilderAnalysisError, \
    RingBuilderAnalyzer, get_query_list
from swift.ring_manager.builder import RingBuilderManagerConflict, \
    RingBuilderManagerError
from swift.ring_manager.publisher import RingBuilderPublisherError
from swift.ring_manager.common import NormalTimestamp
from swift.ring_manager.routing import Route
from swift.ring_manager.store import RingAlreadyExists, RingBuildNotFound, \
    RingBuildPublishedVersionConflict, RingBuildVersionConflict, \
    RingNotFound, RingVersionFileNotFound, RingVersionNotFound


RING_FIELDS = [
    'id',
    'background_ring_push',
    'cluster',
    'default',
    'deprecated',
    'disabled',
    'ever_pushed',
    'is_composite',
    'last_rebalance_time',
    'min_part_hours',
    'name',
    'num_replicas',
    'overload',
    'part_power',
    'policy_type',
    'rebalance_prohibited',
    'storage_policy_index',
    'target_adj_percent',
    'ec_multi_region_type',
    'rebalance_cork',
    'ec_num_parity_fragments',
    'ec_duplication_factor',
    'ec_object_segment_size',
    'ec_type',
    'ec_num_data_fragments',
    'resource_uri',
    'builder_files',
    'builder_path',
    'builder_version',
    'next_part_power',
    'partition_power_increase_state',
    'allowed_partition_power_actions',
    'device_count',
    'devices_url',
]

PARTITION_POWER_READONLY_FIELDS = set([
    'next_part_power',
    'partition_power_increase_state',
    'allowed_partition_power_actions',
])


class RingController(object):
    """Own ring-specific routes and their orchestration."""

    def __init__(self, store, builder_manager, ring_builder_dir, publisher,
                 ring_build_executor, build_pool, build_worker,
                 max_json_request_body_size,
                 max_partitions_at_risk_selectors,
                 file_iterable_factory=http.RingManagerFileIterable):
        self._store = store
        self._builder_manager = builder_manager
        self._ring_builder_dir = ring_builder_dir
        self._publisher = publisher
        self._ring_build_executor = ring_build_executor
        self._build_pool = build_pool
        self._build_worker = build_worker
        self._max_json_request_body_size = max_json_request_body_size
        self._max_partitions_at_risk_selectors = \
            max_partitions_at_risk_selectors
        self._file_iterable_factory = file_iterable_factory

    def routes(self):
        return [
            Route(r'^/api/v1/rings/schema/?$',
                  ('GET',), self.ring_schema),
            Route(r'^/api/v1/rings/?$',
                  ('GET', 'POST'), self.ring_list),
            Route(r'^/api/v1/rings/membership/device/'
                  r'(?P<device_id>[0-9]+)/?$',
                  ('GET',), self.ring_membership_device),
            Route(r'^/api/v1/rings/builds/?$',
                  ('GET',), self.ring_builds),
            Route(r'^/api/v1/rings/builds/(?P<build_id>[^/]+)/?$',
                  ('GET',), self.ring_build_detail),
            Route(r'^/api/v1/rings/releases/?$',
                  ('GET', 'POST'), self.ring_versions),
            Route(r'^/api/v1/rings/releases/latest/?$',
                  ('GET',), self.latest_ring_version),
            Route(r'^/api/v1/rings/releases/latest/manifest/?$',
                  ('GET',), self.latest_ring_version_manifest),
            Route(r'^/api/v1/rings/releases/latest/files/'
                  r'(?P<file_name>[^/]+)/?$',
                  ('GET',), self.latest_ring_version_file),
            Route(r'^/api/v1/rings/releases/(?P<version>[^/]+)/?$',
                  ('GET',), self.ring_version_detail),
            Route(r'^/api/v1/rings/releases/(?P<version>[^/]+)/manifest/?$',
                  ('GET',), self.ring_version_manifest),
            Route(r'^/api/v1/rings/releases/(?P<version>[^/]+)/files/'
                  r'(?P<file_name>[^/]+)/?$',
                  ('GET',), self.ring_version_file),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/versions/?$',
                  ('GET', 'POST'), self.ring_artifact_versions),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/versions/latest/?$',
                  ('GET',), self.latest_ring_artifact_version),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/versions/latest/'
                  r'files/(?P<file_name>[^/]+)/?$',
                  ('GET',), self.latest_ring_artifact_version_file),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/versions/'
                  r'(?P<version>[^/]+)/?$',
                  ('GET',), self.ring_artifact_version_detail),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/versions/'
                  r'(?P<version>[^/]+)/files/(?P<file_name>[^/]+)/?$',
                  ('GET',), self.ring_artifact_version_file),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/?$',
                  ('GET', 'PUT', 'PATCH', 'DELETE'), self.ring_detail),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/devices/?$',
                  ('GET', 'PUT'), self.ring_devices),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/devices/add/?$',
                  ('POST',), self.ring_devices_add),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/devices/remove/?$',
                  ('POST',), self.ring_devices_remove),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/'
                  r'partition_power_increase/'
                  r'(?P<action>prepare|increase|cancel|finish)/?$',
                  ('POST',), self.ring_partition_power_increase),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/builder/?$',
                  ('GET',), self.ring_builder),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/builder/file/?$',
                  ('GET',), self.ring_builder_file),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/parts/?$',
                  ('GET',), self.ring_parts),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/rebalance/?$',
                  ('GET',), self.ring_rebalance),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/dispersion/?$',
                  ('GET',), self.ring_dispersion),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/at_risk/?$',
                  ('GET',), self.ring_at_risk),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/count_parts/?$',
                  ('GET',), self.ring_count_parts),
            Route(r'^/api/v1/rings/(?P<ring_id>[^/]+)/'
                  r'partitions_at_risk/?$',
                  ('GET', 'POST'), self.ring_partitions_at_risk,
                  read_only_methods=('GET', 'HEAD', 'POST')),
        ]

    def _json_response(self, req, data, status=200, headers=None):
        return http.json_response(
            req, data, status=status, headers=headers)

    def _json_error(self, req, status_class, message):
        return http.json_error(req, status_class, message)

    def _json_request_body(self, req):
        return http.json_request_body(
            req, self._max_json_request_body_size)

    def _collection_response(self, req, objects):
        return http.collection_response(req, objects)

    def _artifact_file_response(self, req, file_info):
        return http.artifact_file_response(
            req, file_info, file_iterable_cls=self._file_iterable_factory)

    def _payload_values(self, payload, *names):
        values = []
        for name in names:
            if name not in payload:
                continue
            value = payload.get(name)
            if value in (None, ''):
                continue
            if isinstance(value, list):
                for item in value:
                    if item in (None, ''):
                        continue
                    if isinstance(item, (dict, list)):
                        raise ValueError(
                            '%s entries must be scalar values' % name)
                    values.append(item)
            elif isinstance(value, dict):
                raise ValueError(
                    '%s must be a scalar value or list' % name)
            else:
                values.append(value)
        return values

    def _payload_string_values(self, payload, *names):
        values = []
        for value in self._payload_values(payload, *names):
            if not isinstance(value, str):
                raise ValueError(
                    '%s entries must be strings' % (names[0],))
            values.append(value)
        return values

    def _check_partitions_at_risk_selector_count(
            self, node_ips, replication_ips, device_ids):
        selector_count = (
            len(node_ips) + len(replication_ips) + len(device_ids))
        if selector_count > self._max_partitions_at_risk_selectors:
            raise ValueError(
                'partitions_at_risk accepts at most %d down selectors; '
                'got %d' % (
                    self._max_partitions_at_risk_selectors, selector_count))

    def _request_bool_value(self, value, name):
        if value in (None, ''):
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            if value in (0, 1):
                return bool(value)
            raise ValueError('%s must be a boolean value' % name)
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in ('true', '1', 'yes', 'on', 't', 'y'):
                return True
            if lowered in ('false', '0', 'no', 'off', 'f', 'n'):
                return False
        raise ValueError('%s must be a boolean value' % name)

    def _devices_from_request_body(self, payload):
        devices = []
        if 'nodes' in payload:
            nodes = payload['nodes']
            if not isinstance(nodes, list) or not nodes:
                raise ValueError("'nodes' must be a non-empty list")
            for node in nodes:
                if not isinstance(node, dict):
                    raise ValueError("'nodes' entries must be objects")
                labels = node.get('device_labels')
                node_devices = node.get('devices')
                if labels is None and node_devices is None:
                    raise ValueError(
                        "'device_labels' or 'devices' is required")
                if labels is not None:
                    if not isinstance(labels, list) or not labels:
                        raise ValueError(
                            "'device_labels' must be a non-empty list")
                    for label in labels:
                        if not label:
                            raise ValueError(
                                "'device_labels' cannot contain empty values")
                        device = {'label': label}
                        if node.get('id') is not None:
                            device['node_id'] = node['id']
                        if node.get('resource_uri') is not None:
                            device['node'] = node['resource_uri']
                        devices.append(device)
                if node_devices is not None:
                    if not isinstance(node_devices, list) or not node_devices:
                        raise ValueError("'devices' must be a non-empty list")
                    node_keys = (
                        'region', 'zone', 'ip', 'port', 'replication_ip',
                        'replication_port')
                    for item in node_devices:
                        if not isinstance(item, dict):
                            raise ValueError(
                                "'devices' entries must be objects")
                        device_name = item.get('device', item.get('name'))
                        if not device_name:
                            raise ValueError(
                                "'devices' entries require device or name")
                        device = {}
                        if node.get('id') is not None:
                            device['node_id'] = node['id']
                        for key in node_keys:
                            if node.get(key) is not None:
                                device[key] = node[key]
                        device.update(dict(
                            (key, value) for key, value in item.items()
                            if key != 'name'))
                        device.setdefault('device', device_name)
                        device.setdefault('label', '%s:%s' % (
                            node['id'], device_name)
                            if node.get('id') is not None else device_name)
                        if (device.get('weight') is None and
                                node.get('weight') is not None):
                            device['weight'] = node['weight']
                        devices.append(device)
        elif 'devices' in payload:
            payload_devices = payload['devices']
            if not isinstance(payload_devices, list) or not payload_devices:
                raise ValueError("'devices' must be a non-empty list")
            for device in payload_devices:
                if isinstance(device, dict):
                    devices.append(device)
                elif device:
                    devices.append({'label': device})
                else:
                    raise ValueError("'devices' cannot contain empty values")
        else:
            raise ValueError("'nodes' or 'devices' is required")
        return devices

    def _not_implemented(self, req, name):
        return self._json_error(
            req, HTTPNotImplemented,
            '%s is not implemented in the initial ring-manager service' % name)

    def _get_ring(self, req, ring_id, hydrate_builder=False):
        try:
            ring = self._store.get_ring(ring_id)
        except RingNotFound:
            raise HTTPNotFound(request=req)
        if hydrate_builder:
            return self._hydrate_builder_metadata(ring)
        return ring

    def _device_collection_response(self, req, ring_id, page):
        next_marker = page.get('next_marker')
        next_path = None
        if next_marker:
            next_path = '/api/v1/rings/%s/devices/?marker=%s' % (
                quote(ring_id, safe=''), quote(next_marker, safe=''))
        devices = page.get('devices', [])
        return self._json_response(req, {
            'devices': devices,
            'meta': {
                'limit': len(devices),
                'next': next_path,
                'marker': next_marker,
                'total_count': page.get('total_count', len(devices)),
            },
        })

    def _check_partition_power_fields(self, metadata):
        readonly = sorted(
            key for key in metadata
            if key in PARTITION_POWER_READONLY_FIELDS)
        if readonly:
            raise ValueError(
                'Read-only ring fields may not be set: %s' %
                ', '.join(readonly))

    def _remember_builder_metadata(self, ring_id, ring, builder_path,
                                   device_count):
        updates = self._builder_metadata_updates(
            ring, builder_path, device_count)
        self._store.update_ring(ring_id, updates)

    def _builder_metadata_updates(self, ring, builder_path, device_count):
        updates = {'device_count': device_count}
        if builder_path and not (
                ring.get('builder_files') or ring.get('builder_path')):
            updates['builder_files'] = [builder_path]
        return updates

    def _hydrate_builder_metadata(self, ring):
        hydrated = copy.deepcopy(ring)
        hydrated.update(self._builder_manager.builder_info(hydrated))
        return hydrated

    def _analyzer(self, ring):
        return RingBuilderAnalyzer(ring, self._ring_builder_dir)

    def _non_negative_int_value(self, value, name):
        if isinstance(value, bool):
            raise ValueError('%s must be a non-negative integer' % name)
        if isinstance(value, int):
            parsed = value
        elif isinstance(value, str):
            try:
                parsed = int(value)
            except ValueError:
                raise ValueError(
                    '%s must be a non-negative integer' % name)
        else:
            raise ValueError('%s must be a non-negative integer' % name)
        if parsed < 0:
            raise ValueError('%s must be a non-negative integer' % name)
        return parsed

    def _builder_file_record(self, ring):
        return self._builder_file_record_from_path(
            ring, self._validated_builder_path(ring))

    def _validated_builder_path(self, ring):
        builder_path = self._builder_manager.builder_path(ring)
        root = os.path.realpath(self._ring_builder_dir)
        resolved = os.path.realpath(builder_path)
        try:
            common_path = os.path.commonpath([root, resolved])
        except ValueError:
            common_path = None
        if common_path != root:
            raise RingBuilderManagerError(
                'ring %s builder file is outside ring_builder_dir' %
                ring.get('id'))
        return resolved

    def _hash_file(self, path):
        checksum = hashlib.sha256()
        etag = md5(usedforsecurity=False)
        bytes_read = 0
        with open(path, 'rb') as fp:
            while True:
                chunk = fp.read(http.DEFAULT_FILE_CHUNK_SIZE)
                if not chunk:
                    break
                bytes_read += len(chunk)
                checksum.update(chunk)
                etag.update(chunk)
        return bytes_read, checksum.hexdigest(), etag.hexdigest()

    def _remove_snapshot(self, snapshot_path):
        http.remove_file(snapshot_path)

    def _snapshot_builder_file(self, builder_path):
        snapshot_dir = self._store.state_dir or tempfile.gettempdir()
        mkdirs(snapshot_dir)
        fd, snapshot_path = tempfile.mkstemp(
            prefix='.ring-manager-builder-snapshot-', dir=snapshot_dir)
        checksum = hashlib.sha256()
        etag = md5(usedforsecurity=False)
        bytes_written = 0
        try:
            with os.fdopen(fd, 'wb') as out_fp:
                with open(builder_path, 'rb') as in_fp:
                    while True:
                        chunk = in_fp.read(http.DEFAULT_FILE_CHUNK_SIZE)
                        if not chunk:
                            break
                        out_fp.write(chunk)
                        bytes_written += len(chunk)
                        checksum.update(chunk)
                        etag.update(chunk)
            return (snapshot_path, bytes_written, checksum.hexdigest(),
                    etag.hexdigest())
        except Exception:
            self._remove_snapshot(snapshot_path)
            raise

    def _builder_file_record_from_path(self, ring, resolved,
                                       snapshot_path=None,
                                       snapshot_bytes=None,
                                       snapshot_sha256=None,
                                       snapshot_md5=None):
        try:
            builder = RingBuilder.load(snapshot_path or resolved)
        except swift_exceptions.FileNotFoundError:
            raise
        except (swift_exceptions.PermissionError,
                swift_exceptions.UnPicklingError) as err:
            raise RingBuilderManagerError(
                'ring %s builder file could not be loaded: %s' %
                (ring.get('id'), err))
        except Exception as err:
            raise RingBuilderManagerError(
                'ring %s builder file could not be loaded: %s' %
                (ring.get('id'), err))
        if snapshot_path is None:
            stat_result = os.stat(resolved)
            bytes_read, sha256, etag = self._hash_file(resolved)
            if bytes_read == stat_result.st_size:
                bytes_read = stat_result.st_size
        else:
            bytes_read = snapshot_bytes
            sha256 = snapshot_sha256
            etag = snapshot_md5
        return {
            'path': snapshot_path or resolved,
            'unlink_on_close': snapshot_path is not None,
            'bytes': bytes_read,
            'md5': etag,
            'sha256': sha256,
            'builder_version': builder.version,
            'partition_power_increase':
                self._builder_manager.partition_power_increase_info(
                    ring, builder),
        }

    def _builder_file_snapshot_record(self, ring):
        resolved = self._validated_builder_path(ring)
        snapshot_path, snapshot_bytes, snapshot_sha256, snapshot_md5 = \
            self._snapshot_builder_file(resolved)
        try:
            return self._builder_file_record_from_path(
                ring, resolved, snapshot_path=snapshot_path,
                snapshot_bytes=snapshot_bytes,
                snapshot_sha256=snapshot_sha256,
                snapshot_md5=snapshot_md5)
        except Exception:
            self._remove_snapshot(snapshot_path)
            raise

    def _builder_file_response(self, req, record):
        return http.builder_file_response(
            req, record, remove_file_callback=self._remove_snapshot,
            file_iterable_cls=self._file_iterable_factory)

    def _apply_builder_settings(self, ring, builder_updates):
        builder_path, _builder, device_count = \
            self._builder_manager.update_builder_settings(
                ring, builder_updates)
        if builder_path is None:
            return {}
        return self._builder_metadata_updates(
            ring, builder_path, device_count)

    def ring_schema(self, req):
        fields = dict((name, {
            'nullable': True,
            'readonly': name in ('id', 'resource_uri', 'ever_pushed',
                                 'last_rebalance_time', 'builder_version',
                                 'next_part_power',
                                 'partition_power_increase_state',
                                 'allowed_partition_power_actions',
                                 'device_count', 'devices_url'),
            'type': 'string',
        }) for name in RING_FIELDS)
        return self._json_response(req, {
            'allowed_detail_http_methods': [
                'get', 'put', 'patch', 'delete'],
            'allowed_list_http_methods': ['get', 'post'],
            'fields': fields,
        })

    def ring_list(self, req):
        if req.method == 'POST':
            payload = self._json_request_body(req)
            self._check_partition_power_fields(payload)
            metadata, builder_updates = \
                self._builder_manager.split_builder_fields(payload)
            try:
                ring = self._store.create_ring(metadata)
            except RingAlreadyExists:
                return self._json_error(
                    req, HTTPConflict, 'Ring already exists')
            try:
                builder_metadata = self._apply_builder_settings(
                    ring, builder_updates)
                if builder_metadata:
                    ring = self._store.update_ring(
                        ring['id'], builder_metadata)
            except RingBuilderManagerError as err:
                try:
                    self._store.delete_ring(ring['id'])
                except RingNotFound:
                    pass
                return self._json_error(req, HTTPBadRequest, str(err))
            return self._json_response(
                req, self._hydrate_builder_metadata(ring), status=201)
        return self._collection_response(
            req, self._store.list_rings(req.params.get('cluster_id')))

    def ring_detail(self, req, ring_id):
        ring_id = unquote(ring_id)
        if req.method == 'DELETE':
            try:
                self._store.delete_ring(ring_id)
            except RingNotFound:
                return HTTPNotFound(request=req)
            return HTTPNoContent(request=req)
        if req.method in ('PUT', 'PATCH'):
            payload = self._json_request_body(req)
            self._check_partition_power_fields(payload)
            metadata, builder_updates = \
                self._builder_manager.split_builder_fields(payload)
            try:
                existing = self._store.get_ring(ring_id)
            except RingNotFound:
                return HTTPNotFound(request=req)
            if 'id' in metadata and str(metadata['id']) != str(ring_id):
                raise ValueError('Request body id does not match ring id')
            candidate = copy.deepcopy(metadata)
            if req.method == 'PATCH':
                candidate = copy.deepcopy(existing)
                candidate.update(metadata)
            candidate['id'] = existing['id']
            try:
                builder_metadata = self._apply_builder_settings(
                    candidate, builder_updates)
            except RingBuilderManagerError as err:
                return self._json_error(req, HTTPBadRequest, str(err))
            metadata.update(builder_metadata)
            try:
                ring = self._store.update_ring(
                    ring_id, metadata,
                    replace=req.method == 'PUT')
            except RingNotFound:
                return HTTPNotFound(request=req)
            return self._json_response(
                req, self._hydrate_builder_metadata(ring))
        ring = self._get_ring(req, ring_id, hydrate_builder=True)
        return self._json_response(req, ring)

    def ring_membership_device(self, req, device_id):
        return self._not_implemented(req, 'ring membership by device')

    def ring_devices(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id)
        if req.method == 'PUT':
            devices = self._devices_from_request_body(
                self._json_request_body(req))
            try:
                builder_path, devices, device_count = \
                    self._builder_manager.replace_devices(ring, devices)
                self._remember_builder_metadata(
                    ring_id, ring, builder_path, device_count)
            except RingBuilderManagerError as err:
                return self._json_error(req, HTTPBadRequest, str(err))
            return self._json_response(req, {
                'devices': devices,
                'device_count': device_count,
            })
        try:
            page = self._builder_manager.list_devices(
                ring, marker=req.params.get('marker'),
                limit=req.params.get('limit', 1000),
                include_removed=config_true_value(
                    req.params.get('include_removed', 'false')))
        except RingBuilderManagerError as err:
            return self._json_error(req, HTTPBadRequest, str(err))
        return self._device_collection_response(req, ring_id, page)

    def ring_devices_add(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id)
        devices = self._devices_from_request_body(self._json_request_body(req))
        try:
            builder_path, added, device_count = \
                self._builder_manager.add_devices(ring, devices)
            self._remember_builder_metadata(
                ring_id, ring, builder_path, device_count)
        except RingBuilderManagerError as err:
            return self._json_error(req, HTTPBadRequest, str(err))
        return self._json_response(req, {
            'action': 'add_rings',
            'ring': ring.get('name'),
            'devices': added,
            'device_count': device_count,
        })

    def ring_devices_remove(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id)
        devices = self._devices_from_request_body(self._json_request_body(req))
        try:
            builder_path, removed, device_count = \
                self._builder_manager.remove_devices(ring, devices)
            self._remember_builder_metadata(
                ring_id, ring, builder_path, device_count)
        except RingBuilderManagerError as err:
            return self._json_error(req, HTTPBadRequest, str(err))
        return self._json_response(req, {
            'action': 'remove_rings',
            'ring': ring.get('name'),
            'devices': removed,
            'device_count': device_count,
        })

    def _active_ring_build_conflict(self, ring_id):
        active = self._store.active_ring_builds_for_ring(ring_id)
        if active:
            build = active[0]
            return 'ring %s has active ring build %s in state %s' % (
                ring_id, build.get('id'), build.get('state', 'unknown'))
        return None

    def ring_partition_power_increase(self, req, ring_id, action):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id)
        if action in ('prepare', 'increase'):
            conflict = self._active_ring_build_conflict(ring_id)
            if conflict is not None:
                return self._json_error(req, HTTPConflict, conflict)
        try:
            builder_path, _builder, device_count = \
                self._builder_manager.change_partition_power_increase(
                    ring, action)
            self._remember_builder_metadata(
                ring_id, ring, builder_path, device_count)
        except RingBuilderManagerConflict as err:
            return self._json_error(req, HTTPConflict, str(err))
        except RingBuilderManagerError as err:
            return self._json_error(req, HTTPBadRequest, str(err))
        ring = self._store.get_ring(ring_id)
        response = self._hydrate_builder_metadata(ring)
        response.update({
            'action': action,
            'requires_publish': True,
        })
        return self._json_response(req, response)

    def ring_versions(self, req):
        if req.method == 'POST':
            try:
                payload = self._json_request_body(req)
                if config_true_value(str(
                        payload.get('artifact_only', 'false'))):
                    raise RingBuilderPublisherError(
                        'artifact-only builds must use '
                        '/api/v1/rings/<ring_id>/versions/')
                self._validate_release_build_request(payload)
                build = self._enqueue_ring_build(payload)
            except RingNotFound:
                return HTTPNotFound(request=req)
            except RingBuildVersionConflict as err:
                return self._json_error(req, HTTPConflict, str(err))
            except RingBuildPublishedVersionConflict as err:
                return self._json_error(req, HTTPBadRequest, str(err))
            except (RingBuilderPublisherError,
                    RingBuilderManagerError) as err:
                return self._json_error(req, HTTPBadRequest, str(err))
            return self._json_response(
                req, build, status=202,
                headers={'Location': build['resource_uri']})
        return self._collection_response(
            req,
            self._store.list_ring_versions(req.params.get('cluster_id')))

    def ring_artifact_versions(self, req, ring_id):
        ring_id = unquote(ring_id)
        if req.method == 'POST':
            try:
                payload = self._json_request_body(req)
                if payload.get('version') is not None:
                    raise RingBuilderPublisherError(
                        'artifact-only builds do not accept version; the '
                        'Swift builder version identifies the resulting '
                        'ring artifact')
                payload['ring_id'] = ring_id
                payload['rings'] = [ring_id]
                payload['artifact_only'] = True
                self._store.get_ring(ring_id)
                build = self._enqueue_ring_build(payload)
            except RingNotFound:
                return HTTPNotFound(request=req)
            except RingBuildVersionConflict as err:
                return self._json_error(req, HTTPConflict, str(err))
            except RingBuildPublishedVersionConflict as err:
                return self._json_error(req, HTTPBadRequest, str(err))
            except (RingBuilderPublisherError,
                    RingBuilderManagerError) as err:
                return self._json_error(req, HTTPBadRequest, str(err))
            return self._json_response(
                req, build, status=202,
                headers={'Location': build['resource_uri']})
        try:
            versions = self._store.list_ring_artifact_versions(ring_id)
        except RingNotFound:
            return HTTPNotFound(request=req)
        return self._collection_response(req, versions)

    def _validate_release_build_request(self, payload):
        ring_ids = payload.get('rings')
        if ring_ids is None:
            return
        if not isinstance(ring_ids, list) or not ring_ids:
            raise RingBuilderPublisherError('rings must be a non-empty list')
        seen = set()
        for ring_id in ring_ids:
            ring = self._store.get_ring(ring_id)
            if self._store.ring_is_disabled(ring):
                raise RingBuilderPublisherError(
                    'ring %s is disabled; enable it before publishing' %
                    ring['id'])
            if str(ring['id']) in seen:
                raise RingBuilderPublisherError(
                    'rings contains duplicate ring ids')
            seen.add(str(ring['id']))

    def _enqueue_ring_build(self, payload):
        build = self._store.create_ring_build(
            payload, NormalTimestamp.now().internal)
        if self._ring_build_executor == 'manager':
            self._build_pool.spawn_n(self._build_worker.process_jobs)
        return build

    def ring_builds(self, req):
        return self._collection_response(req, self._store.list_ring_builds())

    def ring_build_detail(self, req, build_id):
        try:
            build = self._store.get_ring_build(unquote(build_id))
        except RingBuildNotFound:
            return HTTPNotFound(request=req)
        return self._json_response(req, build)

    def latest_ring_artifact_version(self, req, ring_id):
        ring_id = unquote(ring_id)
        try:
            version = self._store.get_ring_artifact_version(ring_id, 'latest')
        except (RingNotFound, RingVersionNotFound):
            return HTTPNotFound(request=req)
        return self._json_response(req, version)

    def ring_artifact_version_detail(self, req, ring_id, version):
        ring_id = unquote(ring_id)
        try:
            version = self._store.get_ring_artifact_version(
                ring_id, unquote(version))
        except (RingNotFound, RingVersionNotFound):
            return HTTPNotFound(request=req)
        return self._json_response(req, version)

    def latest_ring_artifact_version_file(self, req, ring_id, file_name):
        ring_id = unquote(ring_id)
        try:
            concrete_version = \
                self._store.get_concrete_ring_artifact_version_id(
                    ring_id, 'latest')
        except (RingNotFound, RingVersionNotFound):
            return HTTPNotFound(request=req)
        location = '/api/v1/rings/%s/versions/%s/files/%s' % (
            quote(ring_id, safe=''),
            quote(concrete_version, safe=''),
            quote(file_name, safe=''))
        return HTTPTemporaryRedirect(
            request=req, headers={'Location': location})

    def ring_artifact_version_file(self, req, ring_id, version, file_name):
        ring_id = unquote(ring_id)
        try:
            _version, file_info = self._store.get_ring_artifact_version_file(
                ring_id, unquote(version), unquote(file_name))
        except (RingNotFound, RingVersionNotFound, RingVersionFileNotFound):
            return HTTPNotFound(request=req)
        return self._artifact_file_response(req, file_info)

    def latest_ring_version(self, req):
        try:
            version = self._store.get_ring_version('latest')
        except RingVersionNotFound:
            return HTTPNotFound(request=req)
        return self._json_response(req, version)

    def ring_version_detail(self, req, version):
        try:
            version = self._store.get_ring_version(unquote(version))
        except RingVersionNotFound:
            return HTTPNotFound(request=req)
        return self._json_response(req, version)

    def latest_ring_version_manifest(self, req):
        return self.ring_version_manifest(req, 'latest')

    def ring_version_manifest(self, req, version):
        try:
            manifest = self._store.get_ring_version_manifest(
                unquote(version))
        except RingVersionNotFound:
            return HTTPNotFound(request=req)
        return self._json_response(req, manifest)

    def latest_ring_version_file(self, req, file_name):
        try:
            concrete_version = self._store.get_concrete_ring_version_id(
                'latest')
        except RingVersionNotFound:
            return HTTPNotFound(request=req)
        location = '/api/v1/rings/releases/%s/files/%s' % (
            quote(concrete_version, safe=''), quote(file_name, safe=''))
        return HTTPTemporaryRedirect(
            request=req, headers={'Location': location})

    def ring_version_file(self, req, version, file_name):
        try:
            _version, file_info = self._store.get_ring_version_file(
                unquote(version), unquote(file_name))
        except (RingVersionNotFound, RingVersionFileNotFound):
            return HTTPNotFound(request=req)
        return self._artifact_file_response(req, file_info)

    def ring_builder(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id)
        try:
            record = self._builder_file_record(ring)
        except swift_exceptions.FileNotFoundError:
            return HTTPNotFound(request=req)
        except RingBuilderManagerError:
            return self._json_error(
                req, HTTPBadRequest, 'Ring builder file is unavailable')
        except IOError as err:
            if err.errno in (errno.ENOENT, errno.EACCES, errno.EPERM):
                return HTTPNotFound(request=req)
            raise
        return self._json_response(req, {
            'ring_id': ring_id,
            'builder_version': record['builder_version'],
            'next_part_power':
                record['partition_power_increase']['next_part_power'],
            'partition_power_increase_state':
                record['partition_power_increase'][
                    'partition_power_increase_state'],
            'allowed_partition_power_actions':
                record['partition_power_increase'][
                    'allowed_partition_power_actions'],
            'latest_swift_ring_version': ring.get(
                'latest_swift_ring_version'),
            'file': {
                'bytes': record['bytes'],
                'md5': record['md5'],
                'sha256': record['sha256'],
                'url': '/api/v1/rings/%s/builder/file/' %
                quote(str(ring_id), safe=''),
            },
        })

    def ring_builder_file(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id)
        try:
            record = self._builder_file_snapshot_record(ring)
        except swift_exceptions.FileNotFoundError:
            return HTTPNotFound(request=req)
        except RingBuilderManagerError:
            return self._json_error(
                req, HTTPBadRequest, 'Ring builder file is unavailable')
        except IOError as err:
            if err.errno in (errno.ENOENT, errno.EACCES, errno.EPERM):
                return HTTPNotFound(request=req)
            raise
        return self._builder_file_response(req, record)

    def ring_parts(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id, hydrate_builder=True)
        return self._json_response(req, self._analyzer(ring).parts())

    def ring_rebalance(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id, hydrate_builder=True)
        return self._json_response(
            req, self._analyzer(ring).rebalance_status())

    def ring_dispersion(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id, hydrate_builder=True)
        return self._json_response(
            req, self._analyzer(ring).dispersion(
                req.params.get('level', 'zone')))

    def ring_at_risk(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id, hydrate_builder=True)
        return self._json_response(req, self._analyzer(ring).at_risk())

    def ring_count_parts(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id, hydrate_builder=True)
        risk_count = req.params.get('risk_count')
        if risk_count is not None:
            risk_count = int(risk_count)
        return self._json_response(
            req, self._analyzer(ring).count_parts(
                get_query_list(req, 'replication_ip'), risk_count))

    def ring_partitions_at_risk(self, req, ring_id):
        ring_id = unquote(ring_id)
        ring = self._get_ring(req, ring_id)
        payload = {}
        try:
            if req.method == 'POST':
                payload = self._json_request_body(req)
            risk_count_value = payload.get(
                'risk_count', req.params.get('risk_count'))
            if risk_count_value in (None, ''):
                risk_count = None
            else:
                risk_count = self._non_negative_int_value(
                    risk_count_value, 'risk_count')
            device_ids = [
                self._non_negative_int_value(value, 'device_id')
                for value in get_query_list(req, 'device_id')]
            device_ids.extend([
                self._non_negative_int_value(value, 'device_id')
                for value in self._payload_values(
                    payload, 'device_id', 'device_ids')])
            node_ips = [
                value for value in
                get_query_list(req, 'node_ip') + get_query_list(req, 'ip')
                if value]
            node_ips.extend(self._payload_string_values(
                payload, 'node_ip', 'node_ips', 'ip', 'ips'))
            replication_ips = [
                value for value in get_query_list(req, 'replication_ip')
                if value]
            replication_ips.extend(self._payload_string_values(
                payload, 'replication_ip', 'replication_ips'))
            self._check_partitions_at_risk_selector_count(
                node_ips, replication_ips, device_ids)
        except ValueError as err:
            return self._json_error(req, HTTPBadRequest, str(err))
        if not any((node_ips, replication_ips, device_ids)):
            return self._json_error(
                req, HTTPBadRequest,
                'partitions_at_risk requires at least one down selector')
        try:
            include_partitions = self._request_bool_value(
                payload.get('details', req.params.get('details', 'false')),
                'details')
        except ValueError as err:
            return self._json_error(req, HTTPBadRequest, str(err))
        try:
            analysis = self._analyzer(ring).partitions_at_risk(
                node_ips=node_ips,
                replication_ips=replication_ips,
                device_ids=device_ids,
                risk_count=risk_count,
                include_partitions=include_partitions)
        except RingBuilderAnalysisError as err:
            return self._json_error(req, HTTPConflict, str(err))
        return self._json_response(req, analysis)
