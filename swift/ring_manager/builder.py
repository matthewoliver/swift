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
import math
import os
import stat
import uuid

from swift.common import exceptions as swift_exceptions
from swift.common.ring.builder import RingBuilder
from swift.common.ring.utils import validate_and_normalize_address, \
    validate_device_name
from swift.common.utils import fsync, fsync_dir, lock_file, mkdirs
from swift.ring_manager.common import DEFAULT_BUILDER_LOCK_TIMEOUT


class RingBuilderManagerError(Exception):
    pass


DEFAULT_MAX_EXPLICIT_DEVICE_ID = 1000000


def _ensure_directory_durable(directory):
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


def _temporary_builder_file(builder_path):
    directory = os.path.dirname(builder_path) or '.'
    basename = os.path.basename(builder_path)
    return os.path.join(
        directory, '.%s.%s.tmp' % (basename, uuid.uuid4().hex))


def _existing_file_mode(path):
    try:
        return stat.S_IMODE(os.stat(path).st_mode)
    except OSError as err:
        if err.errno == errno.ENOENT:
            return None
        raise


def save_builder_durable(builder, builder_path):
    directory = os.path.dirname(builder_path) or '.'
    _ensure_directory_durable(directory)
    temp_path = _temporary_builder_file(builder_path)
    existing_mode = _existing_file_mode(builder_path)
    try:
        builder.save(temp_path)
        if existing_mode is not None:
            os.chmod(temp_path, existing_mode)
        with open(temp_path, 'rb') as fp:
            fsync(fp.fileno())
        os.rename(temp_path, builder_path)
        temp_path = None
        fsync_dir(directory)
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except OSError as err:
                if err.errno != errno.ENOENT:
                    raise


class RingBuilderManager(object):
    """
    Builder-backed device and topology helpers for one ring-manager service.

    Ring JSON stores small logical metadata. Swift builder files remain the
    source of truth for desired device topology.
    """

    DEVICE_FIELDS = (
        'id', 'region', 'zone', 'ip', 'port', 'replication_ip',
        'replication_port', 'device', 'weight', 'parts', 'meta')
    BUILDER_FIELDS = (
        'part_power', 'num_replicas', 'min_part_hours', 'overload')

    def __init__(self, ring_builder_dir=None,
                 max_explicit_device_id=DEFAULT_MAX_EXPLICIT_DEVICE_ID,
                 builder_lock_timeout=DEFAULT_BUILDER_LOCK_TIMEOUT):
        self.ring_builder_dir = ring_builder_dir
        if isinstance(max_explicit_device_id, bool):
            raise RingBuilderManagerError(
                'max_explicit_device_id must be a positive integer')
        if isinstance(max_explicit_device_id, int):
            pass
        elif (isinstance(max_explicit_device_id, str) and
              max_explicit_device_id.isdigit()):
            max_explicit_device_id = int(max_explicit_device_id)
        else:
            raise RingBuilderManagerError(
                'max_explicit_device_id must be a positive integer')
        if max_explicit_device_id <= 0:
            raise RingBuilderManagerError(
                'max_explicit_device_id must be a positive integer')
        self.max_explicit_device_id = max_explicit_device_id
        self.builder_lock_timeout = builder_lock_timeout

    def _join_builder_dir(self, ring, path):
        if os.path.isabs(path):
            return path
        root = ring.get('builder_dir') or self.ring_builder_dir
        if root:
            return os.path.join(root, path)
        return path

    def _default_builder_name(self, ring):
        ring_id = ring.get('id')
        ring_type = ring.get('ring_type') or ring.get('type')
        if ring_type == 'account' or ring_id == 'account':
            return 'account.builder'
        if ring_type == 'container' or ring_id == 'container':
            return 'container.builder'
        policy_index = ring.get('storage_policy_index')
        if policy_index in (None, '', 0, '0'):
            return 'object.builder'
        return 'object-%s.builder' % policy_index

    def builder_path(self, ring):
        files = ring.get('builder_files')
        if files:
            if not isinstance(files, list) or len(files) != 1:
                raise RingBuilderManagerError(
                    'ring %s requires exactly one builder file' %
                    ring.get('id'))
            return self._join_builder_dir(ring, files[0])
        if ring.get('builder_path'):
            return self._join_builder_dir(ring, ring['builder_path'])
        return self._join_builder_dir(ring, self._default_builder_name(ring))

    def _required_ring_value(self, ring, key):
        value = ring.get(key)
        if value in (None, ''):
            raise RingBuilderManagerError(
                'ring %s requires %s to create a builder' %
                (ring.get('id'), key))
        return value

    def _builder_parameter_error(self, ring, message):
        return RingBuilderManagerError(
            'ring %s has invalid builder parameters: %s' %
            (ring.get('id'), message))

    def _ascii_decimal_int(self, value):
        if (not isinstance(value, str) or not value or
                not all('0' <= char <= '9' for char in value)):
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def _builder_non_negative_int_field(self, ring, field, value):
        parsed = None
        if isinstance(value, bool):
            pass
        elif isinstance(value, int):
            parsed = value
        elif isinstance(value, float):
            if math.isfinite(value) and value.is_integer():
                parsed = int(value)
        elif isinstance(value, str):
            parsed = self._ascii_decimal_int(value)
        if parsed is None or parsed < 0:
            raise self._builder_parameter_error(
                ring, '%s must be a non-negative integer' % field)
        return parsed

    def _builder_float_field(self, ring, field, value, minimum, minimum_text):
        if isinstance(value, bool):
            valid = False
        else:
            try:
                value = float(value)
                valid = math.isfinite(value) and value >= minimum
            except (TypeError, ValueError, OverflowError):
                valid = False
        if not valid:
            raise self._builder_parameter_error(
                ring, '%s must be a finite number %s' %
                (field, minimum_text))
        return value

    def _builder_replicas_field(self, ring, value):
        return self._builder_float_field(
            ring, 'num_replicas', value, 1, 'at least 1')

    def _ensure_replica_slots_fit(self, ring, builder, replicas):
        try:
            int(builder.parts * replicas)
        except (OverflowError, ValueError):
            raise self._builder_parameter_error(
                ring, 'num_replicas creates too many replica slots')

    def _builder_overload_field(self, ring, value):
        return self._builder_float_field(
            ring, 'overload', value, 0, 'greater than or equal to 0')

    def new_builder(self, ring):
        part_power = self._builder_non_negative_int_field(
            ring, 'part_power',
            self._required_ring_value(ring, 'part_power'))
        replicas = self._builder_replicas_field(
            ring, self._required_ring_value(ring, 'num_replicas'))
        min_part_hours = self._builder_non_negative_int_field(
            ring, 'min_part_hours',
            self._required_ring_value(ring, 'min_part_hours'))
        overload = ring.get('overload')
        if overload is not None:
            overload = self._builder_overload_field(ring, overload)
        try:
            builder = RingBuilder(part_power, replicas, min_part_hours)
            self._ensure_replica_slots_fit(ring, builder, replicas)
            if overload is not None:
                builder.set_overload(overload)
        except (TypeError, ValueError) as err:
            raise RingBuilderManagerError(
                'ring %s has invalid builder parameters: %s' %
                (ring.get('id'), err))
        return builder

    def load_builder(self, ring):
        builder_path = self.builder_path(ring)
        try:
            builder = RingBuilder.load(builder_path)
        except (swift_exceptions.PermissionError,
                swift_exceptions.UnPicklingError) as err:
            raise RingBuilderManagerError(
                'ring %s builder %s could not be loaded: %s' %
                (ring.get('id'), builder_path, err))
        return builder_path, builder

    def load_or_create_builder(self, ring):
        try:
            return self.load_builder(ring)
        except swift_exceptions.FileNotFoundError:
            return self.builder_path(ring), self.new_builder(ring)

    def _builder_lock_path(self, builder_path):
        return '%s.lock' % builder_path

    def _locked_builder_mutation(self, ring, func):
        builder_path = self.builder_path(ring)
        directory = os.path.dirname(builder_path)
        if directory:
            mkdirs(directory)
        try:
            with lock_file(
                    self._builder_lock_path(builder_path),
                    timeout=self.builder_lock_timeout, unlink=False):
                return func()
        except swift_exceptions.LockTimeout:
            raise RingBuilderManagerError(
                'timed out waiting for builder lock for ring %s' %
                ring.get('id'))

    def split_builder_fields(self, payload):
        metadata = copy.deepcopy(payload)
        builder_updates = {}
        for key in self.BUILDER_FIELDS:
            if key in metadata:
                builder_updates[key] = metadata.pop(key)
        return metadata, builder_updates

    def strip_builder_fields(self, ring):
        ring = copy.deepcopy(ring)
        ring.pop('devices', None)
        for key in self.BUILDER_FIELDS:
            ring.pop(key, None)
        return ring

    def builder_info(self, ring):
        try:
            builder_path, builder = self.load_builder(ring)
        except swift_exceptions.FileNotFoundError:
            return {}
        info = {
            'part_power': builder.part_power,
            'num_replicas': builder.replicas,
            'min_part_hours': builder.min_part_hours,
            'overload': builder.overload,
            'builder_version': builder.version,
            'device_count': self.active_device_count(builder),
        }
        if ring.get('builder_files'):
            info['builder_files'] = ring['builder_files']
        elif not ring.get('builder_path'):
            info['builder_files'] = [builder_path]
        return info

    def apply_ring_settings(self, ring, builder):
        updates = dict((key, ring[key]) for key in self.BUILDER_FIELDS
                       if key in ring)
        return self.apply_builder_settings(ring, builder, updates)

    def apply_builder_settings(self, ring, builder, updates):
        if updates.get('part_power') is not None:
            desired_part_power = self._builder_non_negative_int_field(
                ring, 'part_power', updates['part_power'])
            if builder.part_power != desired_part_power:
                raise RingBuilderManagerError(
                    'ring %s builder part_power is %s, metadata wants %s' %
                    (ring.get('id'), builder.part_power,
                     desired_part_power))
        if updates.get('num_replicas') is not None:
            replicas = self._builder_replicas_field(
                ring, updates['num_replicas'])
            self._ensure_replica_slots_fit(ring, builder, replicas)
            try:
                builder.set_replicas(replicas)
            except (OverflowError, ValueError):
                raise self._builder_parameter_error(
                    ring, 'num_replicas creates too many replica slots')
        if updates.get('min_part_hours') is not None:
            min_part_hours = self._builder_non_negative_int_field(
                ring, 'min_part_hours', updates['min_part_hours'])
            builder.change_min_part_hours(min_part_hours)
        if updates.get('overload') is not None:
            overload = self._builder_overload_field(
                ring, updates['overload'])
            builder.set_overload(overload)

    def update_builder_settings(self, ring, updates):
        if not updates:
            return None, None, None
        return self._locked_builder_mutation(
            ring, lambda: self._update_builder_settings_locked(
                ring, updates))

    def _update_builder_settings_locked(self, ring, updates):
        try:
            builder_path, builder = self.load_builder(ring)
        except swift_exceptions.FileNotFoundError:
            builder_state = copy.deepcopy(ring)
            builder_state.update(updates)
            builder_path = self.builder_path(builder_state)
            builder = self.new_builder(builder_state)
        else:
            self.apply_builder_settings(ring, builder, updates)
        self.save_builder(builder, builder_path)
        return builder_path, builder, self.active_device_count(builder)

    def save_builder(self, builder, builder_path):
        save_builder_durable(builder, builder_path)

    def _device_key(self, dev):
        return (
            str(dev.get('ip')), int(dev.get('port')), str(dev.get('device')))

    def _selector_matches(self, dev, selector):
        if selector.get('id') is not None:
            try:
                if int(selector['id']) == int(dev['id']):
                    return True
            except (TypeError, ValueError):
                return False
        if selector.get('label') is not None:
            label = str(selector['label'])
            if label in (str(dev.get('label', '')), str(dev.get('meta', ''))):
                return True
        keys = [key for key in ('ip', 'port', 'device')
                if selector.get(key) is not None]
        if keys and all(str(selector[key]) == str(dev.get(key))
                        for key in keys):
            return True
        return False

    def _device_description(self, device):
        return device.get('label', device)

    def _non_negative_int_field(self, ring, device, field):
        value = device.get(field)
        if isinstance(value, bool):
            valid = False
        elif isinstance(value, int):
            valid = value >= 0
        elif isinstance(value, str):
            value = self._ascii_decimal_int(value)
            if value is None:
                valid = False
            else:
                valid = True
        else:
            valid = False
        if not valid:
            raise RingBuilderManagerError(
                'ring %s device %s field %s must be a non-negative '
                'integer' %
                (ring.get('id'), self._device_description(device), field))
        return int(value)

    def _explicit_device_id_field(self, ring, device):
        dev_id = self._non_negative_int_field(ring, device, 'id')
        if dev_id > self.max_explicit_device_id:
            raise RingBuilderManagerError(
                'ring %s device %s field id must be less than or equal '
                'to max_explicit_device_id %s' %
                (ring.get('id'), self._device_description(device),
                 self.max_explicit_device_id))
        return dev_id

    def _tcp_port_field(self, ring, device, field):
        value = self._non_negative_int_field(ring, device, field)
        if value < 1 or value > 65535:
            raise RingBuilderManagerError(
                'ring %s device %s field %s must be an integer from '
                '1 to 65535' %
                (ring.get('id'), self._device_description(device), field))
        return value

    def _weight_field(self, ring, device):
        value = device.get('weight')
        if isinstance(value, bool):
            valid = False
        else:
            try:
                value = float(value)
                valid = math.isfinite(value) and value >= 0
            except (TypeError, ValueError):
                valid = False
        if not valid:
            raise RingBuilderManagerError(
                'ring %s device %s field weight must be a finite '
                'non-negative number' %
                (ring.get('id'), self._device_description(device)))
        return value

    def _address_field(self, ring, device, field):
        value = device.get(field)
        if not isinstance(value, str) or not value:
            raise RingBuilderManagerError(
                'ring %s device %s field %s must be a non-empty address' %
                (ring.get('id'), self._device_description(device), field))
        try:
            return validate_and_normalize_address(value)
        except ValueError as err:
            raise RingBuilderManagerError(
                'ring %s device %s field %s has invalid address: %s' %
                (ring.get('id'), self._device_description(device), field,
                 err))

    def _device_name_field(self, ring, device):
        value = device.get('device')
        if not isinstance(value, str):
            raise RingBuilderManagerError(
                'ring %s device %s field device must be a string' %
                (ring.get('id'), self._device_description(device)))
        if not validate_device_name(value):
            raise RingBuilderManagerError(
                'ring %s device %s field device is invalid' %
                (ring.get('id'), self._device_description(device)))
        if value in ('.', '..') or '/' in value or '\\' in value:
            raise RingBuilderManagerError(
                'ring %s device %s field device must be a device name, '
                'not a path' %
                (ring.get('id'), self._device_description(device)))
        if any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise RingBuilderManagerError(
                'ring %s device %s field device must not contain control '
                'characters' %
                (ring.get('id'), self._device_description(device)))
        return value

    def normalize_device(self, ring, device):
        if not isinstance(device, dict):
            raise RingBuilderManagerError(
                'ring %s devices must be objects' % ring.get('id'))
        required = ('region', 'zone', 'ip', 'port', 'device', 'weight')
        missing = [key for key in required if device.get(key) in (None, '')]
        if missing:
            raise RingBuilderManagerError(
                'ring %s device %s is missing required key(s): %s' %
                (ring.get('id'), device.get('label', device),
                 ', '.join(missing)))
        port = self._tcp_port_field(ring, device, 'port')
        replication_ip = device['ip'] if device.get('replication_ip') is None \
            else device.get('replication_ip')
        replication_port = port \
            if device.get('replication_port') is None \
            else device.get('replication_port')
        replication_address = {
            'label': self._device_description(device),
            'replication_ip': replication_ip,
        }
        replication_port_device = {
            'label': self._device_description(device),
            'replication_port': replication_port,
        }
        normalized = {
            'ip': self._address_field(ring, device, 'ip'),
            'device': self._device_name_field(ring, device),
            'replication_ip': self._address_field(
                ring, replication_address, 'replication_ip'),
            'meta': str(device.get('meta') or device.get('label') or ''),
        }
        normalized.update({
            'region': self._non_negative_int_field(ring, device, 'region'),
            'zone': self._non_negative_int_field(ring, device, 'zone'),
            'port': port,
            'weight': self._weight_field(ring, device),
            'replication_port': self._tcp_port_field(
                ring, replication_port_device, 'replication_port'),
        })
        if 'id' in device and device.get('id') is not None:
            normalized['id'] = self._explicit_device_id_field(ring, device)
        return normalized

    def normalize_devices(self, ring, devices):
        return [self.normalize_device(ring, device) for device in devices]

    def _check_desired_device_ids(self, ring, desired, builder,
                                  existing_by_key=None):
        existing_by_key = existing_by_key or {}
        pending = self._pending_remove_ids(builder)
        existing_by_id = {}
        for dev in builder.devs:
            if dev is not None:
                existing_by_id[int(dev['id'])] = dev
        desired_by_id = {}
        for dev in desired:
            if 'id' not in dev:
                continue
            dev_id = int(dev['id'])
            key = self._device_key(dev)
            prior_key = desired_by_id.get(dev_id)
            if prior_key is not None and prior_key != key:
                raise RingBuilderManagerError(
                    'ring %s has duplicate desired device id %s' %
                    (ring.get('id'), dev_id))
            desired_by_id[dev_id] = key
            existing = existing_by_key.get(key)
            if existing is not None and int(existing['id']) not in pending:
                if int(existing['id']) != dev_id:
                    raise RingBuilderManagerError(
                        'ring %s device %s explicit id %s does not match '
                        'existing id %s' %
                        (ring.get('id'), key, dev_id, existing['id']))
                continue
            if dev_id in existing_by_id:
                raise RingBuilderManagerError(
                    'ring %s device %s has duplicate id %s' %
                    (ring.get('id'), key, dev_id))

    def _sync_existing_dev(self, builder, existing, desired):
        changed = False
        for key in ('region', 'zone', 'weight'):
            if existing.get(key) != desired[key]:
                changed = True
        for key in ('ip', 'port', 'replication_ip', 'replication_port',
                    'device', 'meta'):
            if existing.get(key) != desired[key]:
                changed = True
        if not changed:
            return
        existing.update((key, desired[key]) for key in (
            'region', 'zone', 'ip', 'port', 'replication_ip',
            'replication_port', 'device', 'weight', 'meta'))
        builder.devs_changed = True
        builder.version += 1
        builder._ring = None

    def _pending_remove_ids(self, builder):
        return set(dev['id'] for dev in builder._remove_devs if dev)

    def _public_device(self, dev, pending_removal=False):
        public = dict((key, copy.deepcopy(dev[key]))
                      for key in self.DEVICE_FIELDS if key in dev)
        if public.get('meta') and 'label' not in public:
            public['label'] = public['meta']
        if pending_removal:
            public['pending_removal'] = True
        return public

    def _active_devs(self, builder):
        pending = self._pending_remove_ids(builder)
        for dev in builder.devs:
            if not dev or dev['id'] in pending:
                continue
            yield dev

    def active_device_count(self, builder):
        return sum(1 for _dev in self._active_devs(builder))

    def list_devices(self, ring, marker=None, limit=1000,
                     include_removed=False):
        try:
            _builder_path, builder = self.load_builder(ring)
        except swift_exceptions.FileNotFoundError:
            return {
                'devices': [],
                'total_count': 0,
                'next_marker': None,
            }
        pending = self._pending_remove_ids(builder)
        devices = []
        for dev in builder.devs:
            if not dev:
                continue
            if not include_removed and dev['id'] in pending:
                continue
            devices.append(self._public_device(
                dev, pending_removal=dev['id'] in pending))
        devices.sort(key=lambda dev: int(dev.get('id', -1)))
        if marker not in (None, ''):
            try:
                marker = int(marker)
            except (TypeError, ValueError):
                raise RingBuilderManagerError('marker must be an integer')
            devices = [dev for dev in devices if int(dev['id']) > marker]
        total_count = len(devices)
        if limit is not None:
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                raise RingBuilderManagerError('limit must be an integer')
            if limit < 0:
                raise RingBuilderManagerError('limit must be non-negative')
            devices = devices[:limit]
        next_marker = None
        if limit is not None and len(devices) < total_count and devices:
            next_marker = str(devices[-1]['id'])
        return {
            'devices': devices,
            'total_count': total_count,
            'next_marker': next_marker,
        }

    def add_devices(self, ring, devices):
        return self._locked_builder_mutation(
            ring, lambda: self._add_devices_locked(ring, devices))

    def _add_devices_locked(self, ring, devices):
        builder_path, builder = self.load_or_create_builder(ring)
        desired = self.normalize_devices(ring, devices)
        existing = dict((self._device_key(dev), dev)
                        for dev in self._active_devs(builder))
        self._check_desired_device_ids(
            ring, desired, builder, existing_by_key=existing)
        added = []
        for dev in desired:
            key = self._device_key(dev)
            if key in existing:
                continue
            try:
                builder.add_dev(dev)
            except (swift_exceptions.RingBuilderError,
                    TypeError, ValueError) as err:
                raise RingBuilderManagerError(
                    'ring %s device %s add failed: %s' %
                    (ring.get('id'), key, err))
            existing[key] = dev
            added.append(self._public_device(dev))
        self.save_builder(builder, builder_path)
        return builder_path, added, self.active_device_count(builder)

    def replace_devices(self, ring, devices):
        return self._locked_builder_mutation(
            ring, lambda: self._replace_devices_locked(ring, devices))

    def _replace_devices_locked(self, ring, devices):
        builder_path, builder = self.load_or_create_builder(ring)
        desired = self.normalize_devices(ring, devices)
        desired_by_key = {}
        for dev in desired:
            key = self._device_key(dev)
            if key in desired_by_key:
                raise RingBuilderManagerError(
                    'ring %s has duplicate device %s:%s/%s' %
                    (ring.get('id'), key[0], key[1], key[2]))
            desired_by_key[key] = dev

        existing_by_key = {}
        for dev in builder.devs:
            if dev is None:
                continue
            existing_by_key[self._device_key(dev)] = dev
        self._check_desired_device_ids(
            ring, desired, builder, existing_by_key=existing_by_key)

        pending = self._pending_remove_ids(builder)
        for key, dev in existing_by_key.items():
            if key not in desired_by_key and dev['id'] not in pending:
                builder.remove_dev(dev['id'])

        for key, dev in desired_by_key.items():
            existing = existing_by_key.get(key)
            if existing is None or existing['id'] in pending:
                try:
                    builder.add_dev(dev)
                except (swift_exceptions.RingBuilderError,
                        TypeError, ValueError) as err:
                    raise RingBuilderManagerError(
                        'ring %s device %s add failed: %s' %
                        (ring.get('id'), key, err))
            else:
                self._sync_existing_dev(builder, existing, dev)

        self.save_builder(builder, builder_path)
        return builder_path, self.list_devices(ring)['devices'], \
            self.active_device_count(builder)

    def remove_devices(self, ring, selectors):
        return self._locked_builder_mutation(
            ring, lambda: self._remove_devices_locked(ring, selectors))

    def _remove_devices_locked(self, ring, selectors):
        try:
            builder_path, builder = self.load_builder(ring)
        except swift_exceptions.FileNotFoundError:
            return self.builder_path(ring), [], 0
        pending = self._pending_remove_ids(builder)
        removed = []
        for dev in list(builder.devs):
            if not dev or dev['id'] in pending:
                continue
            if any(self._selector_matches(dev, selector)
                   for selector in selectors):
                builder.remove_dev(dev['id'])
                pending.add(dev['id'])
                removed.append(self._public_device(
                    dev, pending_removal=True))
        self.save_builder(builder, builder_path)
        return builder_path, removed, self.active_device_count(builder)
