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

import argparse
import errno
import hashlib
import json
import math
import os
import socket
import sys
import urllib.request as urllib_request

from urllib.parse import quote, urlencode, urljoin

from swift.common.ring.utils import parse_add_value, parse_search_value
from swift.common.utils import mkdirs
from swift.ring_manager.common import load_secret, validate_relative_api_url


USER_AGENT = 'swift-ring-manager'


class RingManagerCLIError(Exception):
    pass


class RingManagerClient(object):
    def __init__(self, url, admin_key=None, admin_key_file=None,
                 auth_token=None, read_key=None, read_key_file=None,
                 read_auth_token=None, timeout=30, opener=None):
        if not url:
            raise RingManagerCLIError(
                'Ring manager URL is required. Use --url or set '
                'SWIFT_RING_MANAGER_URL.')
        self.url = url.rstrip('/')
        try:
            self.admin_key = load_secret(
                admin_key, 'admin_key', admin_key_file, 'admin_key_file')
            self.read_key = load_secret(
                read_key, 'read_key', read_key_file, 'read_key_file')
        except ValueError as err:
            raise RingManagerCLIError(str(err))
        self.auth_token = auth_token
        self.read_auth_token = read_auth_token
        self.timeout = timeout
        self.opener = opener or urllib_request.urlopen

    def _url(self, path_or_url):
        return urljoin(self.url + '/', path_or_url)

    def _headers(self, method, headers=None, admin=False, read_only=False):
        request_headers = {'User-Agent': USER_AGENT}
        read_only = (read_only or method in ('GET', 'HEAD')) and not admin
        has_read_credentials = read_only and (
            self.read_key or self.read_auth_token)
        if admin and self.admin_key:
            request_headers['X-Ring-Manager-Admin-Key'] = self.admin_key
        elif read_only and self.read_key:
            request_headers['X-Ring-Manager-Read-Key'] = self.read_key
        elif not has_read_credentials and self.admin_key:
            request_headers['X-Ring-Manager-Admin-Key'] = self.admin_key
        if admin and self.auth_token:
            request_headers['X-Auth-Token'] = self.auth_token
        elif read_only and self.read_auth_token:
            request_headers['X-Auth-Token'] = self.read_auth_token
        elif not has_read_credentials and self.auth_token:
            request_headers['X-Auth-Token'] = self.auth_token
        if headers:
            request_headers.update(headers)
        return request_headers

    def request(self, method, path_or_url, body=None, headers=None,
                parse_json=True, admin=False, read_only=False,
                acceptable_statuses=None):
        acceptable_statuses = set(acceptable_statuses or ())
        data = None
        if body is not None:
            data = json.dumps(body, sort_keys=True).encode('ascii')
            headers = dict(headers or {})
            headers.setdefault('Content-Type', 'application/json')
        url = self._url(path_or_url)
        request_headers = self._headers(
            method, headers, admin=admin, read_only=read_only)
        req = urllib_request.Request(
            url, data=data, headers=request_headers, method=method)
        try:
            resp = self.opener(req, timeout=self.timeout)
        except urllib_request.HTTPError as err:
            err_body = err.read()
            if err.code in acceptable_statuses:
                if not parse_json:
                    return err_body, err.headers
                return self._response_body(
                    method, url, err.code, err_body, parse_json)
            message = err_body.decode('utf-8', 'replace')
            raise RingManagerCLIError(
                '%s %s failed with HTTP %s: %s' % (
                    method, url, err.code, message))
        except (urllib_request.URLError, socket.timeout) as err:
            raise RingManagerCLIError(
                '%s %s failed: %s' % (method, url, err))

        status = getattr(resp, 'status', None)
        if status is None:
            status = getattr(resp, 'code', None)
        if status is None and hasattr(resp, 'getcode'):
            status = resp.getcode()
        if status is None:
            status = 200
        resp_body = resp.read()
        if (status < 200 or status >= 300) and \
                status not in acceptable_statuses:
            raise RingManagerCLIError(
                '%s %s failed with HTTP %s: %s' % (
                    method, url, status,
                    resp_body.decode('utf-8', 'replace')))
        if not parse_json:
            return resp_body, resp.info()
        return self._response_body(
            method, url, status, resp_body, parse_json)

    def _response_body(self, method, url, status, resp_body, parse_json):
        if not resp_body:
            return None
        try:
            return json.loads(resp_body.decode('utf-8'))
        except (TypeError, ValueError, UnicodeDecodeError) as err:
            raise RingManagerCLIError(
                '%s %s returned invalid JSON: %s' % (method, url, err))


def _load_yaml(path):
    try:
        import yaml
    except ImportError:
        raise RingManagerCLIError(
            'PyYAML is required to read %s. Install PyYAML or use JSON.' %
            path)
    try:
        with open(path, 'r') as fp:
            value = yaml.safe_load(fp)
    except IOError as err:
        raise RingManagerCLIError('Unable to read %s: %s' % (path, err))
    except yaml.YAMLError as err:
        raise RingManagerCLIError('Invalid YAML in %s: %s' % (path, err))
    return value


def load_structured_file(path):
    try:
        with open(path, 'r') as fp:
            body = fp.read()
    except IOError as err:
        raise RingManagerCLIError('Unable to read %s: %s' % (path, err))
    try:
        value = json.loads(body)
    except ValueError:
        return _load_yaml(path)
    return value


def _require_object(value, name):
    if not isinstance(value, dict):
        raise RingManagerCLIError('%s must be an object' % name)
    return value


def _require_list(value, name):
    if not isinstance(value, list) or not value:
        raise RingManagerCLIError('%s must be a non-empty list' % name)
    return value


def _json_scalar(value):
    try:
        return json.loads(value)
    except ValueError:
        return value


def _parse_key_values(values):
    parsed = {}
    for item in values or []:
        if '=' not in item:
            raise RingManagerCLIError(
                '--set values must use KEY=VALUE syntax: %s' % item)
        key, value = item.split('=', 1)
        if not key:
            raise RingManagerCLIError('--set values require a key')
        parsed[key] = _json_scalar(value)
    return parsed


def _node_id(node):
    return node.get('id', node.get('node_id'))


def _expand_node_inventory(data):
    if 'devices' in data:
        devices = _require_list(data['devices'], 'devices')
        for device in devices:
            if isinstance(device, dict):
                if not device.get('label'):
                    raise RingManagerCLIError(
                        'device entries require label')
            elif not device:
                raise RingManagerCLIError(
                    'device entries cannot be empty')
        return {'devices': devices}

    nodes = _require_list(data.get('nodes'), 'nodes')
    devices = []
    node_keys = (
        'region', 'zone', 'ip', 'port', 'replication_ip',
        'replication_port')
    for node in nodes:
        _require_object(node, 'node')
        node_id = _node_id(node)
        node_devices = _require_list(node.get('devices'), 'node devices')
        for item in node_devices:
            _require_object(item, 'node device')
            device_name = item.get('device', item.get('name'))
            if not device_name:
                raise RingManagerCLIError(
                    'node device entries require name or device')
            device = {}
            if node_id is not None:
                device['node_id'] = node_id
            for key in node_keys:
                if node.get(key) is not None:
                    device[key] = node[key]
            device.update(dict(
                (key, value) for key, value in item.items()
                if key != 'name'))
            device.setdefault('device', device_name)
            device.setdefault('label', '%s:%s' % (
                node_id, device_name) if node_id is not None else device_name)
            if device.get('weight') is None and node.get('weight') is not None:
                device['weight'] = node['weight']
            devices.append(device)
    return {'devices': devices}


def load_devices_payload(path):
    return _expand_node_inventory(
        _require_object(load_structured_file(path), path))


def parse_device_add_value(value):
    original_value = value
    device_id = None
    if value.startswith('d'):
        index = 1
        while index < len(value) and value[index].isdigit():
            index += 1
        if index == 1:
            raise RingManagerCLIError(
                'Invalid device id in %s' % original_value)
        device_id = int(value[1:index])
        value = value[index:]
    try:
        device = parse_add_value(value)
    except ValueError as err:
        raise RingManagerCLIError(str(err))
    if device.get('region') is None:
        device['region'] = 1
    if device.get('replication_ip') is None:
        device['replication_ip'] = device['ip']
    if device.get('replication_port') is None:
        device['replication_port'] = device['port']
    if device_id is not None:
        device['id'] = device_id
    return device


def parse_device_add_values(values):
    values = list(values or [])
    if not values or len(values) % 2:
        raise RingManagerCLIError(
            'device shorthand requires DEVICE_SPEC WEIGHT pairs')
    devices = []
    for index in range(0, len(values), 2):
        device_spec = values[index]
        weight_value = values[index + 1]
        device = parse_device_add_value(device_spec)
        try:
            weight = float(weight_value)
        except (TypeError, ValueError):
            raise RingManagerCLIError(
                'Invalid weight value for %s: %s' % (
                    device_spec, weight_value))
        if not math.isfinite(weight) or weight < 0:
            raise RingManagerCLIError(
                'Invalid weight value for %s: %s' % (
                    device_spec, weight_value))
        device['weight'] = weight
        devices.append(device)
    return {'devices': devices}


def parse_device_search_values(values):
    values = list(values or [])
    if not values:
        raise RingManagerCLIError('No device selectors specified')
    devices = []
    for value in values:
        try:
            device = parse_search_value(value)
        except ValueError as err:
            raise RingManagerCLIError(str(err))
        if not device:
            raise RingManagerCLIError('Device selector cannot be empty')
        devices.append(device)
    return {'devices': devices}


def load_devices_payload_from_args(args, add_values=False):
    device_values = getattr(args, 'device_values', None) or []
    if args.from_file and device_values:
        raise RingManagerCLIError(
            'Use either --from-file or device shorthand arguments, not both')
    if args.from_file:
        return load_devices_payload(args.from_file)
    if add_values:
        return parse_device_add_values(device_values)
    return parse_device_search_values(device_values)


def _ring_payload_from_args(args):
    payload = {}
    if args.from_file:
        payload.update(_require_object(
            load_structured_file(args.from_file), args.from_file))
    for key in ('id', 'name', 'policy_type'):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    if args.ring_type is not None:
        payload['ring_type'] = args.ring_type
    for key in ('storage_policy_index', 'part_power',
                'num_replicas', 'min_part_hours'):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    if args.builder_file:
        payload['builder_files'] = args.builder_file
    if args.disabled is not None:
        payload['disabled'] = args.disabled
    payload.update(_parse_key_values(args.set_values))
    if not payload:
        raise RingManagerCLIError('No ring fields specified')
    return payload


def _ring_import_defaults(ring_id):
    defaults = {}
    if ring_id == 'account':
        defaults['ring_type'] = 'account'
    elif ring_id == 'container':
        defaults['ring_type'] = 'container'
    elif ring_id in ('object', 'object-0'):
        defaults['ring_type'] = 'object'
        defaults['storage_policy_index'] = 0
        defaults['policy_type'] = 'replication'
    elif ring_id.startswith('object-'):
        try:
            policy_index = int(ring_id.split('-', 1)[1])
        except ValueError:
            return defaults
        defaults['ring_type'] = 'object'
        defaults['storage_policy_index'] = policy_index
        defaults['policy_type'] = 'replication'
    return defaults


def _parse_import_ring(value):
    parts = value.split(':', 2)
    if len(parts) not in (2, 3):
        raise RingManagerCLIError(
            '--ring must use RING_ID:BUILDER_FILE[:RING_FILE]: %s' % value)
    ring_id, builder_file = parts[0], parts[1]
    if not ring_id or not builder_file:
        raise RingManagerCLIError(
            '--ring requires non-empty RING_ID and BUILDER_FILE: %s' % value)
    ring = {'id': ring_id, 'builder_file': builder_file}
    ring.update(_ring_import_defaults(ring_id))
    if len(parts) == 3 and parts[2]:
        ring['ring_file'] = parts[2]
    return ring


def _rings_import_payload(args):
    payload = {}
    if args.from_file:
        payload.update(_require_object(
            load_structured_file(args.from_file), args.from_file))
    if args.ring:
        rings = payload.get('rings') or []
        if not isinstance(rings, list):
            raise RingManagerCLIError('rings in --from-file must be a list')
        rings = list(rings)
        rings.extend(_parse_import_ring(value) for value in args.ring)
        payload['rings'] = rings
    if args.version is not None:
        payload['version'] = args.version
    if args.force:
        payload['force'] = True
    if args.set_latest is not None:
        payload['set_latest'] = args.set_latest
    payload.update(_parse_key_values(args.set_values))
    if not payload.get('rings'):
        raise RingManagerCLIError('No rings specified for import')
    return payload


def _request_or_dry_run(client, args, method, path, body=None,
                        parse_json=True):
    if args.dry_run and method not in ('GET', 'HEAD'):
        return {'method': method, 'path': path, 'body': body}
    return client.request(method, path, body=body, parse_json=parse_json)


def _require_confirm(args, action):
    if not args.confirm:
        raise RingManagerCLIError('%s requires --confirm' % action)


def _table_value(value):
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'yes' if value else 'no'
    if isinstance(value, (list, tuple)):
        return ','.join(_table_value(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, separators=(',', ':'))
    return str(value)


def _table_column_value(row, column):
    _heading, getter = column
    if callable(getter):
        value = getter(row)
    else:
        value = row.get(getter)
    return _table_value(value)


def _format_table(rows, columns):
    rows = rows or []
    table = [[heading for heading, _getter in columns]]
    for row in rows:
        table.append([
            _table_column_value(row, column)
            for column in columns])
    widths = [
        max(len(row[index]) for row in table)
        for index in range(len(columns))]
    lines = [
        '  '.join(
            table[0][index].ljust(widths[index])
            for index in range(len(columns))),
        '  '.join('-' * width for width in widths),
    ]
    lines.extend(
        '  '.join(
            row[index].ljust(widths[index])
            for index in range(len(columns)))
        for row in table[1:])
    return '\n'.join(lines) + '\n'


def _collection_rows(value):
    if isinstance(value, dict):
        return value.get('objects', [])
    return []


def _device_rows(value):
    if isinstance(value, dict):
        return value.get('devices', [])
    return []


def _ring_type(row):
    return row.get('ring_type') or row.get('type') or ''


def _policy(row):
    policy_index = row.get('storage_policy_index')
    policy_type = row.get('policy_type')
    if policy_index is None:
        return policy_type
    if policy_type:
        return '%s/%s' % (policy_index, policy_type)
    return policy_index


def _endpoint(row, ip_key='ip', port_key='port'):
    ip = row.get(ip_key)
    port = row.get(port_key)
    if ip is None:
        return ''
    if port is None:
        return ip
    return '%s:%s' % (ip, port)


def _device_state(row):
    if row.get('pending_removal'):
        return 'removing'
    return 'active'


def _file_count(row):
    files = row.get('files') or []
    return len(files)


def _version_markers(row):
    markers = []
    if row.get('latest'):
        markers.append('latest')
    if row.get('desired'):
        markers.append('desired')
    if not markers:
        return ''
    return '[%s]' % ', '.join(markers)


def _format_rings_list(value):
    return _format_table(_collection_rows(value), (
        ('ID', 'id'),
        ('NAME', 'name'),
        ('TYPE', _ring_type),
        ('POLICY', _policy),
        ('DEVICES', 'device_count'),
    ))


def _format_devices_list(value):
    return _format_table(_device_rows(value), (
        ('ID', 'id'),
        ('REGION', 'region'),
        ('ZONE', 'zone'),
        ('ENDPOINT', _endpoint),
        ('REPLICATION', lambda row: _endpoint(
            row, 'replication_ip', 'replication_port')),
        ('DEVICE', 'device'),
        ('WEIGHT', 'weight'),
        ('STATE', _device_state),
        ('META', 'meta'),
    ))


def _format_versions_list(value):
    return _format_table(_collection_rows(value), (
        ('MARKERS', _version_markers),
        ('VERSION', 'version'),
        ('STATE', 'state'),
        ('CREATED', 'created_at'),
        ('FILES', _file_count),
    ))


def _cleanup_summary_rows(value):
    if not isinstance(value, dict):
        return []
    summary = value.get('summary') or {}
    rows = []
    for name, label in (
            ('manifests', 'manifests'),
            ('ring_artifact_versions', 'ring artifact versions'),
            ('artifact_files', 'artifact files')):
        item = summary.get(name) or {}
        row = {
            'name': label,
            'total': item.get('total', 0),
            'protected': item.get('protected', 0),
            'candidates': item.get('candidates', 0),
        }
        if name == 'artifact_files':
            row['candidate_bytes'] = item.get('candidate_bytes', 0)
        rows.append(row)
    return rows


def _candidate_rows(value, key):
    if not isinstance(value, dict):
        return []
    return [row for row in value.get(key) or [] if row.get('candidate')]


def _format_cleanup_plan(value):
    if not isinstance(value, dict):
        return '%s\n' % value
    lines = [
        'Dry run: %s' % _format_bool(value.get('dry_run')),
        'Delete allowed: %s' % _format_bool(value.get('delete_allowed')),
        'Cleanup safe: %s' % _format_bool(value.get('cleanup_safe')),
        'Cleanup blockers: %s' % len(value.get('cleanup_blockers') or []),
        'Generated: %s' % (value.get('generated_at') or ''),
        'Latest: %s' % (value.get('latest_ring_version') or ''),
        'Retention age: %s' % _format_seconds(value.get('retention_age')),
        'Retain versions: %s' % (
            '' if value.get('retain_versions') is None
            else value.get('retain_versions')),
        'Requires tombstones: %s' % _format_bool(
            value.get('requires_tombstones')),
        '',
        _format_table(_cleanup_summary_rows(value), (
            ('STATE', 'name'),
            ('TOTAL', 'total'),
            ('PROTECTED', 'protected'),
            ('CANDIDATES', 'candidates'),
            ('CANDIDATE_BYTES', 'candidate_bytes'),
        )).rstrip(),
    ]
    unknown = value.get('active_builds_with_unknown_namespaces') or []
    if unknown:
        lines.extend(('', 'Active builds with unknown namespaces: %s' %
                      _format_list(unknown)))
    blockers = value.get('cleanup_blockers') or []
    if blockers:
        lines.append('')
        lines.append('Cleanup blockers:')
        for blocker in blockers:
            lines.append('  %s' % _table_value(blocker))
    warnings = value.get('warnings') or []
    lines.append('')
    lines.append('Warnings: %d' % len(warnings))
    for warning in warnings:
        lines.append('  %s' % _table_value(warning))

    detail_sections = (
        ('Manifest candidates', 'manifests', (
            ('VERSION', 'version'),
            ('CREATED', 'created_at'),
            ('FILES', 'files'),
            ('RINGS', 'rings'),
            ('REASONS', 'reasons'),
        )),
        ('Ring artifact version candidates', 'ring_artifact_versions', (
            ('RING', 'ring_id'),
            ('VERSION', 'version'),
            ('CREATED', 'created_at'),
            ('FILES', 'files'),
            ('REASONS', 'reasons'),
        )),
        ('Artifact file candidates', 'artifact_files', (
            ('PATH', 'path'),
            ('BYTES', 'bytes'),
            ('REASONS', 'reasons'),
        )),
    )
    for title, key, columns in detail_sections:
        rows = _candidate_rows(value, key)
        if rows:
            lines.extend(('', '%s:' % title,
                          _format_table(rows, columns).rstrip()))
    return '\n'.join(lines) + '\n'


def _format_list(value):
    if not value:
        return 'none'
    if isinstance(value, (list, tuple)):
        return ','.join(str(item) for item in value)
    return str(value)


def _partition_group_rows(value):
    if not isinstance(value, dict):
        return []
    rows = []
    for builder in value.get('builder_summaries') or []:
        grouped = builder.get('partitions_by_down_replica_count') or {}
        for count, partitions in grouped.items():
            rows.append({
                'builder': builder.get('builder'),
                'down_replicas': count,
                'partition_count': len(partitions or []),
                'partitions': partitions,
            })
    return sorted(
        rows, key=lambda row: (row.get('builder') or '',
                               -int(row['down_replicas']),
                               row['partition_count']))


def _builder_summary_rows(value):
    if not isinstance(value, dict):
        return []
    rows = []
    for builder in value.get('builder_summaries') or []:
        rows.append({
            'builder': builder.get('builder'),
            'matched_devices': builder.get('matched_devices', 0),
            'affected_partitions': builder.get('affected_partitions', 0),
            'at_risk_partitions': builder.get('at_risk_partitions', 0),
            'max_down_replicas': builder.get('max_down_replicas', 0),
            'placement_available': builder.get('placement_available'),
        })
    return rows


def _format_partitions_at_risk(value):
    if not isinstance(value, dict):
        return '%s\n' % value
    summary = value.get('summary') or {}
    selectors = value.get('selectors') or {}
    lines = [
        'Risk count: %s' % value.get('risk_count', ''),
        'Node IPs: %s' % _format_list(selectors.get('node_ips')),
        'Replication IPs: %s' % _format_list(
            selectors.get('replication_ips')),
        'Device IDs: %s' % _format_list(selectors.get('device_ids')),
        'Matched devices: %s' % summary.get('matched_devices', 0),
        'Affected partitions: %s' % summary.get('affected_partitions', 0),
        'At-risk partitions: %s' % summary.get('at_risk_partitions', 0),
        'Max down replicas: %s' % summary.get('max_down_replicas', 0),
    ]
    builder_rows = _builder_summary_rows(value)
    if builder_rows:
        lines.extend(('', 'Builder summaries:',
                      _format_table(builder_rows, (
                          ('BUILDER', 'builder'),
                          ('MATCHED', 'matched_devices'),
                          ('AFFECTED', 'affected_partitions'),
                          ('AT_RISK', 'at_risk_partitions'),
                          ('MAX_DOWN', 'max_down_replicas'),
                          ('PLACEMENT', 'placement_available'),
                      )).rstrip()))
    matched_devices = value.get('matched_devices') or []
    if matched_devices:
        lines.extend(('', 'Matched devices:',
                      _format_table(matched_devices, (
                          ('BUILDER', 'builder'),
                          ('ID', 'id'),
                          ('REGION', 'region'),
                          ('ZONE', 'zone'),
                          ('ENDPOINT', _endpoint),
                          ('REPLICATION', lambda row: _endpoint(
                              row, 'replication_ip', 'replication_port')),
                          ('DEVICE', 'device'),
                      )).rstrip()))
    rows = _partition_group_rows(value)
    if rows:
        lines.extend(('', 'At-risk partition groups:',
                      _format_table(rows, (
                          ('BUILDER', 'builder'),
                          ('DOWN_REPLICAS', 'down_replicas'),
                          ('COUNT', 'partition_count'),
                          ('PARTITIONS', 'partitions'),
                      )).rstrip()))
    return '\n'.join(lines) + '\n'


def _print(value, stdout, json_output=False, formatter=None):
    if value is None:
        return
    if isinstance(value, bytes):
        stdout.write(value.decode('utf-8', 'replace'))
        if not value.endswith(b'\n'):
            stdout.write('\n')
        return
    if formatter and not json_output:
        stdout.write(formatter(value))
        return
    if json_output or isinstance(value, (dict, list)):
        stdout.write(json.dumps(value, sort_keys=True, indent=2))
        stdout.write('\n')
    else:
        stdout.write('%s\n' % value)


def _format_bool(value):
    if value is None:
        return 'unknown'
    return 'yes' if value else 'no'


def _format_seconds(value):
    if value is None:
        return ''
    try:
        return '%.1f' % float(value)
    except (TypeError, ValueError):
        return str(value)


def _format_promotion_status(value):
    sync = {}
    readiness = {}
    builders = {}
    published = {}
    attention = {}
    if isinstance(value, dict):
        sync = value.get('ring_manager_sync') or {}
        readiness = value.get('promotion_readiness') or {}
        builders = readiness.get('builders') or {}
        published = readiness.get('published_state') or {}
        attention = value.get('operator_attention') or {}

    lines = [
        'Mode: %s' % value.get('mode', '') if isinstance(value, dict)
        else 'Mode:',
        'Writable: %s' % _format_bool(value.get('writable'))
        if isinstance(value, dict) else 'Writable: no',
        'Latest: %s' % (
            value.get('latest_ring_version') or ''
            if isinstance(value, dict) else ''),
        'Sync: fresh=%s synced=%s stale=%s source=%s latest=%s '
        'age=%ss threshold=%ss' % (
            _format_bool(sync.get('fresh')),
            _format_bool(sync.get('synced')),
            _format_bool(sync.get('stale')),
            sync.get('source') or '',
            sync.get('latest_ring_version') or '',
            _format_seconds(sync.get('age_seconds')),
            _format_seconds(sync.get('freshness_threshold'))),
        'Promotion readiness: %s' % (
            'ready' if readiness.get('ready') else 'blocked'),
        'Published state: %s blockers=%s' % (
            'ready' if published.get('ready') else 'blocked',
            _format_list(published.get('blockers'))),
        'Builders: %s required=%s checked=%s skipped_disabled=%s' % (
            'ready' if builders.get('ready') else 'blocked',
            builders.get('required', 0),
            builders.get('checked', 0),
            _format_list(builders.get('skipped_disabled'))),
        'Attention: %s reasons=%s' % (
            'yes' if attention.get('needed') else 'no',
            _format_list(attention.get('reasons'))),
        'Blockers: %s' % _format_list(readiness.get('blockers')),
    ]
    if builders.get('missing'):
        lines.append('Missing builders: %s' % _format_list(
            builders.get('missing')))
    if builders.get('error'):
        lines.append('Builder error: %s' % builders.get('error'))
    invalid = builders.get('invalid') or []
    if invalid:
        lines.append('Invalid builders:')
        for item in invalid:
            if isinstance(item, dict):
                lines.append('  %s reason=%s' % (
                    item.get('ring_id', ''),
                    item.get('reason', '')))
            else:
                lines.append('  %s' % item)
    mismatches = builders.get('version_mismatches') or []
    if mismatches:
        lines.append('Builder version mismatches:')
        for item in mismatches:
            if isinstance(item, dict):
                parts = ['  %s' % item.get('ring_id', '')]
                for key in ('expected', 'minimum', 'actual'):
                    if key in item:
                        parts.append('%s=%s' % (key, item.get(key)))
                lines.append(' '.join(parts))
            else:
                lines.append('  %s' % item)
    unpublished = builders.get('unpublished_builder_changes') or []
    if unpublished:
        lines.append('Unpublished builder changes:')
        for item in unpublished:
            if isinstance(item, dict):
                parts = ['  %s' % item.get('ring_id', '')]
                for key in ('published', 'actual'):
                    if key in item:
                        parts.append('%s=%s' % (key, item.get(key)))
                lines.append(' '.join(parts))
            else:
                lines.append('  %s' % item)
    reasons = sync.get('reasons') or []
    if reasons:
        lines.append('Sync reasons: %s' % _format_list(reasons))
    transaction = sync.get('sync_transaction') or {}
    if transaction.get('pending'):
        lines.append('Sync transaction: pending')
    return '\n'.join(lines) + '\n'


def _rings_list(client, args):
    return client.request('GET', '/api/v1/rings/')


def _rings_show(client, args):
    return client.request(
        'GET', '/api/v1/rings/%s/' % quote(args.ring_id, safe=''))


def _rings_create(client, args):
    return _request_or_dry_run(
        client, args, 'POST', '/api/v1/rings/',
        _ring_payload_from_args(args))


def _rings_update(client, args):
    method = 'PUT' if args.replace else 'PATCH'
    return _request_or_dry_run(
        client, args, method,
        '/api/v1/rings/%s/' % quote(args.ring_id, safe=''),
        _ring_payload_from_args(args))


def _rings_delete(client, args):
    _require_confirm(args, 'rings delete')
    return _request_or_dry_run(
        client, args, 'DELETE',
        '/api/v1/rings/%s/' % quote(args.ring_id, safe=''))


def _rings_import(client, args):
    return _request_or_dry_run(
        client, args, 'POST', '/api/v1/rings/import/',
        _rings_import_payload(args))


def _rings_part_power_action(client, args):
    return _request_or_dry_run(
        client, args, 'POST',
        '/api/v1/rings/%s/partition_power_increase/%s/' % (
            quote(args.ring_id, safe=''), args.part_power_action))


def _rings_build_payload(args):
    payload = {}
    if args.seed is not None:
        payload['seed'] = args.seed
    if args.format_version is not None:
        payload['format_version'] = args.format_version
    payload.update(_parse_key_values(args.set_values))
    return payload


def _rings_build(client, args):
    return _request_or_dry_run(
        client, args, 'POST', '/api/v1/rings/%s/versions/' %
        quote(args.ring_id, safe=''), _rings_build_payload(args))


def _builds_list(client, args):
    params = []
    if args.retry_of == '':
        raise RingManagerCLIError('--retry-of must not be empty')
    if args.retry_root == '':
        raise RingManagerCLIError('--retry-root must not be empty')
    if args.retry_of is not None:
        params.append(('retry_of', args.retry_of))
    if args.retry_root is not None:
        params.append(('retry_root', args.retry_root))
    path = '/api/v1/rings/builds/'
    if params:
        path = '%s?%s' % (path, urlencode(params))
    return client.request('GET', path)


def _builds_show(client, args):
    return client.request(
        'GET', '/api/v1/rings/builds/%s/' %
        quote(args.build_id, safe=''))


def _builds_action_payload(args):
    payload = {}
    if args.reason is not None:
        payload['reason'] = args.reason
    return payload


def _builds_cancel(client, args):
    return _request_or_dry_run(
        client, args, 'POST',
        '/api/v1/rings/builds/%s/cancel/' % quote(args.build_id, safe=''),
        _builds_action_payload(args))


def _builds_retry(client, args):
    return _request_or_dry_run(
        client, args, 'POST',
        '/api/v1/rings/builds/%s/retry/' % quote(args.build_id, safe=''),
        _builds_action_payload(args))


def _versions_list(client, args):
    return client.request('GET', '/api/v1/rings/releases/')


def _versions_publish_payload(args):
    payload = {}
    if args.version is not None:
        payload['version'] = args.version
    if args.ring:
        payload['rings'] = args.ring
    if args.seed is not None:
        payload['seed'] = args.seed
    if args.format_version is not None:
        payload['format_version'] = args.format_version
    if args.no_desired:
        payload['set_desired'] = False
    payload.update(_parse_key_values(args.set_values))
    return payload


def _versions_publish(client, args):
    return _request_or_dry_run(
        client, args, 'POST', '/api/v1/rings/releases/',
        _versions_publish_payload(args))


def _versions_show(client, args):
    version = args.version or 'latest'
    return client.request(
        'GET', '/api/v1/rings/releases/%s/' % quote(version, safe=''))


def _versions_set_desired_payload(args):
    payload = {
        'version': args.version,
        'expected_desired': (
            None if args.expect_no_desired else args.expected_desired),
    }
    if args.reason is not None:
        payload['reason'] = args.reason
    return payload


def _versions_set_desired(client, args):
    return _request_or_dry_run(
        client, args, 'PUT', '/api/v1/rings/releases/desired/',
        _versions_set_desired_payload(args))


def _versions_manifest(client, args):
    version = args.version or 'latest'
    return client.request(
        'GET', '/api/v1/rings/releases/%s/manifest/' %
        quote(version, safe=''))


def _artifact_name(file_info):
    name = file_info.get('name')
    if not name:
        raise RingManagerCLIError('Manifest file entry is missing name')
    return os.path.basename(name)


def _verify_artifact(body, file_info, path):
    expected_bytes = file_info.get('bytes')
    if expected_bytes is not None and len(body) != expected_bytes:
        raise RingManagerCLIError(
            '%s has %d bytes, expected %d' % (
                path, len(body), expected_bytes))
    expected_sha256 = file_info.get('sha256')
    if expected_sha256 is not None:
        actual = hashlib.sha256(body).hexdigest()
        if actual != expected_sha256:
            raise RingManagerCLIError(
                '%s sha256 mismatch: got %s, expected %s' % (
                    path, actual, expected_sha256))


def _write_artifact(output_dir, file_info, body):
    mkdirs(output_dir)
    path = os.path.join(output_dir, _artifact_name(file_info))
    temp_path = '%s.tmp' % path
    with open(temp_path, 'wb') as fp:
        fp.write(body)
    os.rename(temp_path, path)
    return path


def _versions_download(client, args):
    version = args.version or 'latest'
    manifest = client.request(
        'GET', '/api/v1/rings/releases/%s/manifest/' %
        quote(version, safe=''))
    files = _require_list(manifest.get('files'), 'manifest files')
    downloaded = []
    for file_info in files:
        _require_object(file_info, 'manifest file')
        url = file_info.get('url')
        if not url:
            concrete_version = manifest.get('version', version)
            url = '/api/v1/rings/releases/%s/files/%s' % (
                quote(str(concrete_version), safe=''),
                quote(_artifact_name(file_info), safe=''))
        try:
            url = validate_relative_api_url(url, 'manifest artifact URL')
        except ValueError as err:
            raise RingManagerCLIError(str(err))
        body, _headers = client.request('GET', url, parse_json=False)
        path = os.path.join(args.output_dir, _artifact_name(file_info))
        _verify_artifact(body, file_info, path)
        downloaded.append(_write_artifact(args.output_dir, file_info, body))
    return {
        'version': manifest.get('version', version),
        'output_dir': args.output_dir,
        'files': downloaded,
    }


def _devices_list(client, args):
    return client.request(
        'GET', '/api/v1/rings/%s/devices/' % quote(args.ring_id, safe=''))


def _devices_add(client, args):
    return _request_or_dry_run(
        client, args, 'POST',
        '/api/v1/rings/%s/devices/add/' % quote(args.ring_id, safe=''),
        load_devices_payload_from_args(args, add_values=True))


def _devices_remove(client, args):
    return _request_or_dry_run(
        client, args, 'POST',
        '/api/v1/rings/%s/devices/remove/' % quote(args.ring_id, safe=''),
        load_devices_payload_from_args(args))


def _devices_replace(client, args):
    return _request_or_dry_run(
        client, args, 'PUT',
        '/api/v1/rings/%s/devices/' % quote(args.ring_id, safe=''),
        load_devices_payload_from_args(args, add_values=True))


def _append_payload_values(payload, key, values):
    values = [value for value in values or [] if value not in (None, '')]
    if not values:
        return
    existing = payload.get(key)
    if existing in (None, ''):
        payload[key] = values
    elif isinstance(existing, list):
        existing.extend(values)
    else:
        payload[key] = [existing] + values


def _partitions_at_risk_payload_from_args(args):
    payload = {}
    if args.from_file:
        payload = _require_object(
            load_structured_file(args.from_file), args.from_file)
    _append_payload_values(payload, 'node_ips', args.node_ip)
    _append_payload_values(payload, 'node_ips', args.ip)
    _append_payload_values(payload, 'replication_ips', args.replication_ip)
    _append_payload_values(payload, 'device_ids', args.device_id)
    if args.risk_count is not None:
        payload['risk_count'] = args.risk_count
    if args.details is not None:
        payload['details'] = bool(args.details)
    return payload


def _partitions_at_risk_has_selector(payload):
    for key in ('node_ip', 'node_ips', 'ip', 'ips',
                'replication_ip', 'replication_ips',
                'device_id', 'device_ids'):
        value = payload.get(key)
        if value in (None, ''):
            continue
        if isinstance(value, list):
            if any(item not in (None, '') for item in value):
                return True
        else:
            return True
    return False


def _analyze(client, args):
    path = '/api/v1/rings/%s/%s/' % (
        quote(args.ring_id, safe=''), args.analysis)
    params = []
    if args.from_file and args.analysis != 'partitions_at_risk':
        raise RingManagerCLIError(
            '--from-file is only supported for partitions_at_risk')
    if args.analysis == 'dispersion' and args.level:
        params.append(('level', args.level))
    if args.analysis == 'count_parts':
        for ip in args.replication_ip or []:
            params.append(('replication_ip', ip))
        if args.risk_count is not None:
            params.append(('risk_count', args.risk_count))
    if args.analysis == 'partitions_at_risk':
        args.formatter = _format_partitions_at_risk
        payload = _partitions_at_risk_payload_from_args(args)
        if not _partitions_at_risk_has_selector(payload):
            raise RingManagerCLIError(
                'partitions_at_risk requires at least one down selector')
        if args.from_file:
            return client.request('POST', path, body=payload, read_only=True)
        for ip in args.node_ip or []:
            params.append(('node_ip', ip))
        for ip in args.ip or []:
            params.append(('ip', ip))
        for ip in args.replication_ip or []:
            params.append(('replication_ip', ip))
        for device_id in args.device_id or []:
            params.append(('device_id', device_id))
        if args.risk_count is not None:
            params.append(('risk_count', args.risk_count))
        include_details = args.details
        if include_details is None:
            include_details = False
        params.append(('details', 'true' if include_details else 'false'))
    if params:
        path = '%s?%s' % (path, urlencode(params))
    return client.request('GET', path)


def _status(client, args):
    path = '/api/v1/ring_manager/status/'
    if args.promotion:
        path = '%s?%s' % (path, urlencode({'promotion': 'true'}))
        args.formatter = _format_promotion_status
    result = client.request('GET', path)
    if args.promotion:
        readiness = {}
        if isinstance(result, dict):
            readiness = result.get('promotion_readiness') or {}
        if not readiness.get('ready'):
            args.exit_status = 1
    return result


def _cleanup_plan(client, args):
    params = []
    if args.retention_age is not None:
        if (not math.isfinite(args.retention_age) or
                args.retention_age < 0):
            raise RingManagerCLIError(
                '--retention-age must be a finite non-negative number')
        params.append(('retention_age', '%g' % args.retention_age))
    if args.retain_versions is not None:
        if args.retain_versions < 0:
            raise RingManagerCLIError(
                '--retain-versions must be a non-negative integer')
        params.append(('retain_versions', str(args.retain_versions)))
    include_details = args.details
    if include_details is None:
        include_details = args.json
    params.append(('details', 'true' if include_details else 'false'))
    path = '/api/v1/ring_manager/artifact_cleanup/plan/'
    if params:
        path = '%s?%s' % (path, urlencode(params))
    result = client.request('GET', path, admin=True)
    if args.check and (
            not isinstance(result, dict) or
            result.get('cleanup_safe') is not True):
        args.exit_status = 1
    return result


def _add_ring_payload_args(parser):
    parser.add_argument(
        '--from-file',
        help='Read ring fields from a JSON or YAML object file.')
    parser.add_argument(
        '--id',
        help='Explicit ring ID. If omitted, the server derives one.')
    parser.add_argument(
        '--name',
        help='Human-readable ring name.')
    parser.add_argument(
        '--type', dest='ring_type',
        choices=('account', 'container', 'object'),
        help='Swift ring type.')
    parser.add_argument(
        '--policy-index', dest='storage_policy_index', type=int,
        help='Storage policy index for object rings.')
    parser.add_argument(
        '--policy-type',
        help='Storage policy type, such as replication or erasure_coding.')
    disabled_group = parser.add_mutually_exclusive_group()
    disabled_group.add_argument(
        '--disabled', action='store_true', default=None,
        help='Exclude this ring from published cluster manifests.')
    disabled_group.add_argument(
        '--enabled', dest='disabled', action='store_false', default=None,
        help='Include this ring in future published cluster manifests.')
    parser.add_argument(
        '--part-power', type=int,
        help='Ring partition power.')
    parser.add_argument(
        '--replicas', '--num-replicas', dest='num_replicas', type=float,
        help='Ring replica count.')
    parser.add_argument(
        '--min-part-hours', type=int,
        help='Minimum hours before a partition can move again.')
    parser.add_argument(
        '--builder-file', action='append',
        help='Builder file path. May be repeated.')
    parser.add_argument(
        '--set', dest='set_values', action='append', default=[],
        help='Set an arbitrary JSON field as KEY=VALUE.')


def make_parser():
    parser = argparse.ArgumentParser(
        prog='swift-ring-manager',
        description='Manage a Swift ring-manager service over HTTP.')
    parser.add_argument(
        '--url',
        default=os.environ.get('SWIFT_RING_MANAGER_URL') or
        os.environ.get('RING_MANAGER_URL'),
        help='Ring-manager base URL. Default: SWIFT_RING_MANAGER_URL')
    parser.add_argument(
        '--admin-key',
        default=os.environ.get('SWIFT_RING_MANAGER_ADMIN_KEY') or
        os.environ.get('RING_MANAGER_ADMIN_KEY'),
        help='Value for X-Ring-Manager-Admin-Key.')
    parser.add_argument(
        '--admin-key-file',
        default=os.environ.get('SWIFT_RING_MANAGER_ADMIN_KEY_FILE') or
        os.environ.get('RING_MANAGER_ADMIN_KEY_FILE'),
        help='File containing X-Ring-Manager-Admin-Key value.')
    parser.add_argument(
        '--read-key',
        default=os.environ.get('SWIFT_RING_MANAGER_READ_KEY') or
        os.environ.get('RING_MANAGER_READ_KEY'),
        help='Value for X-Ring-Manager-Read-Key.')
    parser.add_argument(
        '--read-key-file',
        default=os.environ.get('SWIFT_RING_MANAGER_READ_KEY_FILE') or
        os.environ.get('RING_MANAGER_READ_KEY_FILE'),
        help='File containing X-Ring-Manager-Read-Key value.')
    parser.add_argument(
        '--auth-token',
        default=os.environ.get('SWIFT_RING_MANAGER_AUTH_TOKEN') or
        os.environ.get('RING_MANAGER_AUTH_TOKEN'),
        help='Value for X-Auth-Token.')
    parser.add_argument(
        '--read-auth-token',
        default=os.environ.get('SWIFT_RING_MANAGER_READ_AUTH_TOKEN') or
        os.environ.get('RING_MANAGER_READ_AUTH_TOKEN'),
        help='Read-only value to send in X-Auth-Token.')
    parser.add_argument(
        '--timeout', type=float, default=30,
        help='HTTP request timeout in seconds. Default: 30')
    parser.add_argument(
        '--json', action='store_true',
        help='Print full command results as JSON. List commands print '
             'tables by default.')
    parser.add_argument(
        '--dry-run', action='store_true',
        help='For mutating commands, print the request without sending it.')

    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    status = subparsers.add_parser(
        'status', help='Show ring-manager service status.')
    status.add_argument(
        '--promotion', action='store_true',
        help='Request promotion readiness checks and exit non-zero unless '
             'the standby is ready to promote.')
    status.set_defaults(func=_status)

    cleanup = subparsers.add_parser(
        'cleanup', help='Plan ring-manager state cleanup.')
    cleanup_sub = cleanup.add_subparsers(dest='cleanup_command')
    cleanup_sub.required = True
    cleanup_plan = cleanup_sub.add_parser(
        'plan', help='Dry-run published-state cleanup and report candidates.')
    cleanup_plan.add_argument(
        '--retention-age', type=float,
        help='Consider unreferenced objects older than SECONDS as cleanup '
             'candidates.')
    cleanup_plan.add_argument(
        '--retain-versions', type=int,
        help='Protect this many newest published cluster manifests in '
             'addition to the latest pointer.')
    cleanup_plan.add_argument(
        '--check', action='store_true',
        help='Exit non-zero when the cleanup plan reports blockers. Normal '
             'cleanup plan output remains exploratory and exits zero on a '
             'successful API call.')
    cleanup_detail_group = cleanup_plan.add_mutually_exclusive_group()
    cleanup_detail_group.add_argument(
        '--details', dest='details', action='store_true', default=None,
        help='Request detailed cleanup graph data and print candidate rows.')
    cleanup_detail_group.add_argument(
        '--summary', dest='details', action='store_false', default=None,
        help='Request only policy, warning, and summary counts. This is the '
             'default unless --json is used.')
    cleanup_plan.set_defaults(
        func=_cleanup_plan, formatter=_format_cleanup_plan)

    rings = subparsers.add_parser(
        'rings', help='Manage logical Swift ring metadata.')
    ring_sub = rings.add_subparsers(dest='rings_command')
    ring_sub.required = True
    rings_list = ring_sub.add_parser('list', help='List logical Swift rings.')
    rings_list.set_defaults(func=_rings_list, formatter=_format_rings_list)
    rings_show = ring_sub.add_parser(
        'show', help='Show one logical Swift ring.')
    rings_show.add_argument(
        'ring_id',
        help='Ring ID, such as account, container, object-0, or object-1.')
    rings_show.set_defaults(func=_rings_show)
    rings_part_power = ring_sub.add_parser(
        'part-power',
        help='Run an object-ring partition power increase lifecycle action.')
    part_power_sub = rings_part_power.add_subparsers(
        dest='part_power_action')
    part_power_sub.required = True
    for command, help_text in (
            ('prepare',
             'Prepare an object ring for a partition power increase.'),
            ('increase',
             'Apply a prepared object-ring partition power increase.'),
            ('cancel',
             'Cancel a prepared object-ring partition power increase.'),
            ('finish',
             'Finish cleanup for a partition power increase or '
             'cancellation.')):
        part_power_cmd = part_power_sub.add_parser(command, help=help_text)
        part_power_cmd.add_argument(
            'ring_id',
            help='Object ring ID, such as object-0 or object-1.')
        part_power_cmd.set_defaults(func=_rings_part_power_action)
    rings_create = ring_sub.add_parser(
        'create', help='Create a logical Swift ring.')
    _add_ring_payload_args(rings_create)
    rings_create.set_defaults(func=_rings_create)
    rings_update = ring_sub.add_parser(
        'update', help='Update a logical Swift ring.')
    rings_update.add_argument('ring_id', help='Ring ID to update.')
    rings_update.add_argument(
        '--replace', action='store_true', help='Use PUT instead of PATCH.')
    _add_ring_payload_args(rings_update)
    rings_update.set_defaults(func=_rings_update)
    rings_delete = ring_sub.add_parser(
        'delete', help='Delete a logical Swift ring metadata record.')
    rings_delete.add_argument('ring_id', help='Ring ID to delete.')
    rings_delete.add_argument(
        '--confirm', action='store_true',
        help='Required acknowledgement that this deletes a logical ring '
             'metadata record.')
    rings_delete.set_defaults(func=_rings_delete)
    rings_import = ring_sub.add_parser(
        'import',
        help='Enroll existing Swift builders and optional ring.gz files.')
    rings_import.add_argument(
        '--from-file',
        help='Read the import request body from a JSON or YAML object file.')
    rings_import.add_argument(
        '--ring', action='append',
        help='Ring to import as RING_ID:BUILDER_FILE[:RING_FILE]. May be '
             'repeated.')
    rings_import.add_argument(
        '--version',
        help='Published baseline cluster ring version ID when ring files are '
             'included. Defaults to a generated import ID.')
    rings_import.add_argument(
        '--force', action='store_true',
        help='Update existing logical ring records when imported metadata '
             'changes, such as device_count after a device add or remove. '
             'This cannot overwrite an existing baseline version ID.')
    latest_group = rings_import.add_mutually_exclusive_group()
    latest_group.add_argument(
        '--set-latest', dest='set_latest', action='store_true',
        default=None,
        help='Set the imported baseline manifest as latest.')
    latest_group.add_argument(
        '--no-latest', dest='set_latest', action='store_false',
        default=None,
        help='Create the imported baseline manifest without updating latest.')
    rings_import.add_argument(
        '--set', dest='set_values', action='append', default=[],
        help='Set an arbitrary top-level JSON field as KEY=VALUE.')
    rings_import.set_defaults(func=_rings_import)
    rings_build = ring_sub.add_parser(
        'build',
        help='Build an artifact for one logical ring without a release.')
    rings_build.add_argument(
        'ring_id',
        help='Ring ID to build without publishing a cluster manifest.')
    rings_build.add_argument(
        '--seed', help='Seed value passed to RingBuilder.rebalance.')
    rings_build.add_argument(
        '--format-version', type=int, choices=(1, 2),
        help='Serialized ring format version. Default: Swift default.')
    rings_build.add_argument(
        '--set', dest='set_values', action='append', default=[],
        help='Set an arbitrary JSON field as KEY=VALUE.')
    rings_build.set_defaults(func=_rings_build)

    devices = subparsers.add_parser(
        'devices', help='Manage ring device membership metadata.')
    dev_sub = devices.add_subparsers(dest='devices_command')
    dev_sub.required = True
    devices_list = dev_sub.add_parser(
        'list', help='List device records for a ring.')
    devices_list.add_argument('ring_id', help='Ring ID whose devices to list.')
    devices_list.set_defaults(
        func=_devices_list, formatter=_format_devices_list)
    for command, func in (('add', _devices_add),
                          ('remove', _devices_remove),
                          ('replace', _devices_replace)):
        dev_cmd = dev_sub.add_parser(
            command,
            help='%s device records for a ring.' % command.title())
        dev_cmd.add_argument(
            'ring_id', help='Ring ID whose devices should be changed.')
        dev_cmd.add_argument(
            '--from-file',
            help='Read devices from a JSON or YAML inventory file.')
        if command == 'remove':
            dev_cmd.add_argument(
                'device_values', nargs='*', metavar='DEVICE_SELECTOR',
                help='Device selector shorthand, such as d0, /sdb, or '
                     'r1z1-127.0.0.1:6200/sdb.')
        else:
            dev_cmd.add_argument(
                'device_values', nargs='*',
                metavar='DEVICE_SPEC_OR_WEIGHT',
                help='Device shorthand as DEVICE_SPEC WEIGHT pairs, such as '
                     'r1z1-127.0.0.1:6200/sdb 100. DEVICE_SPEC may include '
                     'leading d<ID> and R<replication_ip>:'
                     '<replication_port>.')
        dev_cmd.set_defaults(func=func)

    analyze = subparsers.add_parser(
        'analyze', help='Run ring analysis endpoints.')
    analyze.add_argument(
        'analysis',
        choices=('parts', 'rebalance', 'dispersion', 'at_risk',
                 'count_parts', 'partitions_at_risk'),
        help='Analysis endpoint to query.')
    analyze.add_argument('ring_id', help='Ring ID to analyze.')
    analyze.add_argument(
        '--level', choices=('region', 'zone', 'ip', 'device'),
        help='Dispersion aggregation level.')
    analyze.add_argument(
        '--replication-ip', action='append',
        help='Replication IP for count_parts or partitions_at_risk. May be '
             'repeated.')
    analyze.add_argument(
        '--node-ip', action='append',
        help='Node/server IP considered down for partitions_at_risk. May be '
             'repeated.')
    analyze.add_argument('--ip', action='append',
                         help='Short alias for --node-ip.')
    analyze.add_argument(
        '--device-id', action='append', type=int,
        help='Swift ring device ID considered down for partitions_at_risk. '
             'May be repeated.')
    analyze.add_argument(
        '--from-file',
        help='JSON or YAML selector body for partitions_at_risk.')
    analyze.add_argument(
        '--risk-count', type=int,
        help='Minimum down replica count for count_parts or '
             'partitions_at_risk.')
    analyze.add_argument(
        '--details', action='store_true', default=None,
        help='Include partition ID lists in partitions_at_risk output.')
    analyze.set_defaults(func=_analyze)

    builds = subparsers.add_parser(
        'builds', help='Inspect persistent ring build jobs.')
    build_sub = builds.add_subparsers(dest='builds_command')
    build_sub.required = True
    builds_list = build_sub.add_parser('list', help='List ring build jobs.')
    builds_list.add_argument(
        '--retry-of',
        help='List jobs directly retried from the given source build ID.')
    builds_list.add_argument(
        '--retry-root',
        help='List all jobs in the retry chain rooted at the given source '
             'build ID.')
    builds_list.set_defaults(func=_builds_list)
    builds_show = build_sub.add_parser('show', help='Show one ring build job.')
    builds_show.add_argument('build_id', help='Build job ID.')
    builds_show.set_defaults(func=_builds_show)
    builds_cancel = build_sub.add_parser(
        'cancel', help='Cancel a queued or deferred ring build job.')
    builds_cancel.add_argument('build_id', help='Build job ID.')
    builds_cancel.add_argument(
        '--reason', help='Optional operator-visible cancellation reason.')
    builds_cancel.set_defaults(func=_builds_cancel)
    builds_retry = build_sub.add_parser(
        'retry', help='Retry a failed or cancelled ring build job.')
    builds_retry.add_argument(
        'build_id', help='Failed or cancelled build job ID.')
    builds_retry.add_argument(
        '--reason', help='Optional operator-visible retry reason.')
    builds_retry.set_defaults(func=_builds_retry)

    versions = subparsers.add_parser(
        'versions', help='Manage published ring release manifests.')
    ver_sub = versions.add_subparsers(dest='versions_command')
    ver_sub.required = True
    versions_list = ver_sub.add_parser('list', help='List published releases.')
    versions_list.set_defaults(
        func=_versions_list, formatter=_format_versions_list)
    versions_publish = ver_sub.add_parser(
        'publish', help='Build artifacts and publish a complete release.')
    versions_publish.add_argument(
        '--version',
        help='Immutable release version ID. Defaults to generated.')
    versions_publish.add_argument(
        '--ring', action='append',
        help='Ring ID to rebuild. May be repeated. Other enabled rings are '
             'carried forward.')
    versions_publish.add_argument(
        '--seed', help='Seed value passed to RingBuilder.rebalance.')
    versions_publish.add_argument(
        '--format-version', type=int, choices=(1, 2),
        help='Serialized ring format version. Default: Swift default.')
    versions_publish.add_argument(
        '--no-desired', action='store_true',
        help='Publish without selecting the release for agent installation.')
    versions_publish.add_argument(
        '--set', dest='set_values', action='append', default=[],
        help='Set an arbitrary JSON field as KEY=VALUE.')
    versions_publish.set_defaults(func=_versions_publish)
    versions_show = ver_sub.add_parser('show', help='Show one release.')
    versions_show.add_argument(
        'version', nargs='?', help='Release version. Defaults to latest.')
    versions_show.set_defaults(func=_versions_show)
    versions_latest = ver_sub.add_parser(
        'latest', help='Show the latest published release.')
    versions_latest.set_defaults(func=_versions_show, version='latest')
    versions_desired = ver_sub.add_parser(
        'desired',
        help='Show the cluster release selected for installation.')
    versions_desired.set_defaults(func=_versions_show, version='desired')
    versions_set_desired = ver_sub.add_parser(
        'set-desired',
        help='Select a known cluster release for agent installation.')
    versions_set_desired.add_argument(
        'version', help='Known cluster release to select.')
    desired_expectation = \
        versions_set_desired.add_mutually_exclusive_group(required=True)
    desired_expectation.add_argument(
        '--expected-desired',
        help='Require this cluster release to be currently desired.')
    desired_expectation.add_argument(
        '--expect-no-desired', action='store_true',
        help='Require that no cluster release is currently desired.')
    versions_set_desired.add_argument(
        '--reason', help='Optional operator-visible selection reason.')
    versions_set_desired.set_defaults(func=_versions_set_desired)
    versions_manifest = ver_sub.add_parser(
        'manifest', help='Show a release manifest.')
    versions_manifest.add_argument(
        'version', nargs='?', help='Release version. Defaults to latest.')
    versions_manifest.set_defaults(func=_versions_manifest)
    versions_download = ver_sub.add_parser(
        'download', help='Download and verify all release artifacts.')
    versions_download.add_argument(
        'version', nargs='?', help='Release version. Defaults to latest.')
    versions_download.add_argument(
        '--output-dir', required=True,
        help='Directory where ring artifacts are written.')
    versions_download.set_defaults(func=_versions_download)

    return parser


def main(argv=None, opener=None, stdout=None, stderr=None):
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    parser = make_parser()
    args, unknown = parser.parse_known_args(argv)
    if unknown:
        if getattr(args, 'from_file', None) and \
                hasattr(args, 'device_values'):
            args.device_values.extend(unknown)
        else:
            parser.error('unrecognized arguments: %s' % ' '.join(unknown))
    try:
        client = RingManagerClient(
            args.url, admin_key=args.admin_key,
            admin_key_file=args.admin_key_file, auth_token=args.auth_token,
            read_key=args.read_key, read_key_file=args.read_key_file,
            read_auth_token=args.read_auth_token, timeout=args.timeout,
            opener=opener)
        result = args.func(client, args)
        _print(result, stdout, json_output=args.json,
               formatter=getattr(args, 'formatter', None))
        if getattr(args, 'exit_status', 0):
            return args.exit_status
    except RingManagerCLIError as err:
        stderr.write('ERROR: %s\n' % err)
        return 1
    except IOError as err:
        if err.errno == errno.EPIPE:
            return 0
        raise
    return 0


if __name__ == '__main__':
    sys.exit(main())
