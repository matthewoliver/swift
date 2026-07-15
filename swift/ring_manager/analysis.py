#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import os

from array import array
from urllib.parse import parse_qs

from swift.common import exceptions as swift_exceptions
from swift.common.ring.builder import RingBuilder
from swift.ring_manager.common import NormalTimestamp, normal_timestamp_float


DEFAULT_LEVEL = 'zone'
LEVEL_TO_TIER_LENGTH = {
    'raw': 4,
    'device': 4,
    'ip': 3,
    'zone': 2,
    'region': 1,
}


class RingBuilderAnalysisError(Exception):
    pass


def _unique(values):
    seen = set()
    unique_values = []
    for value in values or []:
        if value in (None, ''):
            continue
        if value in seen:
            continue
        seen.add(value)
        unique_values.append(value)
    return unique_values


class RingBuilderAnalyzer(object):
    """
    Read-only analysis helpers for one logical ring/policy.

    A logical ring may have more than one Swift builder, for example the legacy
    SwiftStack-NG account/container policy maps to both ``account.builder`` and
    ``container.builder``.
    """

    def __init__(self, ring, ring_builder_dir=None):
        self.ring = ring
        self.ring_builder_dir = ring_builder_dir

    def _join_builder_dir(self, path):
        if os.path.isabs(path):
            return path
        root = self.ring.get('builder_dir') or self.ring_builder_dir
        if root:
            return os.path.join(root, path)
        return path

    def _default_builder_names(self):
        policy_index = self.ring.get('storage_policy_index')
        if policy_index is None:
            return ['account.builder', 'container.builder']
        try:
            policy_index = int(policy_index)
        except (TypeError, ValueError):
            return []
        if policy_index == 0:
            return ['object.builder']
        return ['object-%d.builder' % policy_index]

    def builder_file_specs(self):
        files = self.ring.get('builder_files')
        if files is None and self.ring.get('builder_path'):
            files = [self.ring.get('builder_path')]
        if files is None:
            files = self._default_builder_names()
        return list(files)

    def builder_files(self):
        return [self._join_builder_dir(path)
                for path in self.builder_file_specs()]

    def _builder_file_entries(self):
        seen = set()
        for index, spec in enumerate(self.builder_file_specs()):
            label = os.path.basename(str(spec).rstrip(os.sep))
            if not label:
                label = 'builder-%d' % index
            if label in seen:
                label = '%s#%d' % (label, index)
            seen.add(label)
            yield label, self._join_builder_dir(spec)

    def builders(self):
        for path in self.builder_files():
            try:
                yield RingBuilder.load(path)
            except swift_exceptions.FileNotFoundError:
                continue

    def _load_builder_files(self):
        builders = []
        missing = []
        for label, path in self._builder_file_entries():
            try:
                builders.append((label, RingBuilder.load(path)))
            except swift_exceptions.FileNotFoundError:
                missing.append(label)
        if missing:
            raise RingBuilderAnalysisError(
                'builder files unavailable: %s' % ', '.join(missing))
        if not builders:
            raise RingBuilderAnalysisError('no builder files available')
        return builders

    def _first_builder_info(self):
        for info in self.builders_dispersion_info():
            if info:
                return info
        return {}

    def _get_balance(self, builder):
        try:
            return builder.get_balance()
        except swift_exceptions.EmptyRingError:
            return 0

    def _ensure_dispersion_graph(self, builder):
        if not builder._replica2part2dev:
            return None
        if not builder._dispersion_graph:
            builder._build_dispersion_graph()
        else:
            for tier in builder._dispersion_graph:
                if len(tier) >= 3 and ':' in tier[2]:
                    builder._build_dispersion_graph()
                    break
        return builder._dispersion_graph

    def builders_dispersion_info(self):
        infos = []
        for builder in self.builders():
            info = {}
            infos.append(info)
            dispersion_graph = self._ensure_dispersion_graph(builder)
            if dispersion_graph is None:
                continue
            info['balance'] = self._get_balance(builder)
            info['dispersion'] = builder.dispersion
            info['max_allowed_replicas'] = \
                builder._build_max_replicas_by_tier()
            info['dispersion_graph'] = dispersion_graph
        return infos

    def parts(self):
        dev2parts = collections.defaultdict(int)
        total_parts = 0
        for builder in self.builders():
            for dev in builder.devs:
                if not dev:
                    continue
                dev2parts[dev['device']] += dev['parts']
                total_parts += dev['parts']
        return {
            'total_parts': total_parts,
            'parts': dict(dev2parts),
        }

    def rebalance_status(self, now=None):
        now = normal_timestamp_float(
            NormalTimestamp.now() if now is None else now)
        info = self._first_builder_info()
        last_rebalance_time = float(self.ring.get('last_rebalance_time') or 0)
        ever_pushed = bool(self.ring.get('ever_pushed'))
        if ever_pushed and last_rebalance_time:
            time_since_last_rebalance = max(0, now - last_rebalance_time)
        else:
            time_since_last_rebalance = 0
        min_part_hours = float(self.ring.get('min_part_hours') or 0)
        minimum_time_until_rebalance = max(
            0, min_part_hours * 3600 - time_since_last_rebalance)
        return {
            'balance': info.get('balance', 0),
            'dispersion': info.get('dispersion', 0),
            'requires_rebalance': not bool(self.ring.get('rebalance_cork')),
            'time_since_last_rebalance': time_since_last_rebalance,
            'minimum_time_until_rebalance': minimum_time_until_rebalance,
        }

    def dispersion(self, level=DEFAULT_LEVEL):
        query_level = level if level in LEVEL_TO_TIER_LENGTH else DEFAULT_LEVEL
        matched_tier = LEVEL_TO_TIER_LENGTH[query_level]
        by_level_dispersion = []
        for info in self.builders_dispersion_info():
            if not info:
                continue
            max_allowed_replicas = info['max_allowed_replicas']
            dispersion_graph = info['dispersion_graph']
            for tier in dispersion_graph:
                if len(tier) != matched_tier:
                    continue
                record = {
                    'region': tier[0],
                    'max_replicas': int(max_allowed_replicas[tier]),
                    'num_of_parts': dispersion_graph[tier],
                }
                if len(tier) >= 2:
                    record['zone'] = tier[1]
                if len(tier) >= 3:
                    record['ip'] = tier[2]
                if len(tier) >= 4:
                    record['device'] = tier[3]
                by_level_dispersion.append(record)
        return {
            'dispersion': by_level_dispersion,
            'level': level,
        }

    def at_risk(self):
        dispersion = dict((key, collections.defaultdict(int))
                          for key in ('total', 'dispersed', 'at_risk'))
        for info in self.builders_dispersion_info():
            if not info:
                continue
            max_allowed_replicas = info['max_allowed_replicas']
            dispersion_graph = info['dispersion_graph']
            for tier, replicas in dispersion_graph.items():
                if len(tier) != 3:
                    continue
                max_replicas = int(max_allowed_replicas[tier])
                cluster_ip = tier[2]
                dispersion['total'][cluster_ip] += sum(replicas[1:])
                dispersion['dispersed'][cluster_ip] += sum(
                    replicas[1:max_replicas + 1])
                dispersion['at_risk'][cluster_ip] += sum(
                    replicas[max_replicas + 1:])
        return dict((key, dict(value)) for key, value in dispersion.items())

    def _default_risk_count(self):
        if self.ring.get('policy_type') == 'erasure_coding':
            parity_fragments = self.ring.get('ec_num_parity_fragments')
            if parity_fragments is not None:
                return max(2, int(parity_fragments) - 1)
        replicas = self.ring.get('num_replicas')
        if replicas is not None:
            return max(2, int(float(replicas)) - 1)
        for builder in self.builders():
            return max(2, int(float(builder.replicas)) - 1)
        return 2

    def _placed_dev(self, builder, dev_id):
        if dev_id == builder.none_dev_id or dev_id >= len(builder.devs):
            return None
        return builder.devs[dev_id]

    def count_parts(self, replication_ips, risk_count=None):
        if risk_count is None:
            risk_count = self._default_risk_count()
        risk_count = max(2, int(risk_count))
        grouped_part_counts = collections.defaultdict(list)
        replication_ips = set(replication_ips)
        if not replication_ips:
            return {
                'risk_count': risk_count,
                'count_parts': {},
            }

        for builder in self.builders():
            if not (any(builder.devs) and builder._replica2part2dev):
                continue
            partition_count = {}
            for replica in builder._replica2part2dev:
                for partition, dev_id in enumerate(replica):
                    dev = self._placed_dev(builder, dev_id)
                    if dev is None:
                        continue
                    if dev['replication_ip'] not in replication_ips:
                        continue
                    partition_count[partition] = \
                        partition_count.get(partition, 0) + 1
            for partition, count in partition_count.items():
                if count >= risk_count:
                    grouped_part_counts[count].append(partition)
        return {
            'risk_count': risk_count,
            'count_parts': dict(grouped_part_counts),
        }

    def _down_device_matcher(self, node_ips=None, replication_ips=None,
                             device_ids=None):
        node_ips = set(node_ips or [])
        replication_ips = set(replication_ips or [])
        device_ids = set(device_ids or [])

        def matches(dev):
            if dev is None:
                return False
            if node_ips and dev.get('ip') in node_ips:
                return True
            if replication_ips and dev.get('replication_ip') in \
                    replication_ips:
                return True
            if device_ids and dev.get('id') in device_ids:
                return True
            return False

        return matches

    def partitions_at_risk(self, node_ips=None, replication_ips=None,
                           device_ids=None, risk_count=None,
                           include_partitions=True):
        if risk_count is None:
            risk_count = self._default_risk_count()
        risk_count = max(2, int(risk_count))
        node_ips = _unique(node_ips)
        replication_ips = _unique(replication_ips)
        device_ids = _unique(device_ids)

        matcher = self._down_device_matcher(
            node_ips=node_ips, replication_ips=replication_ips,
            device_ids=device_ids)
        selectors = {
            'node_ips': node_ips,
            'replication_ips': replication_ips,
            'device_ids': device_ids,
        }
        if not any(selectors.values()):
            result = {
                'risk_count': risk_count,
                'selectors': selectors,
                'summary': {
                    'matched_devices': 0,
                    'affected_partitions': 0,
                    'at_risk_partitions': 0,
                    'max_down_replicas': 0,
                },
                'builder_summaries': [],
                'matched_devices': [],
            }
            return result

        matched_devices_by_key = {}
        builder_summaries = []
        affected_partitions = 0
        at_risk_partitions = 0
        max_down_replicas = 0

        for builder_label, builder in self._load_builder_files():
            builder_matched_devices = 0
            matched_dev_ids = set()
            for dev in builder.devs:
                if matcher(dev):
                    matched_dev_ids.add(dev['id'])
                    builder_matched_devices += 1
                    matched_devices_by_key[(builder_label, dev['id'])] = {
                        'builder': builder_label,
                        'id': dev['id'],
                        'region': dev.get('region'),
                        'zone': dev.get('zone'),
                        'ip': dev.get('ip'),
                        'port': dev.get('port'),
                        'replication_ip': dev.get('replication_ip'),
                        'replication_port': dev.get('replication_port'),
                        'device': dev.get('device'),
                    }
            if not (matched_dev_ids and builder._replica2part2dev):
                builder_summary = {
                    'builder': builder_label,
                    'matched_devices': builder_matched_devices,
                    'affected_partitions': 0,
                    'at_risk_partitions': 0,
                    'max_down_replicas': 0,
                    'placement_available': bool(builder._replica2part2dev),
                }
                if include_partitions:
                    builder_summary['partitions_by_down_replica_count'] = {}
                builder_summaries.append(builder_summary)
                continue

            part_count = max(len(replica) for replica in
                             builder._replica2part2dev)
            partition_count = array('H', [0]) * part_count
            grouped_part_counts = collections.defaultdict(list)
            for replica in builder._replica2part2dev:
                for partition, dev_id in enumerate(replica):
                    if dev_id not in matched_dev_ids:
                        continue
                    partition_count[partition] += 1
            builder_affected_partitions = 0
            builder_at_risk_partitions = 0
            builder_max_down_replicas = 0
            for partition, count in enumerate(partition_count):
                if not count:
                    continue
                affected_partitions += 1
                builder_affected_partitions += 1
                max_down_replicas = max(max_down_replicas, count)
                builder_max_down_replicas = max(
                    builder_max_down_replicas, count)
                if count >= risk_count:
                    at_risk_partitions += 1
                    builder_at_risk_partitions += 1
                    if include_partitions:
                        grouped_part_counts[count].append(partition)
            builder_summary = {
                'builder': builder_label,
                'matched_devices': builder_matched_devices,
                'affected_partitions': builder_affected_partitions,
                'at_risk_partitions': builder_at_risk_partitions,
                'max_down_replicas': builder_max_down_replicas,
                'placement_available': True,
            }
            if include_partitions:
                builder_summary['partitions_by_down_replica_count'] = dict(
                    grouped_part_counts)
            builder_summaries.append(builder_summary)

        matched_devices = sorted(
            matched_devices_by_key.values(),
            key=lambda dev: (dev.get('builder') or '',
                             dev.get('ip') or '',
                             -1 if dev.get('id') is None else dev.get('id')))
        result = {
            'risk_count': risk_count,
            'selectors': selectors,
            'summary': {
                'matched_devices': len(matched_devices),
                'affected_partitions': affected_partitions,
                'at_risk_partitions': at_risk_partitions,
                'max_down_replicas': max_down_replicas,
            },
            'builder_summaries': builder_summaries,
            'matched_devices': matched_devices,
        }
        return result


def get_query_list(req, name):
    values = parse_qs(req.query_string, keep_blank_values=True).get(name, [])
    if len(values) == 1 and ',' in values[0]:
        return [value for value in values[0].split(',') if value]
    return values
