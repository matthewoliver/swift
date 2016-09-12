# Copyright (c) 2015 OpenStack Foundation
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

import errno
import json
import os
import time

from swift import gettext_ as _
from random import random
from hashlib import md5

from eventlet import Timeout

from swift.container.replicator import ContainerReplicator
from swift.container.backend import ContainerBroker, DATADIR, \
    RECORD_TYPE_PIVOT_NODE, RECORD_TYPE_OBJECT, DB_STATE, DB_STATE_NOTFOUND, \
    DB_STATE_UNSHARDED, DB_STATE_SHARDING, DB_STATE_SHARDED
from swift.common import internal_client, db_replicator
from swift.common.bufferedhttp import http_connect
from swift.common.db import DatabaseAlreadyExists
from swift.common.exceptions import DeviceUnavailable, ConnectionTimeout
from swift.common.http import is_success
from swift.common.constraints import CONTAINER_LISTING_LIMIT, \
    SHARD_CONTAINER_SIZE
from swift.common.ring.utils import is_local_device
from swift.common.utils import get_logger, config_true_value, \
    dump_recon_cache, whataremyips, hash_path, \
    storage_directory, Timestamp, PivotRange, pivot_to_pivot_container, \
    find_pivot_range, ismount, majority_size, GreenAsyncPile, \
    account_to_pivot_account
from swift.common.wsgi import ConfigString
from swift.common.storage_policy import POLICIES

# The default internal client config body is to support upgrades without
# requiring deployment of the new /etc/swift/internal-client.conf
ic_conf_body = """
[DEFAULT]
# swift_dir = /etc/swift
# user = swift
# You can specify default log routing here if you want:
# log_name = swift
# log_facility = LOG_LOCAL0
# log_level = INFO
# log_address = /dev/log
#
# comma separated list of functions to call to setup custom log handlers.
# functions get passed: conf, name, log_to_console, log_route, fmt, logger,
# adapted_logger
# log_custom_handlers =
#
# If set, log_udp_host will override log_address
# log_udp_host =
# log_udp_port = 514
#
# You can enable StatsD logging here:
# log_statsd_host = localhost
# log_statsd_port = 8125
# log_statsd_default_sample_rate = 1.0
# log_statsd_sample_rate_factor = 1.0
# log_statsd_metric_prefix =

[pipeline:main]
pipeline = catch_errors proxy-logging cache proxy-server

[app:proxy-server]
use = egg:swift#proxy
account_autocreate = true
# See proxy-server.conf-sample for options

[filter:cache]
use = egg:swift#memcache
# See proxy-server.conf-sample for options

[filter:proxy-logging]
use = egg:swift#proxy_logging

[filter:catch_errors]
use = egg:swift#catch_errors
# See proxy-server.conf-sample for options
""".lstrip()

PR_NAME = 0
PR_CREATED_AT = 1
PR_LOWER = 2
PR_UPPER = 3
PR_OBJECT_COUNT = 4
PR_BYTES_USED = 5

class ContainerSharder(ContainerReplicator):
    """Shards containers."""

    def __init__(self, conf, logger=None, **kargs):
        super(ContainerReplicator, self).__init__(conf, logger=logger)
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='container-sharder')
        self.rcache = os.path.join(self.recon_cache_path,
                                   "container-sharder.recon")
        self.vm_test_mode = config_true_value(conf.get('vm_test_mode', 'no'))

        self.shard_shrink_point = \
            float(conf.get('shard_shrink_point', 50) / 100.0)
        self.shrink_merge_point = \
            float(conf.get('shard_shrink_merge_point', 75) / 100.0)
        self.split_size = SHARD_CONTAINER_SIZE // 2
        self.cpool = GreenAsyncPile(self.cpool)
        self.scanner_batch_size = int(conf.get('shard_scanner_batch_size', 10))
        self.shard_batch_size = int(conf.get('shard_batch_size', 2))

        # internal client
        self.conn_timeout = float(conf.get('conn_timeout', 5))
        request_tries = int(conf.get('request_tries') or 3)
        internal_client_conf_path = conf.get('internal_client_conf_path')
        if not internal_client_conf_path:
            self.logger.warning(
                _('Configuration option internal_client_conf_path not '
                  'defined. Using default configuration, See '
                  'internal-client.conf-sample for options. NOTE: '
                  '"account_autocreate = true" is required.'))
            internal_client_conf = ConfigString(ic_conf_body)
        else:
            internal_client_conf = internal_client_conf_path
        try:
            self.swift = internal_client.InternalClient(
                internal_client_conf, 'Swift Container Sharder', request_tries)
        except IOError as err:
            if err.errno != errno.ENOENT:
                raise
            raise SystemExit(
                _('Unable to load internal client from config: %r (%s)') %
                (internal_client_conf_path, err))

    def _zero_stats(self):
        """Zero out the stats."""
        super(ContainerSharder, self)._zero_stats()
        # TODO add actual sharding stats to track, and zero them out here.

    def _get_local_devices(self):
        self._local_device_ids = set()
        results = set()
        self.ips = whataremyips()
        if not self.ips:
            self.logger.error(_('ERROR Failed to get my own IPs?'))
            return
        for node in self.ring.devs:
            if node and is_local_device(self.ips, self.port,
                                        node['replication_ip'],
                                        node['replication_port']):
                results.add(node['device'])
                self._local_device_ids.add(node['id'])
        return results

    def _get_pivot_ranges(self, account, container, newest=False):
        path = self.swift.make_path(account, container) + \
            '?items=pivot&format=json'
        headers = dict()
        if newest:
            headers['X-Newest'] = 'true'
        try:
            resp = self.swift.make_request('GET', path, headers,
                                           acceptable_statuses=(2,))
        except internal_client.UnexpectedResponse:
            self.logger.error(_("Failed to get pivot points from %s/%s"),
                              account, container)
            return None

        ranges = list()
        try:
            for pivot in json.loads(resp.body):
                lower = pivot.get('lower') or None
                upper = pivot.get('upper') or None
                created_at = pivot.get('created_at') or None
                ranges.append(PivotRange(pivot['name'], lower, upper,
                                         created_at))
        except ValueError:
            # Failed to decode the json response
            return None
        return ranges

    def _get_shard_broker(self, account, container, policy_index):
        """
        Get a local instance of the shard container broker that will be
        pushed out.

        :param account: the account
        :param container: the container
        :returns: a local shard container broker
        """
        part = self.ring.get_part(account, container)
        node = self.find_local_handoff_for_part(part)
        if not node:
            raise DeviceUnavailable(
                'No mounted devices found suitable to Handoff sharded '
                'container %s in partition %s' % (container, part))
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, node['device'], db_dir, hsh + '.db')
        broker = ContainerBroker(db_path, account=account, container=container)
        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(storage_policy_index=policy_index)
            except DatabaseAlreadyExists:
                pass

        # Get the valid info into the broker.container, etc
        broker.get_info()
        return part, broker, node['id']

    def _generate_object_list(self, items, policy_index, delete=False):
        """
        Create a list of dictionary items ready to be consumed by
        Broker.merge_items()

        :param items: list of objects
        :param policy_index: the Policy index of the container
        :param delete: mark the objects as deleted; default False

        :return: A list of item dictionaries ready to be consumed by
                 merge_items.
        """
        objs = list()
        timestamp = None
        for item in items:
            try:
                if delete:
                    # Generate a new delete timestamp based off the existing
                    # created_at, this way we don't clobber other objects that
                    # may exist out there. The idea is, newer object out there
                    # will replace the deleted record, which will be picked up
                    # as a misplaced object and then be pushed and compared to
                    # where it needs to be.
                    ts = Timestamp(item[1])
                    ts.offset += 1
                    timestamp = ts.internal
                obj = {
                    'created_at': timestamp or item[1]}
                if len(item) > 5:
                    # pivot node
                    obj.update({
                        'name': item[0],
                        'lower': item[2],
                        'upper': item[3],
                        'object_count': item[4],
                        'bytes_used': item[5],
                        'deleted': 1 if delete else 0,
                        'storage_policy_index': 0,
                        'record_type': RECORD_TYPE_PIVOT_NODE})
                else:
                    # object item
                    obj.update({
                        'name': item[0],
                        'size': item[2],
                        'content_type': item[3],
                        'etag': item[4],
                        'deleted': 1 if delete else 0,
                        'storage_policy_index': policy_index,
                        'record_type': RECORD_TYPE_OBJECT})
            except Exception:
                self.logger.warning(_("Failed to add object %s, not in the"
                                      'right format'),
                                    item[0] if item[0] else str(item))
            else:
                objs.append(obj)
        return objs

    def _get_node_index(self):
        nodes = self.ring.get_part_nodes(self.part)
        indices = [node['index'] for node in nodes
                   if node['id'] == self.node_id]
        return indices[0] if indices else None

    def _add_shard_metadata(self, broker, root_account, root_container,
                            pivot):
        if not broker.metadata.get('X-Container-Sysmeta-Shard-Account') \
                and pivot:
            timestamp = Timestamp(time.time()).internal
            broker.update_metadata({
                'X-Container-Sysmeta-Shard-Account': (root_account, timestamp),
                'X-Container-Sysmeta-Shard-Container':
                    (root_container, timestamp),
                'X-Container-Sysmeta-Shard-Lower': (pivot.lower, timestamp),
                'X-Container-Sysmeta-Shard-Upper': (pivot.upper, timestamp)})

    def _misplaced_objects(self, broker, root_account, root_container, pivot):
        """
        Search for objects in the current broker that don't belong, and move
        to the container shards they do.

        :param broker: The parent broker to update once misplaced objects have
                       been moved.
        :param root_account: The root account
        :param root_container: The root container
        :param pivot: The pivot point of the container
        """

        self.logger.info(_('Scanning %s/%s for misplaced objects'),
                         broker.account, broker.container)
        queries = []
        policy_index = broker.storage_policy_index
        query = dict(marker='', end_marker='', prefix='', delimiter='',
                     storage_policy_index=policy_index)

        ranges = broker.get_pivot_ranges()
        # only the root container has a bunch of ranges. If root container also
        # happens to be in the list of ranges, it means we are still sharding
        # up the root container, so we need to treat it like any other shard in
        # regards to misplaced object checks.
        in_ranges = any([p[0] == broker.container for p in ranges])
        if (len(ranges) > 0 and not in_ranges) or broker.is_deleted():
            # It's a sharded node or deleted, so anything in the object table
            # is misplaced.
            if broker.get_info()['object_count'] > 0:
                queries.append(query.copy())
            else:
                return
        elif pivot is None:
            # This is an unsharded root container, so we don't need to
            # query anything.
            return
        else:
            # it hasn't been sharded and isn't the root container, so we need
            # to look for objects that shouldn't be in the object table.
            if pivot.upper:
                tmp_q = query.copy()
                tmp_q['marker'] = pivot.upper
                queries.append(tmp_q)
            if pivot.lower:
                tmp_q = query.copy()
                tmp_q['end_marker'] = pivot.lower
                queries.append(tmp_q)

        def run_query(qry):
            objs = broker.list_objects_iter(CONTAINER_LISTING_LIMIT, **qry)
            if not objs:
                return

            # We have a list of misplaced objects, so we better find a home
            # for them
            if not self.ranges:
                self.ranges = self._get_pivot_ranges(
                    root_account, root_container, newest=True)

            pivot_to_obj = {}
            for obj in objs:
                p = find_pivot_range(obj, ranges)

                if p in pivot_to_obj:
                    pivot_to_obj[p].append(obj)
                else:
                    pivot_to_obj[p] = [obj]
                qry['marker'] = obj[0]

            self.logger.info(_('preparing to move misplaced objects found '
                               'in %s/%s'), broker.account, broker.container)
            for piv, obj_list in pivot_to_obj.items():
                acct, cont = pivot_to_pivot_container(root_account,
                                                      root_container,
                                                      pivot_range=piv)

                part, new_broker, node_id = \
                    self._get_shard_broker(acct, cont, policy_index)

                self._add_shard_metadata(new_broker, root_account,
                                         root_container, piv)

                objects = self._generate_object_list(obj_list, policy_index)
                new_broker.merge_items(objects)

                self.cpool.spawn(
                    self._replicate_object, part, new_broker.db_file, node_id)

                # Remove the now relocated misplaced items.
                items = self._generate_object_list(obj_list, policy_index,
                                                   delete=True)
                broker.merge_items(items)
            any(self.cpool)

            if len(objs) == CONTAINER_LISTING_LIMIT:
                # There could be more, so recurse my pretty
                run_query(qry)

        for query in queries:
            run_query(query)

        # wipe out the cache do disable bypass in delete_db
        cleanups = self.shard_cleanups or {}
        self.shard_cleanups = self.shard_brokers = None
        self.logger.info('Cleaning up %d replicated shard containers',
                         len(cleanups) if cleanups else 0)
        for container in cleanups.values():
            self.cpool.spawn(self.delete_db, container)
        any(self.cpool)
        self.logger.info('Finished misplaced shard replication')

    @staticmethod
    def get_pivot_range(broker, timestamp=None):
        _timestamp = None
        lower = broker.metadata.get('X-Container-Sysmeta-Shard-Lower')
        if lower:
            lower, _timestamp = lower
        upper = broker.metadata.get('X-Container-Sysmeta-Shard-Upper')
        if upper:
            upper, _timestamp = upper

        if not lower and not upper:
            return None

        if not lower:
            lower = None
        if not upper:
            upper = None

        if not timestamp:
            timestamp = _timestamp
        return PivotRange(broker.container, lower, upper, timestamp)

    @staticmethod
    def get_shard_root_path(broker):
        """
        Attempt to get the root shard container name and account for the
        container represented by this broker.

        A container shard has 'X-Container-Sysmeta-Shard-{Account,Container}
        set, which will contain the relevant values for the root shard
        container. If they don't exist, then it returns the account and
        container associated directly with the broker.

        :param broker:
        :return: account, container of the root shard container or the brokers
                 if it can't be determined.
        """
        broker.get_info()
        account = broker.metadata.get('X-Container-Sysmeta-Shard-Account')
        if account:
            account = account[0]
        else:
            account = broker.account

        container = broker.metadata.get('X-Container-Sysmeta-Shard-Container')
        if container:
            container = container[0]
        else:
            container = broker.container

        return account, container

    def _post_replicate_hook(self, broker, info, responses):
        return

    def delete_db(self, broker):
        """
        Ensure that replicated sharded databases are only cleaned up at the end
        of the replication run.
        """
        if self.shard_cleanups is not None:
            # this container shouldn't be here, make sure it's cleaned up
            self.shard_cleanups[broker.container] = broker
            return
        return super(ContainerReplicator, self).delete_db(broker)

    def _audit_shard_container(self, broker, pivot, root_account=None,
                               root_container=None):
        # TODO We will need to audit the root (make sure there are no missing
        #      gaps in the ranges.
        # TODO Search for overlaps if you find some, keep the newest and
        #      attempt to correct (remove the older one).
        # TODO If the shard container has a sharding lock then see if it's
        #       needed. Maybe something as simple as sharding lock older then
        #       reclaim age.

        self.logger.info(_('Auditing %s/%s'), broker.account, broker.container)
        continue_with_container = True

        # if the container has been marked as deleted, all metadata will
        # have been erased so no point auditing. But we want it to pass, in
        # case any objects exist inside it.
        if broker.is_deleted():
            return continue_with_container

        if not root_account or not root_container:
            root_account, root_container = \
                ContainerSharder.get_shard_root_path(broker)

        if root_container == broker.container:
            # This is the root container, and therefore the tome of knowledge,
            # all we can do is check there is nothing screwy with the range
            ranges = broker.get_pivot_ranges()
            ranges = [PivotRange(r[0], r[2], r[3], r[1]) for r in ranges]
            overlaps = ContainerSharder.find_overlapping_ranges(ranges)
            for overlap in overlaps:
                self.logger.error(_('Range overlaps found, attempting to '
                                    'correct'))
                newest = max(overlap, key=lambda x: x.timestamp)
                older = set(overlap).difference(set([newest]))

                # now delete the older overlaps, keeping only the newest
                timestamp = Timestamp(newest.timestamp)
                timestamp.offset += 1
                pivots = [(r.name, timestamp.internal, r.lower, r.upper, 0, 0)
                          for r in older]
                self._update_pivot_ranges(root_account, root_container,
                                          'DELETE', pivots)
                continue_with_container = False
            missing_ranges = ContainerSharder.check_complete_ranges(ranges)
            if missing_ranges:
                self.logger.error(_('Missing range(s) dectected: %s'),
                                  '-'.join(missing_ranges))
                continue_with_container = False

            return continue_with_container

        # Get the root view of the world.
        self.ranges = self._get_pivot_ranges(root_account, root_container,
                                             newest=True)
        if self.ranges is None:
            # failed to get the root tree. Error out for now.. we may need to
            # quarantine the container.
            self.logger.warning(_("Failed to get a pivot tree from root "
                                  "container %s/%s, it may not exist."),
                                root_account, root_container)
            return False
        if pivot in self.ranges:
            return continue_with_container

        # pivot isn't in ranges, if it overlaps with an item, were in trouble.
        # If there is overlap lets see if it's newer then this containers,
        # if so, it's safe to delete (quarantine this container).
        # if it's newer, then it might not be updated yet, so just let it
        # continue (or maybe we shouldn't?).
        overlaps = [r for r in self.ranges if r.overlaps(pivot)]
        if overlaps:
            if max(overlaps + [pivot], key=lambda x: x.timestamp) == pivot:
                # pivot is newest so leave it alone for now  as the root might
                # not be updated  yet.
                return False
            else:
                # There is a newer range that overlaps/covers this range.
                # so we are safe to quarantine it.
                # TODO Quratnine
                self.logger.error(_('The range of objects stored in this '
                                    'container (%s/%s) overlaps with another '
                                    'newer pivot'),
                                  broker.account, broker.container)
                return False
        # pivot doesn't exist in the root containers ranges, but doesn't
        # overlap with anything
        return continue_with_container

    def _update_pivot_counts(self, root_account, root_container, broker):
        if broker.container == root_container:
            return
        timestamp = Timestamp(time.time())
        pivot = self.get_pivot_range(broker, timestamp)
        if not pivot:
            return
        tmp_info = broker.get_info()
        pivot = (pivot.name, pivot.timestamp, pivot.lower, pivot.upper,
                 tmp_info['object_count'], tmp_info['bytes_used'])
        self._update_pivot_ranges(root_account, root_container, 'PUT', [pivot])

    def get_metadata_item(self, broker, header):
        item = broker.metadata.get(header)
        return None if item is None else item[0]

    @staticmethod
    def roundrobin_datadirs(datadirs):
        data = []
        idx = {}
        for datadir, node_idx in datadirs:
            data.append(datadir)
            idx[datadir[-1]] = node_idx

        for cont in db_replicator.roundrobin_datadirs(data):
            if cont:
                yield list(cont) + [idx[cont[-1]]]
            else:
                raise StopIteration()

    def _one_shard_pass(self, reported):
        """
        The main function, everything the sharder does forks from this method.

        The sharder loops through each sharded container on server, on each
        container it:
            - audits the container
            - checks and deals with misplaced items
            - 2 phase sharding (if no. objects > shard_container_size)
                - Phase 1, if there is no pivot defined, find it, then move
                  to next container.
                - Phase 2, if there is a pivot defined, shard it.
            - Shrinking (check to see if we need to shrink this container).
        :param reported:
        """
        self._zero_stats()
        self.logger.info(_('Starting container sharding pass'))
        dirs = []
        self.shard_brokers = dict()
        self.shard_cleanups = dict()
        self._local_device_ids = set()
        self.ips = whataremyips()
        for node in self.ring.devs:
            if node and is_local_device(self.ips, self.port,
                                        node['replication_ip'],
                                        node['replication_port']):
                if self.mount_check and not ismount(
                        os.path.join(self.root, node['device'])):
                    self.logger.warn(
                        _('Skipping %(device)s as it is not mounted') % node)
                    continue
                datadir = os.path.join(self.root, node['device'], self.datadir)
                if os.path.isdir(datadir):
                    self._local_device_ids.add(node['id'])
                    dirs.append(((datadir, node['id']), node['index']))
        for part, path, node_id, node_idx in \
                ContainerSharder.roundrobin_datadirs(dirs):
            broker = ContainerBroker(path)
            sharded = broker.metadata.get('X-Container-Sysmeta-Sharding') or \
                broker.metadata.get('X-Container-Sysmeta-Shard-Account')
            if not sharded:
                # Not a shard container
                continue
            self.ranges = []
            self.node_idx = node_idx
            self.node_id = node_id
            root_account, root_container = \
                ContainerSharder.get_shard_root_path(broker)
            pivot = ContainerSharder.get_pivot_range(broker)

            # Before we do any heavy lifting, lets do an audit on the shard
            # container. We grab the root's view of the pivot_points and make
            # sure this container exists in it and in what should be it's
            # parent. If its in both great, If it exists in either but not the
            # other, then this needs to be fixed. If, however, it doesn't
            # exist in either then this container may not exist anymore so
            # quarantine it.
         #   if not self._audit_shard_container(broker, pivot, root_account,
          #                                     root_container):
           #     continue

            # now look and deal with misplaced objects.
            self._misplaced_objects(broker, root_account, root_container,
                                    pivot)

            if broker.is_deleted():
                # This container is deleted so we can skip it. We still want
                # deleted containers to go via misplaced items, cause they may
                # have new objects in sitting in them that may need to move.
                continue

            self.state = broker.get_db_state()
            if self.state in (DB_STATE_SHARDED, DB_STATE_NOTFOUND):
                continue

            self.shard_brokers = dict()
            self.shard_cleanups = dict()

            try:
                if broker.is_shrinking():
                    # No matter what, we want to finish this shrink stage
                    # before anything else.
                    self._shrink(broker, root_account, root_container)
                    continue

                # The sharding mechanism is rather interesting. If the container
                # requires sharding.. that is big enough and sharding hasn't
                # started yet. Then we need to identify a primary node to scan
                # for shards. The others will wait for shards to appear in there
                # pivot_ranges table (via replication). They will start the
                # sharding process

                if self.state == DB_STATE_UNSHARDED:
                    obj_count = broker.get_info()['object_count']
                    if obj_count <= (SHARD_CONTAINER_SIZE *
                                     self.shard_shrink_point):
                        # Shrink
                        self._shrink(broker, root_account, root_container)
                    elif obj_count <= SHARD_CONTAINER_SIZE:
                        continue

                # We are either in the sharding state or we need to start
                # sharding. So find out if this is suppose to be the scanning
                # node, if defined
                scan_idx = self.get_metadata_item(
                    broker, 'X-Container-Sysmeta-Shard-Scanner')
                scan_complete = config_true_value(self.get_metadata_item(
                    broker, 'X-Container-Sysmeta-Sharding-Scan-Done'))

                if not scan_idx and not scan_complete:
                    try:
                        scan_idx = self._find_scanner_node(broker)
                    except Exception:
                        # todo log and continue
                        continue

                if scan_idx and scan_idx == self.node_id and not scan_complete:
                    self._find_pivot_points(broker)
                else:
                    self._shard_on_pivot(broker, root_account,
                                         root_container)
            finally:
                self._update_pivot_counts(root_account, root_container,
                                          broker)

                # wipe out the cache do disable bypass in delete_db
                cleanups = self.shard_cleanups
                self.shard_cleanups = None
                self.logger.info('Cleaning up %d replicated shard containers',
                                 len(cleanups))

                for container in cleanups.values():
                    self.cpool.spawn(self.delete_db, container)

                # Now we wait for all threads to finish.
                all(self.cpool)

                self.logger.info(_('Finished container sharding pass'))

        # all_locs = audit_location_generator(self.devices, DATADIR, '.db',
        #                                    mount_check=self.mount_check,
        #                                    logger=self.logger)
        # for path, device, partition in all_locs:
        #    self.container_audit(path)
        #    if time.time() - reported >= 3600:  # once an hour
        #        self.logger.info(
        #            _('Since %(time)s: Container audits: %(pass)s passed '
        #              'audit, %(fail)s failed audit'),
        #            {'time': time.ctime(reported),
        #             'pass': self.container_passes,
        #             'fail': self.container_failures})
        #        dump_recon_cache(
        #            {'container_audits_since': reported,
        #             'container_audits_passed': self.container_passes,
        #             'container_audits_failed': self.container_failures},
        #            self.rcache, self.logger)
        #        reported = time.time()
        #        self.container_passes = 0
        #        self.container_failures = 0
        #    self.containers_running_time = ratelimit_sleep(
        #        self.containers_running_time, self.max_containers_per_second)
        # return reported

    def _send_request(self, ip, port, contdevice, partition, op, path,
                      headers_out={}, node_id=None):
        if 'user-agent' not in headers_out:
            headers_out['user-agent'] = 'container-sharder %s' % \
                                        os.getpid()
        if 'X-Timestamp' not in headers_out:
            headers_out['X-Timestamp'] = Timestamp(time.time()).normal
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(ip, port, contdevice, partition,
                                    op, path, headers_out)
            with Timeout(self.node_timeout):
                response = conn.getresponse()
                return response, node_id
        except (Exception, Timeout) as x:
            self.logger.info(str(x))
            # Need to do something here.
            return None, node_id

    def _update_pivot_ranges(self, account, container, op, pivots):
        path = "/%s/%s" % (account, container)
        part, nodes = self.ring.get_nodes(account, container)

        for pivot in pivots:
            # (piv.name, timestamp, piv.lower, piv.upper, object_count,
            #  bytes_used)
            obj = pivot[0]
            obj_path = '%s/%s' % (path, obj)
            headers = {
                'x-backend-record-type': RECORD_TYPE_PIVOT_NODE,
                'x-backend-pivot-objects': pivot[4],
                'x-backend-pivot-bytes': pivot[5],
                'x-backend-pivot-lower': pivot[2],
                'x-backend-pivot-upper': pivot[3],
                'x-backend-timestamp': pivot[1],
                'x-size': 0}

            for node in nodes:
                self.cpool.spawn(
                    self._send_request, node['ip'], node['port'],
                    node['device'], part, op, obj_path, headers)
            all(self.cpool)

    @staticmethod
    def check_complete_ranges(ranges):
        lower = set()
        upper = set()
        for r in ranges:
            lower.add(r.lower)
            upper.add(r.upper)
        l = lower.copy()
        lower.difference_update(upper)
        upper.difference_update(l)

        return zip(upper, lower)

    @staticmethod
    def find_overlapping_ranges(ranges):
        result = set()
        for range in ranges:
            res = [r for r in ranges if range != r and range.overlaps(r)]
            if res:
                res.append(range)
                res.sort()
                result.add(tuple(res))

        return result

    def _find_scanner_node(self, broker):
        self.logger.info(_('Started searching for best node to be pivot point '
                           'scanner for %s/%s'),
                         broker.account, broker.container)
        obj_count = [broker.get_info()['object_count']]
        scanner_id = [self._get_node_index()]
        scanner_ids = {}

        def on_success(resp, node_id):
            if not resp or not is_success(resp.status):
                return False

            found_scan_id = resp.getheader('X-Container-Sysmeta-Shard-Scanner')
            found_obj_count = int(resp.getheader('X-Container-Object-Count'))

            if found_scan_id:
                if scanner_ids.get(found_scan_id):
                    scanner_ids[found_scan_id] += 1
                else:
                    scanner_ids[found_scan_id] = 1

            if found_obj_count > obj_count[0]:
                obj_count[0] = found_obj_count
                scanner_id[0] = node_id

            return True

        if not self._get_quorum(broker, success=on_success):
            self.logger.info(_('Failed to reach quorum on a pivot scanner for '
                               '%s/%s'), broker.account, broker.container)
            return

        # We have a quorum of responses but if there is already a node out
        # there with a scan_id set, then this means that there was a quorum
        # previously, and most probably this node failed the POSTing of the
        # metadata OR the POSTing of the metadata failed to get quorum.
        # In either case, we'll just use it, as replication should have/will
        # move the scan_id around anyway. And a scanner will always ask for
        # quorum before adding an item to pivot_ranges, so all should be good.
        if scanner_ids:
            if len(scanner_ids) > 1:
                # find the most occurring or if a tie wait for replication to
                # choose the best
                max_id = max(scanner_ids.items(), key=lambda x: x[1])

                # We need to make sure there isn't a tie, if there is we want
                # to bail and let replication decide
                max_ids = filter(lambda x: x == max_id[1], scanner_ids)
                if len(max_ids) > 1:
                    self.logger.warn(_("Cannot find a scanner node, too many "
                                       "potential scanners nodes (%s). Leaving "
                                       "for replication to determine."),
                                     ",".join([ str(i) for i, c in max_ids]))
                    return
                scanner_id[0] = max_id[0]
            else:
                scanner_id[0] = scanner_ids.keys()[0]

        # Found a node to be the scanner
        headers = {'X-Container-Sysmeta-Shard-Scanner': scanner_id[0]}

        timestamp = Timestamp(time.time()).internal
        broker.update_metadata({
            'X-Container-Sysmeta-Shard-Scanner': (scanner_id[0], timestamp)})

        if not self._get_quorum(broker, op='POST', headers=headers):
            self.logger.info(_('Failed to set node %d as the pivot scanner '
                               'for %s/%s on remote servers'),
                             scanner_id, broker.account, broker.container)
            return

        self.logger.info(_('Best pivot scanner for %s/%s is node %d'),
                         broker.account, broker.container, scanner_id)

        return scanner_id

    def _get_quorum(self, broker, success=None, quorum=None, op='HEAD',
                    headers=None, post_success=None, post_fail=None,
                    account=None, container=None):
        quorum = quorum if quorum else (majority_size(self.ring.replica_count))
        local = False

        if broker:
            local = True
            account = broker.account
            container = broker.container

        if not headers:
            headers = {}

        def default_success(resp, node_id):
            return resp and is_success(resp.status)

        if not success:
            success = default_success

        path = "/%s/%s" % (account, container)
        part, nodes = self.ring.get_nodes(account, container)
        if local:
            nodes = [d for d in nodes
                     if d['ip'] not in self.ips or
                     d['port'] != self.port]

        # Send out requests to get suggested pivots and object counts.
        for node in nodes:
            self.cpool.spawn(
                self._send_request, node['ip'], node['port'], node['device'],
                part, op, path, headers, node_id=node['id'])

        successes = 1 if local else 0
        for resp, node_id in self.cpool:
            if not resp:
                continue
            if success(resp, node_id):
                successes += 1
                if post_success:
                    post_success(resp, node_id)
            else:
                if post_fail:
                    post_fail(resp, node_id)
                continue
        return successes >= quorum

    def _find_pivot_points(self, broker):
        """
        This function is the main work horse of a scanner node, it:
          - look at the pivot_ranges table to see where to continue on from.
          - Once it finds the next pivot_range it'll ask for a quorum as to
             whether this node is still in fact a scanner node.
          - If it is still the pivot, it's time to add it to the pivot_ranges
            table.

        :param broker:
        :return:
        """
        self.logger.info(_('Started searching for pivot points on %s/%s'),
                         broker.account, broker.container)

        # get the last pivot point found to continue from
        pivot_ranges = broker.get_pivot_ranges()
        marker = pivot_ranges[-1][PR_UPPER] if pivot_ranges else None
        progress = len(pivot_ranges) * self.split_size
        obj_count = [broker.get_info()['object_count']]

        def found_last():
            return progress + self.split_size >= obj_count

        if found_last():
            self.logger.info(_("Already found all pivots"))
            return

        found_pivots = []

        for i in range(self.scanner_batch_size):
            next_pivot = broker.get_next_pivot_point(marker)
            if not next_pivot:
                # something happened and we couldn't find pivot. Stop where we
                # are but don't mark complete.
                break

            progress += self.split_size
            found_pivots.append(next_pivot)
            if found_last():
                break

        if not i:
            # we didn't find anything
            self.logger.warning(_("No pivots found, something went wrong. We "
                                  "will try again next pass."))
            return

        # make sure this node is still the scanner (a split brain might have
        # happened and now someone else is).
        def on_success(resp, node_id):
            if not resp or not is_success(resp.status):
                return False

            found_scan_id = resp.getheader('X-Container-Sysmeta-Shard-Scanner')

            if found_scan_id:
                if found_scan_id != self.node_id:
                    return False

            return True

        if not self._get_quorum(broker, success=on_success):
            self.logger.info(_('Failed to reach quorum node %d may not be the '
                               'scanner for %s/%s anymore. Aborting scan.'),
                             self.node_id, broker.account, broker.container)
            return

        # we are still the scanner, so lets write the pivot points.
        pivot_ranges = []
        root_account, root_container = \
                ContainerSharder.get_shard_root_path(broker)
        piv_account = account_to_pivot_account(root_account)
        lower = marker if marker else None
        for i, pivot in enumerate(found_pivots):
            timestamp = Timestamp(time.time()).internal
            piv_name = self.generate_pivot_name(root_container, self.node_id,
                                                pivot, timestamp)
            try:
                policy = POLICIES.get_by_index(broker.storage_policy_index)
                headers = {'X-Storage-Policy': policy.name}
                self.swift.create_container(piv_account, piv_name,
                                            headers=headers)
            except internal_client.UnexpectedResponse as ex:
                self.logger.warning(_('Failed to put container: %s'), str(ex))
                self.logger.error(_('PUT of new shard containers failed, '
                                    'cancelling split of %s/%s. '
                                    'Will try again next pass'),
                                  broker.account, broker.container)
                break
            pivot_ranges.append((piv_name, timestamp, lower, pivot, 0, 0))
            lower = pivot

        if i < len(found_pivots):
            found_pivots = found_pivots[:i]
            progress = (len(pivot_ranges) + len(found_pivots)) * self.split_size

        if found_last():
            # We need to add the final pivot range as well.
            timestamp = Timestamp(time.time()).internal
            piv_name = self.generate_pivot_name(root_container, self.node_id,
                                                '', timestamp)
            pivot_ranges.append((piv_name, timestamp, lower, None, 0, 0))
            # add something the found_pivots so the stats will be correct.
            found_pivots.append('last one')
            try:
                policy = POLICIES.get_by_index(broker.storage_policy_index)
                headers = {'X-Storage-Policy': policy.name}
                self.swift.create_container(piv_account, piv_name,
                                            headers=headers)
            except internal_client.UnexpectedResponse as ex:
                self.logger.warning(_('Failed to put container: %s'), str(ex))
                self.logger.error(_('PUT of new shard containers failed, '
                                    'cancelling split of %s/%s. '
                                    'Will try again next pass'),
                                  broker.account, broker.container)

        items = self._generate_object_list(pivot_ranges, 0)
        broker.merge_items(items)

        self.logger.info(_("Scan pass completed, found %d new pivots."),
                         len(found_pivots))
        if found_last():
            # We've found the last pivot, so mark that in metadata
            timestamp = Timestamp(time.time()).internal
            broker.update_metadata({
                'X-Container-Sysmeta-Sharding-Scan-Done', (True, timestamp)})
            self.logger.info(_(" Final pivot reached."))

    def generate_pivot_name(self, container, node_id, pivot, timestamp=None):
        if not timestamp:
            timestamp = Timestamp(time.time()).internal

        md5sum = md5()
        md5sum.update("%s-%s" % (pivot, timestamp))
        return "%s-%d-%s" % (container, node_id, md5sum.hexdigest())

    def _shrink(self, broker, root_account, root_container):
        """shrinking is a 2 phase process

        Phase 1: If the container is small enough see if there is a neighbour
        to merge with. If so  then mark our intentions as metadata:
            X-Container-Sysmeta-Shard-Merge: <neighbour>
            X-Container-Sysmeta-Shard-Shrink: <this container>

        Phase 2: On a storage node running a sharder what contains a neighbour
        replica, create a new container for the new range. Move all objects in
        to it, delete self and shard-empty, update root and replicate.
        """

        if broker.metadata.get('X-Container-Sysmeta-Shard-Merge') or \
                broker.metadata.get('X-Container-Sysmeta-Shard-Shrink'):
            self._shrink_phase_2(broker, root_account, root_container)
        else:
            self._shrink_phase_1(broker, root_account, root_container)

    def _shrink_phase_1(self, broker, root_account, root_container):
        """Attempt to shrink the current sharded container.

        To shrink a container we need to:
            1. Find out if the container really has few enough objects, that is
               a quruom of counts below the threshold.
            2. Check the neighbours to see if it's possible to shrink/merge
               together, again this requires getting a quorom.
            3. Mark intentions with metadata.
        """
        if root_container == broker.container:
            # This is the root container, can't shrink it.
            return

        self.logger.info(_('Sharded container %s/%s is a candidate for '
                           'shrinking'), broker.account, broker.container)
        pivot = ContainerSharder.get_pivot_range(broker)

        obj_count = [broker.get_info()['object_count']]

        def on_success(resp):
            # We need to make sure that if this neighbour is in the middle
            # of shrinking, it isn't chosen as neighbour to merge into.
            shrink = resp.getheader('X-Container-Sysmeta-Shard-Shrink')
            merge = resp.getheader('X-Container-Sysmeta-Shard-Merge')
            if not shrink and not merge:
                oc = resp.getheader('X-Container-Object-Count')
                try:
                    oc = int(oc)
                except ValueError:
                    oc = SHARD_CONTAINER_SIZE
                obj_count[0] = \
                    max(obj_count[0], oc)
            else:
                obj_count[0] = SHARD_CONTAINER_SIZE

        if not self._get_quorum(broker, post_success=on_success):
            self.logger.info(_('Failed to get quorum on object count in '
                               '%s/%s'), broker.account, broker.container)
            return

        # We have a quorum on the number of object can we still shrink?
        if obj_count[0] > SHARD_CONTAINER_SIZE * self.shard_shrink_point:
            self.logger.info(_('After quorum check there were too many ojects '
                               ' (%d) to continue shrinking %s/%s'),
                             obj_count[0], broker.account, broker.container)
            return
        curr_obj_count = obj_count[0]

        # Now we need to find a neighbour, if possible, to merge with.
        # since we know the current range, we can make some simple assumptions.
        #   1. We know if we are on either end (lower or upper is None).
        #   2. Anything to the left we can use the lower to find the one
        #      directly before, because the upper is always included in
        #      the range. (use find_pivot_range(lower, ranges).
        #   3. Upper + something is in the next range.
        if not self.ranges:
            self.ranges = self._get_pivot_ranges(root_account, root_container,
                                                 newest=True)
            if self.ranges is None:
                self.logger.error(
                    _("Since the audit run of this container and "
                      "now we can't access the root container "
                      "%s/%s aborting."),
                    root_account, root_container)
                return
        lower_n = upper_n = None
        lower_c = upper_c = SHARD_CONTAINER_SIZE
        if pivot.lower:
            lower_n = find_pivot_range(pivot.lower, ranges)

            obj_count = [0]
            acct, cont = pivot_to_pivot_container(root_account, root_container,
                                                  pivot_range=lower_n)

            if self._get_quorum(None, post_success=on_success,
                                account=broker.account, container=cont):
                lower_c = obj_count[0]

        if pivot.upper:
            upper = str(pivot.upper)[:-1] + chr(ord(str(pivot.upper)[-1]) + 1)
            upper_n = find_pivot_range(upper, ranges)

            obj_count = [0]
            acct, cont = pivot_to_pivot_container(root_account, root_container,
                                                  pivot_range=upper_n)

            if self._get_quorum(None, post_success=on_success,
                                account=broker.account, container=cont):
                upper_c = obj_count[0]

        # got counts. now need to compare.
        neighbours = {lower_c: lower_n, upper_c: upper_n}
        smallest = min(neighbours.keys())
        if smallest + curr_obj_count > SHARD_CONTAINER_SIZE * \
                self.shrink_merge_point:
            _acct, cont = pivot_to_pivot_container(
                root_account, root_container, pivot_range=neighbours[smallest])
            self.logger.info(
                _('If this container merges with it\'s smallest neighbour (%s) '
                  'there will be too many objects. %d (merged) > %d '
                  '(shard_merge_point)'), cont, smallest + curr_obj_count,
                SHARD_CONTAINER_SIZE * self.shrink_merge_point)
            return

        # So we now have valid neighbour, so we want to move our objects in to
        # it.
        n_pivot = neighbours[smallest]

        # Now just need to update the metadata on both containers.
        shrink_meta = {
            'X-Container-Sysmeta-Shard-Merge': n_pivot.name,
            'X-Container-Sysmeta-Shard-Shrink': broker.container}
        a, c = pivot_to_pivot_container(root_account, root_container,
                                        pivot=n_pivot)

        # TODO This exception handling needs to be cleaned up.
        try:
            for acct, cont in ((broker.account, broker.container), (a, c)):
                self.swift.set_container_metadata(acct, cont, shrink_meta)
        except Exception as ex:
            self.logger.error('There was a problem adding the metadata %s' % ex)

    def _get_merge_range(self, account, container):
        path = self.swift.make_path(account, container)
        headers = dict()
        try:
            resp = self.swift.make_request('HEAD', path, headers,
                                           acceptable_statuses=(2,))
        except internal_client.UnexpectedResponse:
            self.logger.error(_("Failed to get range from %s/%s"),
                              account, container)
            return None

        lower = resp.headers.get('X-Container-Sysmeta-Shard-Lower')
        upper = resp.headers.get('X-Container-Sysmeta-Shard-Upper')

        if not lower and not upper:
            return None

        if not lower:
            lower = None
        if not upper:
            upper = None

        return PivotRange(container, lower, upper)

    def _shrink_phase_2(self, broker, root_account, root_container):
        # We've set metadata last phase. lets make sure it's still the case.
        shrink_containers = broker.get_shrinking_containers()
        shrink_shard = shrink_containers.get('shrink')
        merge_shard = shrink_containers.get('merge')

        # We move data from the shrinking container to the merge. So if this
        # isn't the shrink container, then continue.
        if not shrink_containers or broker.container != \
                shrink_containers['shrink']:
            return
        self.logger.info(_('Starting Shrinking phase 2 on %s/%s.'),
                         broker.account, broker.container)

        # OK we have a shrink container, now lets make sure we have a quorum on
        # what the containers need to be, just in case.
        def is_success(resp):
            return resp.getheader('X-Container-Sysmeta-Shard-Shrink') == \
                   shrink_shard \
                   and resp.getheader('X-Container-Sysmeta-Shard-Merge') == \
                       merge_shard

        if not self._get_quorum(broker, success=is_success):
            self.logger.info(_('Failed to reach quorum on a empty/full pivot '
                               'for shrinking %s/%s'), broker.account,
                             broker.container)
            # TODO What should be do in this situation? Just wait and hope it
            # still needs to replicate the missing or different metadata or
            # attempt to abort shrinking.
            return

        # OK so we agree, so now we can merge this container (shrink) into the
        # merge container neighbour. First lets build the new pivot
        shrink_range = self.get_pivot_range(broker)
        merge_range = self._get_merge_range(broker.account, merge_shard)
        if merge_range > shrink_range:
            merge_range.lower = shrink_range.lower
        else:
            merge_range.upper = shrink_range.upper

        policy_index = broker.storage_policy_index
        query = dict(marker='', end_marker='', prefix='', delimiter='',
                     storage_policy_index=policy_index)

        try:
            acct, cont = pivot_to_pivot_container(root_account, root_container,
                                                  pivot_range=merge_range)

            new_part, new_broker, node_id = \
                self._get_shard_broker(acct, cont, policy_index)

            self._add_shard_metadata(new_broker, root_account, root_container,
                                     merge_range)

            with new_broker.sharding_lock():
                self._add_items(broker, new_broker, query)

        except DeviceUnavailable as duex:
            self.logger.warning(_(str(duex)))
            return

        timestamp = Timestamp(time.time()).internal
        info = new_broker.get_info()
        merge_piv = (merge_range.name, merge_range.lower, timestamp,
                     merge_range.upper, "+%d" % info['object_count'],
                     "+%d" % info['bytes_used'])
        # Push the new pivot range to the root container,
        # we do this so we can short circuit PUTs.
        self._update_pivot_ranges(root_account, root_container, 'PUT',
                                  (merge_piv,))

        # We also need to remove the shrink pivot from the root container
        empty_piv = (shrink_range.name, timestamp, shrink_range.lower,
                     shrink_range.upper, 0, 0)
        self._update_pivot_ranges(root_account, root_container, 'DELETE',
                                  (empty_piv,))

        # Now we can delete the shrink container (it should be empty)
        broker.delete_db(timestamp)

        part = self.ring.get_part(broker.account, broker.container)
        self.logger.info(_('Replicating phase 2 shrunk containers %s/%s and '
                           '%s/%s'), new_broker.account, new_broker.container,
                         broker.account, broker.container)

        # Remove shrinking headers from the merge container's new broker so
        # they get cleared after replication.
        new_broker.update_metadata({
            'X-Container-Sysmeta-Shard-Merge': ('', timestamp),
            'X-Container-Sysmeta-Shard-Shrink': ('', timestamp)})

        # replicate merge container
        self.cpool.spawn(
            self._replicate_object, new_part, new_broker.db_file, node_id)
        # replicate shrink
        self.cpool.spawn(
            self._replicate_object, part, broker.db_file, node_id)
        any(self.cpool)

    def _add_items(self, broker, broker_to_update, qry, ignore_state=False):
        """
        Move items from one broker to another.

        The qry is a query dict in the form of:
            dict(marker='', end_marker='', prefix='', delimiter='',
                 storage_policy_index=policy_index)
        """
        if not ignore_state:
            db_state = broker.get_db_state()
            if db_state == DB_STATE_SHARDING:
                brokers = broker.get_brokers()
                for b in brokers:
                    q = qry.copy()
                    self._add_items(b, broker_to_update, q, ignore_state=True)
            return
        while True:
            new_items = broker.list_objects_iter(
                CONTAINER_LISTING_LIMIT, **qry)

            # Add new items
            objects = self._generate_object_list(
                new_items, broker.storage_policy_index)
            broker_to_update.merge_items(objects)

            # Delete existing (while we have the same view of the items)
            delete_objs = self._generate_object_list(
                new_items, broker.storage_policy_index, delete=True)
            broker.merge_items(delete_objs)

            if len(new_items) >= CONTAINER_LISTING_LIMIT:
                qry['marker'] = new_items[-1][0]
            else:
                break

    def _sharding_complete(self, root_account, root_container, broker):
        if root_container != broker.container:
            # We aren't in the root container.
            self._update_pivot_ranges(root_account, root_container, 'PUT',
                                      broker.get_pivot_ranges())
            # TODO question when do we delete? wait until all are done?
        broker.set_sharded_state()

    def _shard_on_pivot(self, broker, root_account, root_container):
        last_pivot = self.get_metadata_item(
            broker, 'X-Container-Sysmeta-Shard-Last-%d' % self.node_id)
        scan_complete = self.get_metadata_item(
            broker, 'X-Container-Sysmeta-Sharding-Scan-Done')

        pivot_ranges = broker.get_pivot_ranges()
        if not pivot_ranges:
            # No pivot points yet defined.
            return
        elif broker.get_db_state() == DB_STATE_UNSHARDED:
            # We have a pivot range, which means its time to start sharding
            broker.set_sharding_state()

        pivots_todo = [
            PivotRange(p[PR_NAME], p[PR_LOWER], p[PR_UPPER], p[PR_CREATED_AT])
            for p in pivot_ranges
            if p[PR_UPPER] > last_pivot or p[PR_LOWER] > last_pivot]
        if not pivots_todo:
            # This means no new pivot_ranges have been added since last pass.
            # If the scanner is complete, then we have finished sharding.
            if scan_complete:
                self._sharding_complete(broker)
                return
            else:
                self.logger.info(_('No new pivot of %s/%s found, will try '
                                   'again next pass'),
                                 broker.account, broker.container)
                return

        if last_pivot:
            self.logger.info(_('Continuing to shard %s/%s'),
                             broker.account, broker.container)
        else:
            self.logger.info(_('Starting to shard %s/%s'),
                             broker.account, broker.container)

        for i in range(self.shard_batch_size):
            if pivot_ranges:
                pivot = pivots_todo.pop(0)
            else:
                break

            self.logger.info(_('Sharding %s/%s on pivot %s'),
                             broker.account, broker.container, pivot.upper)

            policy_index = broker.storage_policy_index
            query = dict(marker='', end_marker='', prefix='', delimiter='',
                         storage_policy_index=policy_index)

            q = query.copy()
            q.update({'marker': pivot.lower or '',
                      'end_marker': pivot.upper or ''})
            if pivot.upper:
                      q.update({'include_end_marker': True})
            try:
                acct = account_to_pivot_account(root_account)
                new_part, new_broker, node_id = \
                    self._get_shard_broker(acct, pivot.name, policy_index)

                self._add_shard_metadata(new_broker, root_account,
                                         root_container, pivot)

                with new_broker.sharding_lock():
                    self._add_items(broker, new_broker, q)

            except DeviceUnavailable as duex:
                self.logger.warning(_(str(duex)))
                return

            self.logger.info(_('Replicating new shard container %s/%s'),
                             new_broker.account, new_broker.container)
            self.cpool.spawn(
                self._replicate_object, new_part, new_broker.db_file, node_id)
            last_pivot = pivot.upper
            self.logger.info(_('Node %d sharded %s/%s at pivot %s.'),
                             self.node_id, broker.account, broker.container,
                             pivot.upper)
        any(self.cpool)

        if scan_complete and not pivots_todo:
            # we've finished sharding this container.
            broker.update_metadata({
                'X-Container-Sysmeta-Shard-Last-%d' % self.node_id:
                    ('', Timestamp(time.time()).internal)})
            self._sharding_complete()
        else:
            broker.update_metadata({
                'X-Container-Sysmeta-Shard-Last-%d' % self.node_id:
                    (last_pivot, Timestamp(time.time()).internal)})

    def _push_pivot_ranges_to_container(self, pivot, root_account,
                                        root_container, pivot_point,
                                        storage_policy_index):
        # Push the new distributed node to the container.
        part, root_broker, node_id = \
            self._get_shard_broker(root_account, root_container,
                                   storage_policy_index)
        objects = self._generate_object_list(pivot_point, storage_policy_index)
        root_broker.merge_items(objects)
        self.cpool.spawn(
            self._replicate_object, part, root_broker.db_file, node_id)
        any(self.cpool)

    def run_forever(self, *args, **kwargs):
        """Run the container sharder until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin container sharder pass.'))
            begin = time.time()
            try:
                self._one_shard_pass(reported)
            except (Exception, Timeout):
                self.logger.increment('errors')
                self.logger.exception(_('ERROR sharding'))
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.logger.info(
                _('Container sharder pass completed: %.02fs'), elapsed)
            dump_recon_cache({'container_sharder_pass_completed': elapsed},
                             self.rcache, self.logger)

    def run_once(self, *args, **kwargs):
        """Run the container sharder once."""
        self.logger.info(_('Begin container sharder "once" mode'))
        begin = reported = time.time()
        self._one_shard_pass(reported)
        elapsed = time.time() - begin
        self.logger.info(
            _('Container sharder "once" mode completed: %.02fs'), elapsed)
        dump_recon_cache({'container_sharder_pass_completed': elapsed},
                         self.rcache, self.logger)
