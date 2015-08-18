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
import math
import os
import time
import re
from swift import gettext_ as _
from random import random

from eventlet import Timeout, GreenPool

from swift.container.replicator import ContainerReplicator
from swift.container.backend import ContainerBroker, DATADIR, \
    RECORD_TYPE_TRIE_NODE, RECORD_TYPE_OBJECT
from swift.common import ring, internal_client
from swift.common.db import DatabaseAlreadyExists
from swift.common.exceptions import DeviceUnavailable
from swift.common.request_helpers import get_container_shard_path
from swift.common.shardtrie import ShardTrieDistributedBranchException, \
    ShardTrie, DISTRIBUTED_BRANCH, to_shard_trie, CountingTrie
from swift.common.constraints import SHARD_GROUP_COUNT, CONTAINER_LISTING_LIMIT
from swift.common.ring.utils import is_local_device
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, dump_recon_cache, whataremyips, hash_path, \
    storage_directory, Timestamp
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


class ContainerSharder(ContainerReplicator):
    """Shards containers."""

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='container-sharder')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 1800))
        self.per_diff = int(conf.get('per_diff', 1000))
        self.max_diffs = int(conf.get('max_diffs') or 100)
        self.container_passes = 0
        self.container_failures = 0
        self.containers_running_time = 0
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "container.recon")
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.ring = ring.Ring(swift_dir, ring_name='container')
        self.default_port = 6001
        self.port = int(conf.get('bind_port', self.default_port))
        self.root = conf.get('devices', '/srv/node')
        self.vm_test_mode = config_true_value(conf.get('vm_test_mode', 'no'))
        concurrency = int(conf.get('concurrency', 8))
        self.cpool = GreenPool(size=concurrency)
        self.shard_group_count = int(conf.get('shard_group_count',
                                              SHARD_GROUP_COUNT))
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.reclaim_age = float(conf.get('reclaim_age', 86400 * 7))
        self.extract_device_re = re.compile('%s%s([^%s]+)' % (
            self.root, os.path.sep, os.path.sep))

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
        # TODO add actual sharding stats to track, and zero them out here.
        self.stats = {'attempted': 0, 'success': 0, 'failure': 0, 'ts_repl': 0,
                      'no_change': 0, 'hashmatch': 0, 'rsync': 0, 'diff': 0,
                      'remove': 0, 'empty': 0, 'remote_merge': 0,
                      'start': time.time(), 'diff_capped': 0}

    def _get_local_devices(self):
        self._local_device_ids = set()
        results = set()
        ips = whataremyips()
        if not ips:
            self.logger.error(_('ERROR Failed to get my own IPs?'))
            return
        for node in self.ring.devs:
            if node and is_local_device(ips, self.port,
                                        node['replication_ip'],
                                        node['replication_port']):
                results.add(node['device'])
                self._local_device_ids.add(node['id'])
        return results

    def _get_shard_trie(self, account, container, newest=False):
        path = self.swift.make_path(account, container) + \
            '?format=trie&trie_nodes=distributed'
        headers = {'X-Skip-Sharding': 'On'}
        if newest:
            headers['X-Newest'] = 'true'
        resp = self.swift.make_request('GET', path, headers,
                                       acceptable_statuses=(2,))
        return to_shard_trie(resp.body)

    def _find_shard_container_prefix(self, trie, key, account, container,
                                     trie_cache):
        try:
            _node = trie[key]
            return trie.root_key
        except ShardTrieDistributedBranchException as ex:
            dist_key = ex.key
            if dist_key in trie_cache:
                new_trie = trie_cache[dist_key]
            else:
                acct, cont = get_container_shard_path(account, container,
                                                      dist_key)
                new_trie = self._get_shard_trie(acct, cont)
                if new_trie.is_empty():
                    new_trie._root._key = ex.key
                new_trie.trim_trunk()
                trie_cache[dist_key] = new_trie
            return self._find_shard_container_prefix(new_trie, key, account,
                                                     container, trie_cache)

    def _get_shard_broker(self, account, container, policy_index):
        """
        Get a local instance of the shard container broker that will be
        pushed out.

        :param account: the account
        :param container: the container
        :returns: a local shard container broker
        """
        if container in self.shard_brokers:
            return self.shard_brokers[container][1]
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
        self.shard_brokers[container] = part, broker, node['id']
        return broker

    def _generate_object_list(self, objs_or_trie, policy_index, delete=False,
                              timestamp=None, filter_dist=False):
        """
        Create a list of dictionary items ready to be consumed by
        Broker.merge_items()

        :param objs_or_trie: list of objects or ShardTrie object
        :param policy_index: the Policy index of the container
        :param delete: mark the objects as deleted; default False
        :param timestamp: set the objects timestamp to the provided one.
               This is used specifically when deleting objects that have been
               sharded away.
        :param filter_dist: filter out distributed nodes from the list.

        :return: A list of item dictionaries ready to be consumed by
                 merge_items.
        """
        objs = list()
        if isinstance(objs_or_trie, ShardTrie):
            if filter_dist:
                item_iter = objs_or_trie.get_data_nodes()
            else:
                item_iter = objs_or_trie.get_important_nodes()
            for node in item_iter:
                if node.flag == DISTRIBUTED_BRANCH:
                    obj = {'name': node.full_key(),
                           'created_at': timestamp or node.timestamp,
                           'size': 0,
                           'content_type': '',
                           'etag': '',
                           'deleted': 1 if delete else 0,
                           'storage_policy_index': policy_index,
                           'record_type': RECORD_TYPE_TRIE_NODE}
                else:
                    obj = {'name': node.full_key(),
                           'created_at': timestamp or node.timestamp,
                           'size': node.data['size'],
                           'content_type': node.data['content_type'],
                           'etag': node.data['etag'],
                           'deleted': 1 if delete else 0,
                           'storage_policy_index': policy_index,
                           'record_type': RECORD_TYPE_OBJECT}
                objs.append(obj)
        else:
            for item in objs_or_trie:
                try:
                    obj = {
                        'name': item[0],
                        'created_at': timestamp or item[1]}
                    if len(item) > 3:
                        # object item
                        obj.update({
                            'size': item[2],
                            'content_type': item[3],
                            'etag': item[4],
                            'deleted': 1 if delete else 0,
                            'storage_policy_index': policy_index,
                            'record_type': RECORD_TYPE_OBJECT})
                    else:
                        # Trie node
                        obj.update({
                            'size': 0,
                            'content_type': '',
                            'etag': '',
                            'deleted': 1 if delete else 0,
                            'storage_policy_index': 0,
                            'record_type': RECORD_TYPE_TRIE_NODE})
                except Exception:
                    self.logger.warning(_("Failed to add object %s, not in the"
                                          'right format'),
                                        item[0] if item[0] else str(item))
                else:
                    objs.append(obj)
        return objs

    def _get_and_fill_shard_broker(self, prefix, objs_or_trie, account,
                                   container, policy_index, delete=False,
                                   timestamp=None):
        """
        Go grabs or creates a new container broker in a handoff partition
        to use as the new shard for the container. It then sets the required
        sharding metadata and adds the objects from either a list (as you get
        from the container backend) or a ShardTrie object.

        :param prefix: The prefix of the shard trie.
        :param objs_or_trie: A list of objects or a ShardTrie to grab the
               objects from.
        :param account: The root shard account (the original account).
        :param container: The root shard container (the original container).
        :param policy_index:
        :param delete:
        :return: A database broker or None (if failed to grab one)
        """
        acct, cont = get_container_shard_path(account, container, prefix)
        try:
            broker = self._get_shard_broker(acct, cont, policy_index)
        except DeviceUnavailable as duex:
            self.logger.warning(_(str(duex)))
            return None

        if not broker.metadata.get('X-Container-Sysmeta-Shard-Account') \
                and prefix:
            timestamp = Timestamp(time.time()).internal
            broker.update_metadata({
                'X-Container-Sysmeta-Shard-Account': (account, timestamp),
                'X-Container-Sysmeta-Shard-Container': (container, timestamp),
                'X-Container-Sysmeta-Shard-Prefix': (prefix, timestamp)})

        objects = self._generate_object_list(objs_or_trie, policy_index,
                                             delete, timestamp=timestamp)
        broker.merge_items(objects)

        return self.shard_brokers[cont]

    def _deal_with_misplaced_objects(self, broker, misplaced, account,
                                     container, policy_index):
        """

        :param broker: The parent broker to update once misplaced objects have
                       been moved.
        :param misplaced: The list of misplaced objects as defined by the
                          CountingTrie
        :param account: The root account
        :param container: The root container
        :param policy_index: The policy index of the container
        """
        trie_cache = {}
        shard_prefix_to_obj = {}

        # Get root shard, in case the objects should be in a previous shard.
        trie = self._get_shard_trie(account, container)
        trie_cache[''] = trie

        # [(key, self.full_key, data)]
        # for obj, _node in misplaced:
        for obj, dist_key, data in misplaced:
            prefix = self._find_shard_container_prefix(trie, obj, account,
                                                       container, trie_cache)
            if shard_prefix_to_obj.get(prefix):
                shard_prefix_to_obj[prefix].append(data)
            else:
                shard_prefix_to_obj[prefix] = [data]

        self.logger.info(_('preparing to move misplaced objects found '
                           'in %s/%s'), account, container)
        for shard_prefix, obj_list in shard_prefix_to_obj.iteritems():
            part, new_broker, node_id = self._get_and_fill_shard_broker(
                shard_prefix, obj_list, account, container, policy_index)

            self.cpool.spawn_n(
                self._replicate_object, part, new_broker.db_file, node_id)
        self.cpool.waitall()

        # wipe out the cache do disable bypass in delete_db
        cleanups = self.shard_cleanups
        self.shard_cleanups = self.shard_brokers = None
        self.logger.info('Cleaning up %d replicated shard containers',
                         len(cleanups))
        for container in cleanups.values():
            self.cpool.spawn_n(self.delete_db, container)
        self.cpool.waitall()

        # Remove the now relocated misplaced items.
        timestamp = Timestamp(time.time()).internal
        objs = [data for obj, dist_key, data in misplaced]
        items = self._generate_object_list(objs, policy_index, delete=True,
                                           timestamp=timestamp)
        broker.merge_items(items)
        self.logger.info('Finished misplaced shard replication')

    @staticmethod
    def get_shard_root_path(broker):
        """
        Attempt to get the root shard container name and account for the
        container represented by this broker.

        A container shard has 'X-Container-Sysmeta-Shard-{Account,Container}
        set, which will container the relevant values for the root shard
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

    def _audit_shard_container(self, broker, prefix, root_account=None,
                               root_container=None):
        continue_with_container = True
        if not root_account or not root_container:
            root_account, root_container = \
                ContainerSharder.get_shard_root_path(broker)

        if root_container == broker.container:
            # This is the root container, and therefore the tome of knowledge,
            # So we must assume it's correct (though I may need to this about
            # this some more).
            return continue_with_container

        root_ok = parent_ok = False
        parent_prefix = None
        # Get the root view of the world.
        trie = self._get_shard_trie(root_account, root_container, newest=True)
        try:
            node = trie.get_node(prefix)
            if node:
                root_ok = True
                # the node exists so now we need to find the parent (if there
                # is one)
                while node.parent is not None:
                    node = node.parent
                    if node.flag == DISTRIBUTED_BRANCH:
                        parent_prefix = node.full_key()
                        break
                if node.key == trie.root_key:
                    # Root is the parent.
                    parent_ok = True
            else:
                # node not in root, even worse because no distributed branch
                # exception was thrown, root should also be it's parent.
                pass
        except ShardTrieDistributedBranchException as ex:
            # Container doesn't exist in root, but we do
            # have the parent container's prefix now.
            parent_prefix = ex.key

        if parent_prefix:
            # We need to check the parent trie.
            acct, cont = get_container_shard_path(root_account, root_container,
                                                  parent_prefix)
            trie = self._get_shard_trie(acct, cont, newest=True)
            try:
                node = trie.get_node(prefix)
                if node:
                    parent_ok = True
            except ShardTrieDistributedBranchException:
                # If we get here, then we are in the strange position where
                # the parent thinks the node exists in another sub trie,
                # meaning the parent believes it is not the parent of our
                # container. This means that it's been sharded but the root
                # isn't up to date, we will mark as found for now as the
                # sharder will find the new container and audit it fixing the
                # root container if it failed, or we just happened to get a
                # root container just before it was updated.
                parent_ok = True

        if parent_ok and root_ok:
            # short circuit
            return continue_with_container

        if not parent_ok and not root_ok:
            # We need to quarantine
            self.logger.warning(_("Shard container '%s/%s' is being "
                                  "quarantined, neither the root container "
                                  "'%s/%s' or it's parent knows of it's "
                                  "existance"), broker.account,
                                broker.container, root_account, root_container)
            # TODO quarantine the container
            continue_with_container = False
            return continue_with_container

        trie = ShardTrie()
        timestamp = Timestamp(time.time()).internal
        trie.add(prefix, flag=DISTRIBUTED_BRANCH, timestamp=timestamp)
        if not parent_ok:
            # Update parent
            if parent_prefix:
                acct, cont = get_container_shard_path(root_account,
                                                      root_container,
                                                      parent_prefix)
                self.logger.info(_("Shard container '%s/%s' is missing from "
                                   "its parent container '%s/%s', "
                                   "correcting."),
                                 broker.account, broker.container, acct, cont)
                self._push_dist_node_to_container(parent_prefix, root_account,
                                                  root_container, trie,
                                                  broker.storage_policy_index)
        elif not root_ok:
            # update root container
            self.logger.info(_("Shard container '%s/%s' is missing from "
                               "the root container '%s/%s', correcting."),
                             broker.account, broker.container,
                             root_account, root_container)
            self._push_dist_node_to_container('', root_account, root_container,
                                              trie,
                                              broker.storage_policy_index)
        return continue_with_container

    def _one_shard_pass(self, reported):
        self._zero_stats()
        local_devs = self._get_local_devices()

        all_locs = audit_location_generator(self.devices, DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        self.logger.info(_('Starting container sharding pass'))
        self.shard_brokers = dict()
        self.shard_cleanups = dict()
        for path, device, partition in all_locs:
            # Only shard local containers.
            if device not in local_devs:
                continue
            broker = ContainerBroker(path)
            sharded = broker.metadata.get('X-Container-Sysmeta-Sharding') or \
                broker.metadata.get('X-Container-Sysmeta-Shard-Account')
            if not sharded:
                # Not a shard container
                continue
            if broker.is_deleted():
                # This container is deleted so we can skip it.
                # TODO may need to think about what happens if the container
                # is "deleted" but non-deleted items exist in the object table,
                # if that's even possible (eventual consistancy may, need to
                # look into what happens in container-replicator when one is
                # deleted to be sure).
                continue
            root_account, root_container = \
                ContainerSharder.get_shard_root_path(broker)
            prefix = broker.metadata.get('X-Container-Sysmeta-Shard-Prefix')
            prefix = '' if prefix is None else prefix[0]
            
            is_root = root_container == broker.container

            # Before we do any heavy lifting, lets do an audit on the shard
            # container. We grab the root's view of the dist_nodes and make
            # sure this container exists in it and in what should be it's
            # parent. If its in both great, If it exists in either but not the
            # other, then this needs to be fixed. If, however, it doesn't
            # exist in either then this container may not exist anymore so
            # quarantine it.
            if not self._audit_shard_container(broker, prefix, root_account,
                                               root_container):
                continue

            # Load everything into the count trie
            counting_trie = CountingTrie(prefix, self.shard_group_count)
            self.logger.info(_('Starting scanning for best candidate subtree'
                               ' to for sharing'))
            if not is_root:
                # Search for misplaced distributed items
                for dist_item in broker.get_shard_nodes():
                    counting_trie.add(dist_item[0], distributed=True,
                                      data=dist_item)

            # Now we need to load the objects into the CountingTrie, special
            # attention needs to be given with distributed nodes because we
            # are also using the CountingTrie as a mechanism to find misplaced
            # objects.
            marker = ''
            done = False
            obj_iter = broker.list_objects_iter(CONTAINER_LISTING_LIMIT,
                                                marker, '', '', '')
            dist_iter = iter(broker.get_shard_nodes())
            try:
                ditem = dist_iter.next()
            except StopIteration:
                ditem = None

            while not done:
                count = 0
                for item in obj_iter:
                    while ditem and (item[0].startswith(ditem[0])
                                     or item[0] > ditem[0]):
                        if item[0].startswith(ditem[0]):
                            counting_trie.add(ditem[0], True, ditem)
                        try:
                            ditem = dist_iter.next()
                        except StopIteration:
                            ditem = None

                    count += 1
                    marker = item[0]
                    counting_trie.add(item[0], data=item)
                if count < CONTAINER_LISTING_LIMIT:
                    done = True
                else:
                    obj_iter = broker.list_objects_iter(
                        CONTAINER_LISTING_LIMIT, marker, '', '', '')
            self.logger.info(_('Candidate subtree scan complete'))

            if counting_trie.misplaced:
                # There are objects that shouldn't be in this trie, that is to
                # say, they live beyond a distributed node, so we need to move
                # them to the correct node.
                self._deal_with_misplaced_objects(
                    broker, counting_trie.misplaced, root_account,
                    root_container, broker.storage_policy_index)

                self.shard_brokers = dict()
                self.shard_cleanups = dict()

            if counting_trie.candidates:
                # There is a candidate subtrie for sharding, so we can split
                # this container.
                candidate = counting_trie.candidates[0]
                self._split_container(broker, candidate)
            else:
                # There are no candidates, lets see if it's small enough to
                # merge back into it's parent (shrink).
                info = broker.get_info()

                # 10% of shard_group_count
                shrink_max = math.ceil(self.shard_group_count * 0.1)
                count = info.get('object_count')
                if count and count in (None, '', 0, '0') or \
                        int(count) < max(shrink_max, 10):
                    self._shrink_trie(broker)

        # wipe out the cache do disable bypass in delete_db
        cleanups = self.shard_cleanups
        self.shard_cleanups = None
        self.logger.info('Cleaning up %d replicated shard containers',
                         len(cleanups))

        for container in cleanups.values():
            self.cpool.spawn_n(self.delete_db, container)
        self.cpool.waitall()

        self.logger.info(_('Finished container sharding pass'))

        #all_locs = audit_location_generator(self.devices, DATADIR, '.db',
        #                                    mount_check=self.mount_check,
        #                                    logger=self.logger)
        #for path, device, partition in all_locs:
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
        #return reported

    def _split_container(self, broker, candidate):
        root_account, root_container = \
            ContainerSharder.get_shard_root_path(broker)
        is_root = root_container == broker.container
        dist_error = not is_root
        trie, _misplaced = broker.build_shard_trie(
            broker.storage_policy_index, prefix=candidate,
            dist_error=dist_error)

        node = trie[candidate]
        if node.flag == DISTRIBUTED_BRANCH:
            self.logger.warning(_('Best candidate for a shard trie '
                                  'starts with a distributed node, '
                                  'something is screwy'))
            return
        self.logger.info(_('sharding at container %s on prefix %s'),
                         broker.container, candidate)

        marker = ''
        if trie.metadata['data_node_count'] == \
                CONTAINER_LISTING_LIMIT:
            marker = trie.get_last_node()

        split_trie = trie.split_trie(candidate)

        try:
            part, new_broker, node_id = \
                self._get_and_fill_shard_broker(
                    split_trie.root_key, split_trie, root_account,
                    root_container, broker.storage_policy_index)
        except DeviceUnavailable as duex:
            self.logger.warning(_(str(duex)))
            return

        # there might be more then CONTAINER_LISTING_LIMIT items in the
        # new shard, if so add all the objects to the shard database.
        def add_other_nodes(marker, broker_to_update, delete=False,
                            timestamp=None, filter_dist=False):
            while marker:
                tmp_trie = broker.build_shard_trie(
                    broker.storage_policy_index, marker=marker,
                    prefix=candidate)

                objects = self._generate_object_list(
                    tmp_trie, broker.storage_policy_index, delete,
                    timestamp, filter_dist)
                broker_to_update.merge_items(objects)
                if tmp_trie.metadata['data_node_count'] == \
                        CONTAINER_LISTING_LIMIT:
                    marker = tmp_trie.get_last_node()
                else:
                    marker = ''

        add_other_nodes(marker, new_broker)
        # Make sure the account exists by running a container PUT
        try:
            policy = POLICIES.get_by_index(broker.storage_policy_index)
            headers = {'X-Storage-Policy': policy.name}
            self.swift.create_container(new_broker.account,
                                        new_broker.container,
                                        headers=headers)
        except internal_client.UnexpectedResponse as ex:
            self.logger.warning(_('Failed to put container: %s'),
                                str(ex))

        self.logger.info(_('Replicating new shard container %s/%s'),
                         new_broker.account, new_broker.container)
        self.cpool.spawn_n(
            self._replicate_object, part, new_broker.db_file, node_id)
        self.cpool.waitall()

        self.logger.info(_('Cleaning up sharded objects of old '
                           'container %s/%s'), broker.account,
                         broker.container)
        timestamp = Timestamp(time.time()).internal
        items = self._generate_object_list(split_trie,
                                           broker.storage_policy_index,
                                           delete=True,
                                           timestamp=timestamp,
                                           filter_dist=is_root)
        # Make sure the new distributed node has been added.
        dist_node = trie[node.full_key()]
        dist_trie = ShardTrie(root_node=dist_node)
        dist_node_item = self._generate_object_list(
            dist_trie,
            broker.storage_policy_index, False)
        items += dist_node_item
        broker.merge_items(items)

        # Again we might have to delete more then just the nodes found
        # split trie.
        if trie.metadata['data_node_count'] == \
                CONTAINER_LISTING_LIMIT:
            marker = trie.get_last_node()
            add_other_nodes(marker, broker, True, timestamp=timestamp,
                            filter_dist=is_root)

        if not is_root:
            # Push the new distributed node to the root container,
            # we do this so we can short circuit PUTs.
            self._push_dist_node_to_container('', root_account, root_container,
                                              dist_trie,
                                              broker.storage_policy_index)

        self.logger.info(_('Finished sharding %s/%s, new shard '
                           'container %s/%s. Sharded at prefix %s.'),
                         broker.account, broker.container,
                         new_broker.account, new_broker.container,
                         split_trie.root_key)

    def _push_dist_node_to_container(self, prefix, root_account,
                                     root_container, objs_or_trie,
                                     storage_policy_index):
        # Push the new distributed node to the container.
        part, root_broker, node_id = \
            self._get_and_fill_shard_broker(
                prefix, objs_or_trie, root_account, root_container,
                storage_policy_index)
        self.cpool.spawn_n(
            self._replicate_object, part, root_broker.db_file, node_id)
        self.cpool.waitall()

    def _shrink_trie(self, broker):
        root_account, root_container = \
                ContainerSharder.get_shard_root_path(broker)
        if root_container == broker.container:
            # Can't shrink the root container.
            return

        prefix = broker.metadata.get('X-Container-Sysmeta-Shard-Prefix')
        prefix = '' if prefix is None else prefix[0]

        # Find the parent node, start by getting the root container's trie
        trie = self._get_shard_trie(root_account, root_container, newest=True)
        parent_prefix = None
        try:
            node = trie.get_node(prefix)
            if node:
                # Parent node is the root container.
                parent_prefix = ''
        except ShardTrieDistributedBranchException as ex:
            parent_prefix = ex.key

        parent_acct, parent_cont = get_container_shard_path(root_account,
                                                            root_container,
                                                            parent_prefix)

        self.logger.info(_("Merging sharded container '%s' into '%s'"),
                         broker.container, parent_cont)

        # lets make some timestamps, the delete timestamp needs to be earlier
        # then timestamp of the moved files, in case the current container
        # to revived and not actually deleted and any original files are moved
        # back. Further, we need a new add timestamp because there is a chance
        # that deleted objects still exist in the parent (if the objects
        # existed before the container was split).
        timestamp = Timestamp(time.time())
        del_timestamp = timestamp.internal
        timestamp = Timestamp(int(timestamp), 1)
        add_timestamp = timestamp.internal
        trie, _misplaced = broker.build_shard_trie(broker.storage_policy_index)
        marker = ''
        if trie.metadata['data_node_count'] == CONTAINER_LISTING_LIMIT:
            marker = trie.get_last_node()

        # Start loading the parent container, note we will be removing items
        # from the current one at the same time, this way we are using a
        # consistent view of the objects.
        try:
            part, parent_broker, node_id = \
                self._get_and_fill_shard_broker(
                    parent_prefix, trie, root_account, root_container,
                    broker.storage_policy_index, timestamp=add_timestamp)
        except DeviceUnavailable as duex:
            self.logger.warning(_(str(duex)))
            return

        remove_items = self._generate_object_list(
            trie, broker.storage_policy_index, delete=True,
            timestamp=del_timestamp)
        broker.merge_items(remove_items)

        # there might be more then CONTAINER_LISTING_LIMIT items in the
        # old shard container, which it shouldn't, but you never know what
        # users will set the max_group_count too.
        while marker:
            trie = broker.build_shard_trie(
                broker.storage_policy_index, marker=marker)

            objects = self._generate_object_list(
                trie, broker.storage_policy_index, delete=False,
                timestamp=add_timestamp)
            parent_broker.merge_items(objects)
            remove_items = self._generate_object_list(
                trie, broker.storage_policy_index, delete=True,
                timestamp=del_timestamp)
            broker.merge_items(remove_items)
            if trie.metadata['data_node_count'] == \
                    CONTAINER_LISTING_LIMIT:
                marker = trie.get_last_node()
            else:
                marker = ''

        # Make sure we remove the container from the parent (and root)
        trie = ShardTrie()
        timestamp = Timestamp(time.time()).internal
        trie.add(prefix, flag=DISTRIBUTED_BRANCH, timestamp=timestamp)
        dist_node_item = self._generate_object_list(
            trie, broker.storage_policy_index, delete=True)

        parent_broker.merge_items(dist_node_item)

        # Root container also..
        self.logger.info(_("Updating root container '%s'"), root_container)
        self._push_dist_node_to_container('', root_account, root_container,
                                          trie, broker.storage_policy_index)

        # Now replicate the parent node, so the changes are pushed.
        self.logger.info(_('Replicating parent container %s/%s'),
                         parent_acct, parent_cont)
        self.cpool.spawn_n(
            self._replicate_object, part, parent_broker.db_file, node_id)
        self.cpool.waitall()

        self.logger.info(_('Cleaning up sharded objects of old '
                           'container %s/%s'), broker.account,
                         broker.container)

        # Mark current container as deleted
        broker.delete_db(timestamp)

        # Now replicate the current container
        self.logger.info(_('Replicating current container %s/%s'),
                         broker.account, broker.container)
        self.cpool.spawn_n(
            self._replicate_object, part, broker.db_file, node_id)
        self.cpool.waitall()

        self.logger.info(_('Finished merging %s/%s, back into '
                           'container %s/%s.'),
                         broker.account, broker.container,
                         parent_acct, parent_cont)

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
