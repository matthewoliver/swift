# Copyright (c) 2010-2012 OpenStack Foundation
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

import os
import time
import traceback
from swift import gettext_ as _
from xml.etree.cElementTree import Element, SubElement, tostring

from eventlet import Timeout
from operator import itemgetter
from random import shuffle

import swift.common.db
from swift.container.backend import ContainerBroker, DATADIR, \
    RECORD_TYPE_PIVOT_NODE
from swift.container.replicator import ContainerReplicatorRpc
from swift.common import ring
from swift.common.db import DatabaseAlreadyExists
from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.request_helpers import get_param, get_listing_content_type, \
    split_and_validate_path, is_sys_or_user_meta, get_sys_meta_prefix
from swift.common.utils import get_logger, hash_path, public, \
    Timestamp, storage_directory, validate_sync_to, \
    config_true_value, json, timing_stats, replication, \
    override_bytes_from_content_type, get_log_line, pivot_to_pivot_container, \
    find_pivot_range
from swift.common.constraints import check_mount, valid_timestamp, check_utf8
from swift.common import constraints
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.http import HTTP_NOT_FOUND, is_success
from swift.common.storage_policy import POLICIES
from swift.common.base_storage_server import BaseStorageServer
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPConflict, \
    HTTPCreated, HTTPInternalServerError, HTTPNoContent, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPMethodNotAllowed, Request, Response, \
    HTTPInsufficientStorage, HTTPException, HeaderKeyDict, HTTPMovedPermanently


def gen_resp_headers(info, is_deleted=False):
    """
    Convert container info dict to headers.
    """
    # backend headers are always included
    headers = {
        'X-Backend-Timestamp': Timestamp(info.get('created_at', 0)).internal,
        'X-Backend-PUT-Timestamp': Timestamp(info.get(
            'put_timestamp', 0)).internal,
        'X-Backend-DELETE-Timestamp': Timestamp(
            info.get('delete_timestamp', 0)).internal,
        'X-Backend-Status-Changed-At': Timestamp(
            info.get('status_changed_at', 0)).internal,
        'X-Backend-Storage-Policy-Index': info.get('storage_policy_index', 0),
    }
    if not is_deleted:
        # base container info on deleted containers is not exposed to client
        headers.update({
            'X-Container-Object-Count': info.get('object_count', 0),
            'X-Container-Bytes-Used': info.get('bytes_used', 0),
            'X-Timestamp': Timestamp(info.get('created_at', 0)).normal,
            'X-PUT-Timestamp': Timestamp(
                info.get('put_timestamp', 0)).normal,
            'X-Backend-Pivot-Point': info.get('pivot_point', ''),
        })
    return headers


class ContainerController(BaseStorageServer):
    """WSGI Controller for the container server."""

    # Ensure these are all lowercase
    save_headers = ['x-container-read', 'x-container-write',
                    'x-container-sync-key', 'x-container-sync-to']
    server_type = 'container-server'

    def __init__(self, conf, logger=None):
        super(ContainerController, self).__init__(conf)
        self.logger = logger or get_logger(conf, log_route='container-server')
        self.log_requests = config_true_value(conf.get('log_requests', 'true'))
        self.root = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.ring = ring.Ring(swift_dir, ring_name='container')
        #: ContainerSyncCluster instance for validating sync-to values.
        self.realms_conf = ContainerSyncRealms(
            os.path.join(swift_dir, 'container-sync-realms.conf'),
            self.logger)
        #: The list of hosts we're allowed to send syncs to. This can be
        #: overridden by data in self.realms_conf
        self.allowed_sync_hosts = [
            h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()]
        self.replicator_rpc = ContainerReplicatorRpc(
            self.root, DATADIR, ContainerBroker, self.mount_check,
            logger=self.logger)
        self.auto_create_account_prefix = \
            conf.get('auto_create_account_prefix') or '.'
        if config_true_value(conf.get('allow_versions', 'f')):
            self.save_headers.append('x-versions-location')
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))

    def _get_container_broker(self, drive, part, account, container, **kwargs):
        """
        Get a DB broker for the container.

        :param drive: drive that holds the container
        :param part: partition the container is in
        :param account: account name
        :param container: container name
        :returns: ContainerBroker object
        """
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, drive, db_dir, hsh + '.db')
        kwargs.setdefault('account', account)
        kwargs.setdefault('container', container)
        kwargs.setdefault('logger', self.logger)
        return ContainerBroker(db_path, **kwargs)

    def get_and_validate_policy_index(self, req):
        """
        Validate that the index supplied maps to a policy.

        :returns: policy index from request, or None if not present
        :raises: HTTPBadRequest if the supplied index is bogus
        """

        policy_index = req.headers.get('X-Backend-Storage-Policy-Index', None)
        if policy_index is None:
            return None

        try:
            policy_index = int(policy_index)
        except ValueError:
            raise HTTPBadRequest(
                request=req, content_type="text/plain",
                body=("Invalid X-Storage-Policy-Index %r" % policy_index))

        policy = POLICIES.get_by_index(policy_index)
        if policy is None:
            raise HTTPBadRequest(
                request=req, content_type="text/plain",
                body=("Invalid X-Storage-Policy-Index %r" % policy_index))
        return int(policy)

    def account_update(self, req, account, container, broker):
        """
        Update the account server(s) with latest container info.

        :param req: swob.Request object
        :param account: account name
        :param container: container name
        :param broker: container DB broker object
        :returns: if all the account requests return a 404 error code,
                  HTTPNotFound response object,
                  if the account cannot be updated due to a malformed header,
                  an HTTPBadRequest response object,
                  otherwise None.
        """
        account_hosts = [h.strip() for h in
                         req.headers.get('X-Account-Host', '').split(',')]
        account_devices = [d.strip() for d in
                           req.headers.get('X-Account-Device', '').split(',')]
        account_partition = req.headers.get('X-Account-Partition', '')

        if len(account_hosts) != len(account_devices):
            # This shouldn't happen unless there's a bug in the proxy,
            # but if there is, we want to know about it.
            self.logger.error(_('ERROR Account update failed: different  '
                                'numbers of hosts and devices in request: '
                                '"%s" vs "%s"') %
                               (req.headers.get('X-Account-Host', ''),
                                req.headers.get('X-Account-Device', '')))
            return HTTPBadRequest(req=req)

        if account_partition:
            # zip is lazy on py3, but we need a list, so force evaluation.
            # On py2 it's an extra list copy, but the list is so small
            # (one element per replica in account ring, usually 3) that it
            # doesn't matter.
            updates = list(zip(account_hosts, account_devices))
        else:
            updates = []

        account_404s = 0

        for account_host, account_device in updates:
            account_ip, account_port = account_host.rsplit(':', 1)
            new_path = '/' + '/'.join([account, container])
            info = broker.get_info()
            account_headers = HeaderKeyDict({
                'x-put-timestamp': info['put_timestamp'],
                'x-delete-timestamp': info['delete_timestamp'],
                'x-object-count': info['object_count'],
                'x-bytes-used': info['bytes_used'],
                'x-trans-id': req.headers.get('x-trans-id', '-'),
                'X-Backend-Storage-Policy-Index': info['storage_policy_index'],
                'user-agent': 'container-server %s' % os.getpid(),
                'referer': req.as_referer()})
            if req.headers.get('x-account-override-deleted', 'no').lower() == \
                    'yes':
                account_headers['x-account-override-deleted'] = 'yes'
            try:
                with ConnectionTimeout(self.conn_timeout):
                    conn = http_connect(
                        account_ip, account_port, account_device,
                        account_partition, 'PUT', new_path, account_headers)
                with Timeout(self.node_timeout):
                    account_response = conn.getresponse()
                    account_response.read()
                    if account_response.status == HTTP_NOT_FOUND:
                        account_404s += 1
                    elif not is_success(account_response.status):
                        self.logger.error(_(
                            'ERROR Account update failed '
                            'with %(ip)s:%(port)s/%(device)s (will retry '
                            'later): Response %(status)s %(reason)s'),
                            {'ip': account_ip, 'port': account_port,
                             'device': account_device,
                             'status': account_response.status,
                             'reason': account_response.reason})
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR account update failed with '
                    '%(ip)s:%(port)s/%(device)s (will retry later)'),
                    {'ip': account_ip, 'port': account_port,
                     'device': account_device})
        if updates and account_404s == len(updates):
            return HTTPNotFound(req=req)
        else:
            return None

    @public
    @timing_stats()
    def DELETE(self, req):
        """Handle HTTP DELETE request."""
        drive, part, account, container, obj = split_and_validate_path(
            req, 4, 5, True)
        req_timestamp = valid_timestamp(req)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        # policy index is only relevant for delete_obj (and transitively for
        # auto create accounts)
        obj_policy_index = self.get_and_validate_policy_index(req) or 0
        broker = self._get_container_broker(drive, part, account, container)
        if account.startswith(self.auto_create_account_prefix) and obj and \
                not os.path.exists(broker.db_file):
            try:
                broker.initialize(req_timestamp.internal, obj_policy_index)
            except DatabaseAlreadyExists:
                pass
        if not os.path.exists(broker.db_file):
            return HTTPNotFound()
        if obj:     # delete object
            if len(broker.get_pivot_ranges()) > 0:
                try:
                    # This is a sharded root container, so we need figure out
                    # where the obj should live and return a 301.
                    pivotTrie = broker.build_pivot_ranges()
                    node, weight = pivotTrie.get(obj)
                    piv_acc, piv_cont = pivot_to_pivot_container(
                        account, container, node.key, weight)

                    part, nodes = self.ring.get_nodes(account, container)
                    l_ip, l_port = req.host.split(':')
                    idx = [node['index'] for node in nodes
                           if node['ip'] == l_ip and node['port'] == int(l_port)
                           and node['device'] == drive]
                    if not idx:
                        raise HTTPInternalServerError()
                    idx = idx[0]
                    part, nodes = self.ring.get_nodes(piv_acc, piv_cont)
                    headers = {
                        'X-Backend-Pivot-Account': piv_acc,
                        'X-Backend-Pivot-Container': piv_cont,
                        'X-Container-Host': "%(ip)s:%(port)d" % nodes[idx],
                        'X-Container-Device': nodes[idx]['device'],
                        'X-Container-Partition': part}
                    return HTTPMovedPermanently(headers=headers)
                except Exception:
                    return HTTPInternalServerError()
            record_type = req.headers.get('x-backend-record-type')
            if record_type == RECORD_TYPE_PIVOT_NODE:
                # Pivot point items has different information that needs to
                # be passed. This should only come from the backend.
                obj_count = req.headers.get('x-backend-pivot-objects')
                bytes_used = req.headers.get('x-backend-pivot-bytes')
                obj_timestamp = req.headers.get('x-backend-timestamp')
                req_timestamp = obj_timestamp or req.headers.get('x-timestamp')

                # Level is required when putting a pivot point.
                upper = req.headers.get('x-backend-pivot-upper')
                if not upper:
                    raise HTTPBadRequest()
                broker.delete_object(
                    obj, req_timestamp,
                    obj_policy_index, upper=upper, object_count=obj_count,
                    bytes_used=bytes_used, record_type=record_type)
            else:
                broker.delete_object(obj, req.headers.get('x-timestamp'),
                                     obj_policy_index)
            return HTTPNoContent(request=req)
        else:
            # delete container
            if not broker.empty():
                return HTTPConflict(request=req)
            existed = Timestamp(broker.get_info()['put_timestamp']) and \
                not broker.is_deleted()
            broker.delete_db(req_timestamp.internal)
            if not broker.is_deleted():
                return HTTPConflict(request=req)
            resp = self.account_update(req, account, container, broker)
            if resp:
                return resp
            if existed:
                return HTTPNoContent(request=req)
            return HTTPNotFound()

    def _update_or_create(self, req, broker, timestamp, new_container_policy,
                          requested_policy_index):
        """
        Create new database broker or update timestamps for existing database.

        :param req: the swob request object
        :param broker: the broker instance for the container
        :param timestamp: internalized timestamp
        :param new_container_policy: the storage policy index to use
                                     when creating the container
        :param requested_policy_index: the storage policy index sent in the
                                       request, may be None

        :returns: created, a bool, if database did not previously exist
        """
        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(timestamp, new_container_policy)
            except DatabaseAlreadyExists:
                pass
            else:
                return True  # created
        recreated = broker.is_deleted()
        if recreated:
            # only set storage policy on deleted containers
            broker.set_storage_policy_index(new_container_policy,
                                            timestamp=timestamp)
        elif requested_policy_index is not None:
            # validate requested policy with existing container
            if requested_policy_index != broker.storage_policy_index:
                raise HTTPConflict(request=req,
                                   headers={'x-backend-storage-policy-index':
                                            broker.storage_policy_index})
        broker.update_put_timestamp(timestamp)
        if broker.is_deleted():
            raise HTTPConflict(request=req)
        if recreated:
            broker.update_status_changed_at(timestamp)
        return recreated

    @public
    @timing_stats()
    def PUT(self, req):
        """Handle HTTP PUT request."""
        drive, part, account, container, obj = split_and_validate_path(
            req, 4, 5, True)
        req_timestamp = valid_timestamp(req)
        if 'x-container-sync-to' in req.headers:
            err, sync_to, realm, realm_key = validate_sync_to(
                req.headers['x-container-sync-to'], self.allowed_sync_hosts,
                self.realms_conf)
            if err:
                return HTTPBadRequest(err)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        requested_policy_index = self.get_and_validate_policy_index(req)
        broker = self._get_container_broker(drive, part, account, container)
        if obj:     # put container object
            if os.path.exists(broker.db_file) and \
                            len(broker.get_pivot_ranges()) > 0:
                try:
                    # This is a sharded root container, so we need figure out
                    # where the obj should live and return a 301.
                    # We will send back the details of the primary node at the
                    # same index of this one.
                    part, nodes = self.ring.get_nodes(account, container)
                    l_ip, l_port = req.host.split(':')
                    idx = [node['index'] for node in nodes
                           if node['ip'] == l_ip and node['port'] == int(l_port)
                           and node['device'] == drive]
                    if not idx:
                        raise HTTPInternalServerError()
                    idx = idx[0]
                    ranges = broker.build_pivot_ranges()
                    piv_range = find_pivot_range(obj, ranges)
                    piv_acc, piv_cont = pivot_to_pivot_container(
                        account, container, pivot_range=piv_range)

                    part, nodes = self.ring.get_nodes(piv_acc, piv_cont)
                    location = "http://%(ip)s:%(port)d/%(device)s" % nodes[idx]
                    location += "/%s/%s/%s/%s" % (part, piv_acc, piv_cont, obj)
                    headers = {
                        'Location': location,
                        'X-Backend-Pivot-Account': piv_acc,
                        'X-Backend-Pivot-Container': piv_cont,
                        'X-Container-Host': "%(ip)s:%(port)d" % nodes[idx],
                        'X-Container-Device': nodes[idx]['device'],
                        'X-Container-Partition': part}
                    return HTTPMovedPermanently(headers=headers)
                except Exception:
                    return HTTPInternalServerError()
            # obj put expects the policy_index header, default is for
            # legacy support during upgrade.
            obj_policy_index = requested_policy_index or 0
            if account.startswith(self.auto_create_account_prefix) and \
                    not os.path.exists(broker.db_file):
                try:
                    broker.initialize(req_timestamp.internal, obj_policy_index)
                except DatabaseAlreadyExists:
                    pass
            if not os.path.exists(broker.db_file):
                return HTTPNotFound()
            record_type = req.headers.get('x-backend-record-type')
            if record_type == RECORD_TYPE_PIVOT_NODE:
                # Pivot point items has different information that needs to
                # be passed. This should only come from the backend.
                obj_count = req.headers.get('x-backend-pivot-objects')
                bytes_used = req.headers.get('x-backend-pivot-bytes')
                obj_timestamp = req.headers.get('x-backend-timestamp')
                req_timestamp = obj_timestamp or req_timestamp

                # Level is required when putting a pivot point.
                upper = req.headers.get('x-backend-pivot-upper')
                if not upper:
                    raise HTTPBadRequest()
                broker.put_object(
                    obj, req_timestamp.internal,
                    int(req.headers['x-size']), '', '', 0,
                    upper=upper, object_count=obj_count,
                    bytes_used=bytes_used, record_type=record_type)
            else:
                broker.put_object(obj, req_timestamp.internal,
                                  int(req.headers['x-size']),
                                  req.headers['x-content-type'],
                                  req.headers['x-etag'], 0,
                                  obj_policy_index)
            return HTTPCreated(request=req)
        else:   # put container
            if requested_policy_index is None:
                # use the default index sent by the proxy if available
                new_container_policy = req.headers.get(
                    'X-Backend-Storage-Policy-Default', int(POLICIES.default))
            else:
                new_container_policy = requested_policy_index
            created = self._update_or_create(req, broker,
                                             req_timestamp.internal,
                                             new_container_policy,
                                             requested_policy_index)
            if req.headers.get('X-Container-Sharding'):
                sharding_sysmeta = get_sys_meta_prefix('container') + 'sharding'
                req.headers[sharding_sysmeta] = \
                    config_true_value(req.headers['X-Container-Sharding'])
            metadata = {}
            metadata.update(
                (key, (value, req_timestamp.internal))
                for key, value in req.headers.items()
                if key.lower() in self.save_headers or
                is_sys_or_user_meta('container', key))
            if 'X-Container-Sync-To' in metadata:
                if 'X-Container-Sync-To' not in broker.metadata or \
                        metadata['X-Container-Sync-To'][0] != \
                        broker.metadata['X-Container-Sync-To'][0]:
                    broker.set_x_container_sync_points(-1, -1)
            broker.update_metadata(metadata, validate_metadata=True)
            resp = self.account_update(req, account, container, broker)
            if resp:
                return resp
            if created:
                return HTTPCreated(request=req,
                                   headers={'x-backend-storage-policy-index':
                                            broker.storage_policy_index})
            else:
                return HTTPAccepted(request=req,
                                    headers={'x-backend-storage-policy-index':
                                             broker.storage_policy_index})

    def _add_metadata(self, headers, metadata):
        headers.update(
            (key, value)
            for key, (value, timestamp) in metadata.items()
            if value != '' and (key.lower() in self.save_headers or
                                is_sys_or_user_meta('container', key)))

        if headers.get('X-Container-Sysmeta-Sharding'):
            headers['X-Container-Sharding'] = \
                config_true_value(headers.get('X-Container-Sysmeta-Sharding'))

    @public
    @timing_stats(sample_rate=0.1)
    def HEAD(self, req):
        """Handle HTTP HEAD request."""
        drive, part, account, container, obj = split_and_validate_path(
            req, 4, 5, True)
        out_content_type = get_listing_content_type(req)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container,
                                            pending_timeout=0.1,
                                            stale_reads_ok=True)
        info, is_deleted = broker.get_info_is_deleted()
        headers = gen_resp_headers(info, is_deleted=is_deleted)
        if is_deleted:
            return HTTPNotFound(request=req, headers=headers)
        if len(broker.get_pivot_ranges()) > 0:
            # Sharded, so lets ask the ranges table how many objects and bytes
            # are used.
            usage = broker.get_pivot_usage()
            headers.update(
                    {'X-Container-Object-Count': usage.get('object_count', 0),
                     'X-Container-Bytes-Used': usage.get('bytes_used', 0)})
        self._add_metadata(headers, broker.metadata)
        headers['Content-Type'] = out_content_type
        return HTTPNoContent(request=req, headers=headers, charset='utf-8')

    def update_data_record(self, record, pivot=False):
        """
        Perform any mutations to container listing records that are common to
        all serialization formats, and returns it as a dict.

        Converts created time to iso timestamp.
        Replaces size with 'swift_bytes' content type parameter.

        :param record: object entry record
        :param pivot: Is this a pivot node, if so it uses a different
                      dictionary
        :returns: modified record
        """
        if isinstance(record, dict):
            # Conversion has already happened (e.g. from a sharded node)
            return record
        if pivot:
            (lower, created, upper, object_count, bytes_used) = record[:5]
            response = {'lower': lower, 'upper': upper,
                        'object_count': object_count,
                        'bytes_used': bytes_used,
                        'created_at': created}
        else:
            (name, created, size, content_type, etag) = record[:5]
            if content_type is None:
                return {'subdir': name}
            response = {'bytes': size, 'hash': etag, 'name': name,
                        'content_type': content_type}
        response['last_modified'] = Timestamp(created).isoformat
        if not pivot:
            override_bytes_from_content_type(response, logger=self.logger)
        return response

    def GET_sharded(self, req, broker, headers, marker='', end_marker='',
                    prefix='', limit=constraints.CONTAINER_LISTING_LIMIT):

        def _send_request(node, part, op, path, params):
            try:
                qs = '&'.join(['='.join(v) for v in params.iteritems()])
                with ConnectionTimeout(self.conn_timeout):
                    conn = http_connect(
                        node['ip'], node['port'], node['device'], part, op,
                        path, query_string=qs)
                with Timeout(self.node_timeout):
                    resp = conn.getresponse()
                    return resp
            except (Exception, Timeout) as ex:
                return None

        # Firstly we need the requested container's, the root container, pivot
        # tree.
        ranges = broker.build_pivot_ranges()

        # Now we need to find out where to start from.
        start = True
        spiv = epiv = None
        if marker or prefix:
            spiv = find_pivot_range(max(marker, prefix), ranges)
            start = False

        if end_marker:
            epiv = find_pivot_range(end_marker, ranges)
        end = False

        object_count = 0
        object_bytes = 0
        objects = list()
        params = req.params.copy()
        params.update({'format': 'json', 'limit': str(limit)})
        for piv_range in ranges:
            if not start and spiv:
                if piv_range == spiv:
                    start = True
                else:
                    continue

            if epiv and piv_range == epiv:
                end = True

            piv_acct, piv_cont = pivot_to_pivot_container(
                broker.account, broker.container, pivot_range=piv_range)
            path = '/%s/%s' % (piv_acct, piv_cont)
            part, nodes = self.ring.get_nodes(piv_acct, piv_cont)
            shuffle(nodes)
            for node in nodes:
                resp = _send_request(node, part, req.method, path, params)
                if resp and is_success(resp.status):
                    object_count += \
                        int(resp.getheader('X-Container-Object-Count')) or 0
                    object_bytes += \
                        int(resp.getheader('X-Container-Bytes-Used')) or 0
                    try:
                        objs = json.load(resp)
                    except ValueError as ex:
                        objs = []
                    if objs:
                        objects.extend(objs)
                        limit -= len(objs)
                    else:
                        end = True
                    break
            if end:
                break
        headers['X-Container-Object-Count'] = object_count
        headers['X-Container-Bytes-Used'] = object_bytes

        out_content_type = get_listing_content_type(req)
        return self.create_listing(req, out_content_type, {}, headers,
                                   broker.metadata, objects, broker.container)

    @public
    @timing_stats()
    def GET(self, req):
        """Handle HTTP GET request."""
        drive, part, account, container, obj = split_and_validate_path(
            req, 4, 5, True)
        path = get_param(req, 'path')
        prefix = get_param(req, 'prefix')
        delimiter = get_param(req, 'delimiter')
        nodes = get_param(req, 'nodes')
        if delimiter and (len(delimiter) > 1 or ord(delimiter) > 254):
            # delimiters can be made more flexible later
            return HTTPPreconditionFailed(body='Bad delimiter')
        marker = get_param(req, 'marker', '')
        end_marker = get_param(req, 'end_marker')
        limit = constraints.CONTAINER_LISTING_LIMIT
        given_limit = get_param(req, 'limit')
        if given_limit and given_limit.isdigit():
            limit = int(given_limit)
            if limit > constraints.CONTAINER_LISTING_LIMIT:
                return HTTPPreconditionFailed(
                    request=req,
                    body='Maximum limit is %d'
                    % constraints.CONTAINER_LISTING_LIMIT)
        out_content_type = get_listing_content_type(req)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container,
                                            pending_timeout=0.1,
                                            stale_reads_ok=True)
        info, is_deleted = broker.get_info_is_deleted()
        resp_headers = gen_resp_headers(info, is_deleted=is_deleted)
        if is_deleted:
            return HTTPNotFound(request=req, headers=resp_headers)
        kargs = {}
        if nodes and nodes.lower() == "pivot":
            container_list = broker.get_pivot_ranges()
            kargs.update(dict(pivot=True))
        elif len(broker.get_pivot_ranges()) > 0:
            # Sharded container so we need to pass to GET_sharded
            return self.GET_sharded(req, broker, resp_headers, marker,
                                    end_marker, prefix, limit)
        else:
            container_list = broker.list_objects_iter(
                limit, marker, end_marker, prefix, delimiter, path,
                storage_policy_index=info['storage_policy_index'])
        return self.create_listing(req, out_content_type, info, resp_headers,
                                   broker.metadata, container_list, container,
                                   **kargs)

    def create_listing(self, req, out_content_type, info, resp_headers,
                       metadata, container_list, container, pivot=False):
        self._add_metadata(resp_headers, metadata)
        ret = Response(request=req, headers=resp_headers,
                       content_type=out_content_type, charset='utf-8')
        if out_content_type == 'application/json':
            ret.body = json.dumps([self.update_data_record(record, pivot)
                                   for record in container_list])
        elif out_content_type.endswith('/xml'):
            doc = Element('container', name=container.decode('utf-8'))
            fields = ["name", "hash", "bytes", "content_type", "last_modified"]
            if pivot:
                fields = ["lower", "upper", "object_count", "bytes_used",
                          "last_modified"]
            for obj in container_list:
                record = self.update_data_record(obj, pivot)
                if 'subdir' in record:
                    name = record['subdir'].decode('utf-8')
                    sub = SubElement(doc, 'subdir', name=name)
                    SubElement(sub, 'name').text = name
                else:
                    obj_element = SubElement(doc, 'object')
                    for field in fields:
                        SubElement(obj_element, field).text = str(
                            record.pop(field)).decode('utf-8')
                    for field in sorted(record):
                        SubElement(obj_element, field).text = str(
                            record[field]).decode('utf-8')
            ret.body = tostring(doc, encoding='UTF-8').replace(
                "<?xml version='1.0' encoding='UTF-8'?>",
                '<?xml version="1.0" encoding="UTF-8"?>', 1)
        else:
            if not container_list:
                return HTTPNoContent(request=req, headers=resp_headers)
            if isinstance(container_list[0], dict):
                index = 'name'
            else:
                index = 0
            ret.body = '\n'.join(rec[index] for rec in container_list) + '\n'
        return ret

    @public
    @replication
    @timing_stats(sample_rate=0.01)
    def REPLICATE(self, req):
        """
        Handle HTTP REPLICATE request (json-encoded RPC calls for replication.)
        """
        post_args = split_and_validate_path(req, 3)
        drive, partition, hash = post_args
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        try:
            args = json.load(req.environ['wsgi.input'])
        except ValueError as err:
            return HTTPBadRequest(body=str(err), content_type='text/plain')
        ret = self.replicator_rpc.dispatch(post_args, args)
        ret.request = req
        return ret

    @public
    @timing_stats()
    def POST(self, req):
        """Handle HTTP POST request."""
        drive, part, account, container = split_and_validate_path(req, 4)
        req_timestamp = valid_timestamp(req)
        if 'x-container-sync-to' in req.headers:
            err, sync_to, realm, realm_key = validate_sync_to(
                req.headers['x-container-sync-to'], self.allowed_sync_hosts,
                self.realms_conf)
            if err:
                return HTTPBadRequest(err)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container)
        if broker.is_deleted():
            return HTTPNotFound(request=req)
        if req.headers.get('X-Container-Sharding'):
            sharding_sysmeta = get_sys_meta_prefix('container') + 'sharding'
            req.headers[sharding_sysmeta] = req.headers['X-Container-Sharding']

        metadata = {}
        metadata.update(
            (key, (value, req_timestamp.internal))
            for key, value in req.headers.items()
            if key.lower() in self.save_headers or
            is_sys_or_user_meta('container', key))
        if metadata:
            if 'X-Container-Sync-To' in metadata:
                if 'X-Container-Sync-To' not in broker.metadata or \
                        metadata['X-Container-Sync-To'][0] != \
                        broker.metadata['X-Container-Sync-To'][0]:
                    broker.set_x_container_sync_points(-1, -1)
            broker.update_metadata(metadata, validate_metadata=True)
        return HTTPNoContent(request=req)

    def __call__(self, env, start_response):
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)
        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8 or contains NULL')
        else:
            try:
                # disallow methods which have not been marked 'public'
                try:
                    if req.method not in self.allowed_methods:
                        raise AttributeError('Not allowed method.')
                except AttributeError:
                    res = HTTPMethodNotAllowed()
                else:
                    method = getattr(self, req.method)
                    res = method(req)
            except HTTPException as error_response:
                res = error_response
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR __call__ error with %(method)s %(path)s '),
                    {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        if self.log_requests:
            trans_time = time.time() - start_time
            log_message = get_log_line(req, res, trans_time, '')
            if req.method.upper() == 'REPLICATE':
                self.logger.debug(log_message)
            else:
                self.logger.info(log_message)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI container server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ContainerController(conf)
