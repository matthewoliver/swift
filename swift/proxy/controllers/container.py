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

from swift import gettext_ as _
import time
import json

from six.moves.urllib.parse import unquote
from swift.common.utils import public, csv_append, Timestamp, \
    config_true_value, PivotRange, account_to_pivot_account
from swift.common.constraints import check_metadata, CONTAINER_LISTING_LIMIT
from swift.common import constraints
from swift.common.http import HTTP_ACCEPTED, is_success
from swift.common.request_helpers import get_listing_content_type
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation, set_info_cache, clear_info_cache
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPNotFound, Request
from swift.container.backend import  DB_STATE_SHARDING, DB_STATE_UNSHARDED, \
    DB_STATE_SHARDED

class ContainerController(Controller):
    """WSGI controller for container requests"""
    server_type = 'Container'

    # Ensure these are all lowercase
    pass_through_headers = ['x-container-read', 'x-container-write',
                            'x-container-sync-key', 'x-container-sync-to',
                            'x-versions-location', 'x-container-sharding']

    def __init__(self, app, account_name, container_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)

    def _x_remove_headers(self):
        st = self.server_type.lower()
        return ['x-remove-%s-read' % st,
                'x-remove-%s-write' % st,
                'x-remove-versions-location',
                'x-remove-%s-sync-key' % st,
                'x-remove-%s-sync-to' % st]

    def _convert_policy_to_index(self, req):
        """
        Helper method to convert a policy name (from a request from a client)
        to a policy index (for a request to a backend).

        :param req: incoming request
        """
        policy_name = req.headers.get('X-Storage-Policy')
        if not policy_name:
            return
        policy = POLICIES.get_by_name(policy_name)
        if not policy:
            raise HTTPBadRequest(request=req,
                                 content_type="text/plain",
                                 body=("Invalid %s '%s'"
                                       % ('X-Storage-Policy', policy_name)))
        if policy.is_deprecated:
            body = 'Storage Policy %r is deprecated' % (policy.name)
            raise HTTPBadRequest(request=req, body=body)
        return int(policy)

    def clean_acls(self, req):
        if 'swift.clean_acl' in req.environ:
            for header in ('x-container-read', 'x-container-write'):
                if header in req.headers:
                    try:
                        req.headers[header] = \
                            req.environ['swift.clean_acl'](header,
                                                           req.headers[header])
                    except ValueError as err:
                        return HTTPBadRequest(request=req, body=str(err))
        return None

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        ai = self.account_info(self.account_name, req)
        if not ai[1]:
            if 'swift.authorize' in req.environ:
                aresp = req.environ['swift.authorize'](req)
                if aresp:
                    # Don't cache this. It doesn't reflect the state of the
                    # container, just that the user can't access it.
                    return aresp
            # Don't cache this. The lack of account will be cached, and that
            # is sufficient.
            return HTTPNotFound(request=req)
        part = self.app.container_ring.get_part(
            self.account_name, self.container_name)
        concurrency = self.app.container_ring.replica_count \
            if self.app.concurrent_gets else 1
        node_iter = self.app.iter_nodes(self.app.container_ring, part)
        resp = self.GETorHEAD_base(
            req, _('Container'), node_iter, part,
            req.swift_entity_path, concurrency)
        sharding_state = \
            resp.headers.get('X-Backend-Sharding-State', DB_STATE_UNSHARDED)
        if req.method == "GET" and sharding_state in (DB_STATE_SHARDING,
                                                      DB_STATE_SHARDED):
            if not req.environ.get('swift.skip_shard'):
                new_resp = self._get_sharded(req, resp, sharding_state)
                if new_resp:
                    resp = new_resp

        # Cache this. We just made a request to a storage node and got
        # up-to-date information for the container.
        resp.headers['X-Backend-Recheck-Container-Existence'] = str(
            self.app.recheck_container_existence)
        set_info_cache(self.app, req.environ, self.account_name,
                       self.container_name, resp)
        if 'swift.authorize' in req.environ:
            req.acl = resp.headers.get('x-container-read')
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                # Don't cache this. It doesn't reflect the state of the
                # container, just that the user can't access it.
                return aresp
        if not req.environ.get('swift_owner', False):
            for key in self.app.swift_owner_headers:
                if key in resp.headers:
                    del resp.headers[key]
        return resp

    def _get_pivot_ranges(self, req, sharding_state=DB_STATE_SHARDED,
                          upto=None, account=None, container=None):

        ranges = []
        account = account if account else self.account_name
        container = container if container else self.container_name
        part, nodes = self.app.container_ring.get_nodes(account, container)

        path = "/v1/%s/%s" % (self.account_name, self.container_name)
        piv_request = Request.blank(path)
        params = req.params
        params.update({
            'items': 'pivot',
            'format': 'json'})

        if params.get('marker') and sharding_state == DB_STATE_SHARDING and \
                        params.get('marker') > upto:
            return ranges

        if params.get('end_marker') and sharding_state == DB_STATE_SHARDING:
            params.update({'end_marker': min(params['end_marker', upto])})

        piv_request.params = params.items()
        headers = {'X-Timestamp': Timestamp(time.time()).internal,
                   'X-Trans-Id': self.trans_id}
        piv_resp = self.make_requests(piv_request, self.app.container_ring,
                                      part, "GET", path, headers)
        if not is_success(piv_resp.status_int):
            return ranges

        try:
            for pivot in json.loads(piv_resp.body):
                lower = pivot.get('lower') or None
                upper = pivot.get('upper') or None
                created_at = pivot.get('created_at') or None
                object_count = pivot.get('object_count') or 0
                bytes_used = pivot.get('bytes_used') or 0
                meta_timestamp = pivot.get('meta_timestamp') or None
                ranges.append(PivotRange(pivot['name'], created_at, lower,
                                         upper, object_count, bytes_used,
                                         meta_timestamp))
        except ValueError:
            # Failed to decode the json response
            return ranges

    def _get_sharded(self, req, resp, sharding_state):
        # if sharding, we need to vist all the pivots before the upto and
        # merge with this response.
        upto = None
        if sharding_state == DB_STATE_SHARDING:
            def filter_key(x):
                x[0].startswith('X-Container-Sysmeta-Shard-Last-')

            uptos = filter(filter_key, req.headers.items())
            if uptos:
                upto = max(uptos, key=lambda x: x[-1])[0]

            if not upto:
                # nothing has been sharded yet, so lets short circuit and
                # return the current response.
                return None

        # In whatever case we need the list of PivotRanges that contain this
        # range
        ranges = self._get_pivot_ranges(req, sharding_state, upto)
        if not ranges:
            # can't find ranges or there was a problem getting the ranges. So
            # return what we have.
            return None

        out_content_type = get_listing_content_type(req)
        if sharding_state == DB_STATE_SHARDING and out_content_type != \
                'application/json':
            # We need the initial container response to be json format.
            new_req = Request.blank(environ=req.environ.copy())
            new_req.environ['swift.skip_shard'] = True
            new_req.params = new_req.params['format'] = 'json'
            resp = self.GET(new_req)
            if not is_success(resp.status_int):
                return resp

        piv_account = account_to_pivot_account(self.account_name)
        object_count = 0
        object_bytes = 0
        limit = req.params.get('limit', CONTAINER_LISTING_LIMIT)
        root_container_resp = None
        for piv_range in ranges:
            if sharding_state == DB_STATE_SHARDING and piv_account > upto:
                if not root_container_resp:
                    # get the response
                    # TODO dont re-get the resp above, do it now. Also got
                    #    the < piv range wrong above.
                    pass
        

    @public
    @delay_denial
    @cors_validation
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req)

    @public
    @delay_denial
    @cors_validation
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req)

    @public
    @cors_validation
    def PUT(self, req):
        """HTTP PUT request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        policy_index = self._convert_policy_to_index(req)
        if not req.environ.get('swift_owner'):
            for key in self.app.swift_owner_headers:
                req.headers.pop(key, None)
        if len(self.container_name) > constraints.MAX_CONTAINER_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Container name length of %d longer than %d' % \
                        (len(self.container_name),
                         constraints.MAX_CONTAINER_NAME_LENGTH)
            return resp
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts and self.app.account_autocreate:
            self.autocreate_account(req, self.account_name)
            account_partition, accounts, container_count = \
                self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        if 0 < self.app.max_containers_per_account <= container_count and \
                self.account_name not in self.app.max_containers_whitelist:
            container_info = \
                self.container_info(self.account_name, self.container_name,
                                    req)
            if not is_success(container_info.get('status')):
                resp = HTTPForbidden(request=req)
                resp.body = 'Reached container limit of %s' % \
                    self.app.max_containers_per_account
                return resp
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self._backend_requests(req, len(containers),
                                         account_partition, accounts,
                                         policy_index)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        resp = self.make_requests(
            req, self.app.container_ring,
            container_partition, 'PUT', req.swift_entity_path, headers)
        return resp

    @public
    @cors_validation
    def POST(self, req):
        """HTTP POST request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        if not req.environ.get('swift_owner'):
            for key in self.app.swift_owner_headers:
                req.headers.pop(key, None)
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self.generate_request_headers(req, transfer=True)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        resp = self.make_requests(
            req, self.app.container_ring, container_partition, 'POST',
            req.swift_entity_path, [headers] * len(containers))
        return resp

    @public
    @cors_validation
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self._backend_requests(req, len(containers),
                                         account_partition, accounts)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        resp = self.make_requests(
            req, self.app.container_ring, container_partition, 'DELETE',
            req.swift_entity_path, headers)
        # Indicates no server had the container
        if resp.status_int == HTTP_ACCEPTED:
            return HTTPNotFound(request=req)
        return resp

    def _backend_requests(self, req, n_outgoing, account_partition, accounts,
                          policy_index=None):
        additional = {'X-Timestamp': Timestamp(time.time()).internal}
        if policy_index is None:
            additional['X-Backend-Storage-Policy-Default'] = \
                int(POLICIES.default)
        else:
            additional['X-Backend-Storage-Policy-Index'] = str(policy_index)
        headers = [self.generate_request_headers(req, transfer=True,
                                                 additional=additional)
                   for _junk in range(n_outgoing)]

        for i, account in enumerate(accounts):
            i = i % len(headers)

            headers[i]['X-Account-Partition'] = account_partition
            headers[i]['X-Account-Host'] = csv_append(
                headers[i].get('X-Account-Host'),
                '%(ip)s:%(port)s' % account)
            headers[i]['X-Account-Device'] = csv_append(
                headers[i].get('X-Account-Device'),
                account['device'])

        return headers
