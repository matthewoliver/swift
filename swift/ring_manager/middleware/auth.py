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

from swift.common.swob import HTTPServiceUnavailable, HTTPUnauthorized, Request
from swift.common.utils import config_true_value, get_logger, streq_const_time
from swift.ring_manager.common import load_secret_from_conf, stats_increment


READ_METHODS = ('GET', 'HEAD')


class RingManagerAuthMiddleware(object):
    """Key authentication for the ring-manager control plane."""

    def __init__(self, app, conf, logger=None):
        self.app = app
        self.logger = logger or get_logger(
            conf, log_route='ring-manager-auth',
            statsd_tail_prefix='ring-manager-auth')
        self.admin_key = load_secret_from_conf(
            conf, ('admin_key', 'ring_manager_admin_key'),
            ('admin_key_file', 'ring_manager_admin_key_file')) or ''
        self.read_key = load_secret_from_conf(
            conf, ('read_key', 'ring_manager_read_key'),
            ('read_key_file', 'ring_manager_read_key_file')) or ''
        self.allow_unauthenticated = config_true_value(
            conf.get('allow_unauthenticated', 'false'))

    def _authorized_with_key(self, req, key, headers):
        if not key:
            return False
        for header in headers:
            value = req.headers.get(header)
            if value and streq_const_time(value, key):
                return True
        return False

    def _admin_authorized(self, req):
        return self._authorized_with_key(
            req, self.admin_key,
            ('x-ring-manager-admin-key', 'x-auth-token'))

    def _read_authorized(self, req):
        return self._authorized_with_key(
            req, self.read_key,
            ('x-ring-manager-read-key', 'x-auth-token'))

    def _is_read_only_post(self, req):
        if req.method != 'POST':
            return False
        parts = req.path.rstrip('/').strip('/').split('/')
        return (len(parts) == 5 and parts[:3] == ['api', 'v1', 'rings'] and
                parts[4] == 'partitions_at_risk')

    def _requires_admin(self, req):
        if self._is_read_only_post(req):
            return False
        if req.method not in READ_METHODS:
            return True
        path = req.path.rstrip('/')
        return (
            path.endswith('/builder') or
            path.endswith('/builder/file'))

    def _authorized(self, req):
        if self._admin_authorized(req):
            return True
        if not self._requires_admin(req) and self._read_authorized(req):
            return True
        return False

    def _has_key_for_request(self, req):
        if not self._requires_admin(req):
            return bool(self.admin_key or self.read_key)
        return bool(self.admin_key)

    def _missing_key_message(self, req):
        if not self._requires_admin(req):
            return 'Ring manager admin_key or read_key is not configured'
        return 'Ring manager admin_key is not configured'

    def __call__(self, env, start_response):
        req = Request(env)
        if req.method == 'OPTIONS' or req.path == '/healthcheck':
            return self.app(env, start_response)
        if self.allow_unauthenticated:
            return self.app(env, start_response)
        if not self._has_key_for_request(req):
            stats_increment(self.logger, 'auth.unavailable')
            return HTTPServiceUnavailable(
                request=req, body=self._missing_key_message(req))(
                    env, start_response)
        if self._authorized(req):
            return self.app(env, start_response)
        stats_increment(self.logger, 'auth.unauthorized')
        return HTTPUnauthorized(request=req)(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def ring_manager_auth_filter(app):
        return RingManagerAuthMiddleware(app, conf)
    return ring_manager_auth_filter
