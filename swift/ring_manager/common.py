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
import posixpath
import stat
import shlex
import subprocess

from urllib.parse import unquote, urlparse

from swift.common.utils.timestamp import NormalTimestamp


DEFAULT_SWIFT_DIR = '/etc/swift'
DEFAULT_RING_MANAGER_STATE_DIR = os.path.join(
    DEFAULT_SWIFT_DIR, 'ring-manager-state')
DEFAULT_RING_ARTIFACT_DIR = os.path.join(
    DEFAULT_SWIFT_DIR, 'ring-manager-artifacts')
DEFAULT_RING_BUILDER_DIR = DEFAULT_SWIFT_DIR
DEFAULT_BUILDER_LOCK_TIMEOUT = 600
DEFAULT_RING_BUILD_EXECUTOR = 'external'
RING_BUILD_EXECUTORS = ('external', 'manager')
RING_API_READONLY_FIELDS = frozenset((
    'resource_uri',
    'ever_pushed',
    'imported_at',
    'last_rebalance_time',
    'latest_swift_ring_version',
    'latest_version',
    'builder_version',
    'next_part_power',
    'partition_power_increase_state',
    'allowed_partition_power_actions',
    'device_count',
    'devices_url',
))
DEFAULT_RING_BUILD_MANAGER_WORKERS = 1
DEFAULT_BUILD_JOB_LEASE_TIMEOUT = 3600
DEFAULT_RING_MANAGER_SYNC_FRESHNESS_THRESHOLD = 300
DEFAULT_STATE_CHANGE_HOOK_TIMEOUT = 30
DEFAULT_ARTIFACT_HOOK_TIMEOUT = 30
RESERVED_ARTIFACT_VERSION_IDS = frozenset(('desired', 'latest'))
RING_MANAGER_SYNC_JOURNAL = '.ring-manager-sync-transaction.json'


def stats_increment(logger, metric, step=1):
    if not logger:
        return
    try:
        if step == 1 and hasattr(logger, 'increment'):
            logger.increment(metric)
        elif hasattr(logger, 'update_stats'):
            logger.update_stats(metric, step)
    except Exception:
        pass


def stats_timing(logger, metric, elapsed):
    if not logger or not hasattr(logger, 'timing'):
        return
    try:
        logger.timing(metric, float(elapsed) * 1000)
    except Exception:
        pass


def stats_timing_since(logger, metric, started_at):
    stats_timing(logger, metric, float(NormalTimestamp.now()) - started_at)


class StateChangeHook(object):
    """Best-effort hook for external state history or audit integrations."""

    def __init__(self, command=None, state_dir=None, timeout=None,
                 logger=None, background_runner=None):
        self.command = command
        self.state_dir = os.path.abspath(state_dir) if state_dir else None
        self.timeout = self._normal_timeout(timeout)
        self.logger = logger
        self.background_runner = background_runner

    def _normal_timeout(self, timeout):
        if timeout is None:
            return DEFAULT_STATE_CHANGE_HOOK_TIMEOUT
        timeout = float(timeout)
        if timeout == 0:
            return DEFAULT_STATE_CHANGE_HOOK_TIMEOUT
        if timeout < 0:
            raise ValueError(
                'ring_manager_state_change_hook_timeout must be non-negative')
        return timeout

    def _log_warning(self, msg, *args):
        if self.logger:
            self.logger.warning(msg, *args)

    def _relpath(self, path):
        path = os.path.abspath(path)
        if not self.state_dir:
            return path
        try:
            return os.path.relpath(path, self.state_dir)
        except ValueError:
            return path

    def run(self, action, path):
        if self.background_runner:
            try:
                self.background_runner(self._run, action, path)
            except Exception as err:
                stats_increment(self.logger, 'state_change_hook.failures')
                self._log_warning(
                    'Unable to queue ring-manager state change hook %r for '
                    '%s %s: %s', self.command, action, self._relpath(path),
                    err)
            return
        self._run(action, path)

    def _run(self, action, path):
        if not self.command:
            return
        try:
            argv = shlex.split(self.command)
        except ValueError as err:
            self._log_warning(
                'Invalid ring-manager state change hook command %r: %s',
                self.command, err)
            return
        if not argv:
            return
        path = os.path.abspath(path)
        relpath = self._relpath(path)
        env = os.environ.copy()
        env.update({
            'RING_MANAGER_STATE_ACTION': action,
            'RING_MANAGER_STATE_DIR': self.state_dir or '',
            'RING_MANAGER_STATE_PATH': path,
            'RING_MANAGER_STATE_RELPATH': relpath,
        })
        started_at = float(NormalTimestamp.now())
        try:
            proc = subprocess.Popen(
                argv, cwd=self.state_dir, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            stats_increment(self.logger, 'state_change_hook.timeouts')
            stats_increment(self.logger, 'state_change_hook.failures')
            stats_timing_since(
                self.logger, 'state_change_hook.timing', started_at)
            self._log_warning(
                'Ring-manager state change hook timed out after %s seconds '
                'for %s %s', self.timeout, action, relpath)
            return
        except (OSError, ValueError) as err:
            stats_increment(self.logger, 'state_change_hook.failures')
            stats_timing_since(
                self.logger, 'state_change_hook.timing', started_at)
            self._log_warning(
                'Unable to run ring-manager state change hook %r for %s %s: '
                '%s', self.command, action, relpath, err)
            return
        if proc.returncode:
            stats_increment(self.logger, 'state_change_hook.failures')
            stats_timing_since(
                self.logger, 'state_change_hook.timing', started_at)
            output = (stderr or stdout or b'').decode('utf-8', 'replace')
            self._log_warning(
                'Ring-manager state change hook exited %s for %s %s: %s',
                proc.returncode, action, relpath, output.strip())
            return
        stats_increment(self.logger, 'state_change_hook.successes')
        stats_timing_since(self.logger, 'state_change_hook.timing',
                           started_at)


class ArtifactLifecycleHook(object):
    """Best-effort hook for external immutable artifact publication."""

    def __init__(self, command=None, artifact_dir=None, timeout=None,
                 logger=None):
        self.command = command
        self.artifact_dir = (
            os.path.abspath(artifact_dir) if artifact_dir else None)
        self.timeout = self._normal_timeout(timeout)
        self.logger = logger

    def _normal_timeout(self, timeout):
        if timeout is None:
            return DEFAULT_ARTIFACT_HOOK_TIMEOUT
        timeout = float(timeout)
        if timeout == 0:
            return DEFAULT_ARTIFACT_HOOK_TIMEOUT
        if timeout < 0:
            raise ValueError(
                'ring_manager_artifact_hook_timeout must be non-negative')
        return timeout

    def _log_warning(self, msg, *args):
        if self.logger:
            self.logger.warning(msg, *args)

    def run(self, event, namespace, release=None, ring_id=None,
            version=None):
        if not self.command:
            return
        if not self.artifact_dir:
            stats_increment(self.logger, 'artifact_hook.failures')
            self._log_warning(
                'Unable to run ring-manager artifact hook %r without '
                'ring_artifact_dir', self.command)
            return
        try:
            argv = shlex.split(self.command)
        except ValueError as err:
            stats_increment(self.logger, 'artifact_hook.failures')
            self._log_warning(
                'Invalid ring-manager artifact hook command %r: %s',
                self.command, err)
            return
        if not argv:
            stats_increment(self.logger, 'artifact_hook.failures')
            self._log_warning(
                'Invalid empty ring-manager artifact hook command %r',
                self.command)
            return
        namespace = str(namespace)
        path = os.path.abspath(os.path.join(self.artifact_dir, namespace))
        relpath = os.path.relpath(path, self.artifact_dir)
        env = os.environ.copy()
        env.update({
            'RING_MANAGER_ARTIFACT_EVENT': str(event),
            'RING_MANAGER_ARTIFACT_DIR': self.artifact_dir or '',
            'RING_MANAGER_ARTIFACT_NAMESPACE': namespace,
            'RING_MANAGER_ARTIFACT_PATH': path,
            'RING_MANAGER_ARTIFACT_RELPATH': relpath,
            'RING_MANAGER_ARTIFACT_RELEASE': str(release or ''),
            'RING_MANAGER_ARTIFACT_RING_ID': str(ring_id or ''),
            'RING_MANAGER_ARTIFACT_VERSION': str(version or ''),
        })
        started_at = float(NormalTimestamp.now())
        try:
            proc = subprocess.Popen(
                argv, cwd=self.artifact_dir, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            stats_increment(self.logger, 'artifact_hook.timeouts')
            stats_increment(self.logger, 'artifact_hook.failures')
            stats_timing_since(
                self.logger, 'artifact_hook.timing', started_at)
            self._log_warning(
                'Ring-manager artifact hook timed out after %s seconds for '
                '%s %s', self.timeout, event, relpath)
            return
        except (OSError, ValueError) as err:
            stats_increment(self.logger, 'artifact_hook.failures')
            stats_timing_since(
                self.logger, 'artifact_hook.timing', started_at)
            self._log_warning(
                'Unable to run ring-manager artifact hook %r for %s %s: %s',
                self.command, event, relpath, err)
            return
        if proc.returncode:
            stats_increment(self.logger, 'artifact_hook.failures')
            stats_timing_since(
                self.logger, 'artifact_hook.timing', started_at)
            output = (stderr or stdout or b'').decode('utf-8', 'replace')
            self._log_warning(
                'Ring-manager artifact hook exited %s for %s %s: %s',
                proc.returncode, event, relpath, output.strip())
            return
        stats_increment(self.logger, 'artifact_hook.successes')
        stats_timing_since(self.logger, 'artifact_hook.timing', started_at)


def validate_path_component(value, field_name):
    if value in (None, ''):
        raise ValueError('%s must be a non-empty path component' %
                         field_name)
    value = str(value)
    if value in ('.', '..'):
        raise ValueError('%s must be a non-empty path component' %
                         field_name)
    if os.path.isabs(value) or '/' in value or '\\' in value:
        raise ValueError('%s must not contain path separators' % field_name)
    return value


def validate_artifact_version_id(value, field_name='version'):
    value = validate_path_component(value, field_name)
    if value in RESERVED_ARTIFACT_VERSION_IDS:
        raise ValueError('%s %r is reserved' % (field_name, value))
    return value


def resolve_artifact_path(root, path, field_name='artifact path'):
    if not root:
        raise ValueError('ring_artifact_dir is required')
    if path in (None, ''):
        raise ValueError('%s is required' % field_name)
    root = os.path.realpath(root)
    path = str(path)
    if os.path.isabs(path):
        resolved = os.path.realpath(path)
    else:
        resolved = os.path.realpath(os.path.join(root, path))
    try:
        common_path = os.path.commonpath([root, resolved])
    except ValueError:
        common_path = None
    if common_path != root:
        raise ValueError('%s escapes ring_artifact_dir' % field_name)
    return resolved


def validate_relative_api_url(value, field_name='artifact URL'):
    if not isinstance(value, str) or not value:
        raise ValueError('%s is required' % field_name)
    parsed = urlparse(value)
    if parsed.scheme or parsed.netloc:
        raise ValueError('%s must be a same-origin relative URL' % field_name)
    if parsed.fragment:
        raise ValueError('%s must not contain a fragment' % field_name)
    if not parsed.path.startswith('/api/v1/'):
        raise ValueError('%s must start with /api/v1/' % field_name)
    decoded_path = unquote(parsed.path)
    if '\\' in decoded_path:
        raise ValueError('%s must not contain backslashes' % field_name)
    if any(segment in ('.', '..') for segment in decoded_path.split('/')):
        raise ValueError('%s must not contain dot segments' % field_name)
    normalized_path = posixpath.normpath(decoded_path)
    if (normalized_path != '/api/v1' and
            not normalized_path.startswith('/api/v1/')):
        raise ValueError('%s must start with /api/v1/' % field_name)
    return value


def _configured(value):
    return value not in (None, '')


def read_secret_file(path, field_name='secret_file'):
    if not path:
        raise ValueError('%s is required' % field_name)
    flags = os.O_RDONLY
    flags |= getattr(os, 'O_NOFOLLOW', 0)
    try:
        fd = os.open(path, flags)
    except OSError as err:
        raise ValueError('Unable to open %s %s: %s' % (
            field_name, path, err))
    try:
        file_stat = os.fstat(fd)
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError('%s must be a regular file: %s' % (
                field_name, path))
        if file_stat.st_uid not in (0, os.geteuid()):
            raise ValueError(
                '%s must be owned by root or the effective service user: %s' %
                (field_name, path))
        if file_stat.st_mode & 0o077:
            raise ValueError(
                '%s must not allow group or other permissions: %s' %
                (field_name, path))
        with os.fdopen(fd, 'rb') as fp:
            fd = None
            secret = fp.read()
    except OSError as err:
        raise ValueError('Unable to read %s %s: %s' % (
            field_name, path, err))
    finally:
        if fd is not None:
            os.close(fd)

    if secret.endswith(b'\r\n'):
        secret = secret[:-2]
    elif secret.endswith(b'\n') or secret.endswith(b'\r'):
        secret = secret[:-1]
    if b'\x00' in secret:
        raise ValueError('%s must not contain NUL bytes' % field_name)
    if b'\n' in secret or b'\r' in secret:
        raise ValueError('%s must not contain embedded newlines' % field_name)
    if any(byte < 0x20 or byte == 0x7f for byte in secret):
        raise ValueError('%s must not contain control characters' % field_name)
    if not secret or not secret.strip():
        raise ValueError('%s must not be empty' % field_name)
    try:
        return secret.decode('utf-8')
    except UnicodeDecodeError as err:
        raise ValueError('%s must be valid UTF-8: %s' % (field_name, err))


def load_secret(value=None, value_name='secret', file_path=None,
                file_name='secret_file'):
    if _configured(value) and _configured(file_path):
        raise ValueError('%s and %s are mutually exclusive' % (
            value_name, file_name))
    if _configured(file_path):
        return read_secret_file(file_path, file_name)
    return value


def _first_configured(conf, names):
    for name in names:
        value = conf.get(name)
        if _configured(value):
            return value, name
    return None, None


def load_secret_from_conf(conf, value_names, file_names):
    value, value_name = _first_configured(conf, value_names)
    file_path, file_name = _first_configured(conf, file_names)
    return load_secret(value, value_name or value_names[0],
                       file_path, file_name or file_names[0])


def normal_timestamp(timestamp=None):
    if timestamp is None:
        return NormalTimestamp.now()
    if isinstance(timestamp, NormalTimestamp):
        return timestamp
    return NormalTimestamp(timestamp)


def normal_timestamp_internal(timestamp=None):
    return normal_timestamp(timestamp).internal


def normal_timestamp_float(timestamp=None):
    return float(normal_timestamp(timestamp))
