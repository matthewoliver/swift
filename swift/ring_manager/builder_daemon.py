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

import os
import socket
import sys

from swift.common.concurrency import GreenPile, sleep, spawn
from swift.common.daemon import Daemon, run_daemon
from swift.common.utils import config_positive_float_value, \
    config_positive_int_value, get_logger, non_negative_float, parse_options
from swift.ring_manager.builder import RingBuilderManagerError
from swift.ring_manager.common import DEFAULT_BUILD_JOB_LEASE_TIMEOUT, \
    DEFAULT_BUILDER_LOCK_TIMEOUT, DEFAULT_RING_ARTIFACT_DIR, \
    DEFAULT_RING_BUILDER_DIR, DEFAULT_RING_MANAGER_STATE_DIR, \
    NormalTimestamp, normal_timestamp
from swift.ring_manager.publisher import RingBuilderPublisher, \
    RingBuilderPublisherDeferred, RingBuilderPublisherError
from swift.ring_manager.store import RingManagerStore, RingNotFound


DEFAULT_INTERVAL = 5.0
DEFAULT_CONCURRENCY = 1
USER_AGENT = 'swift-ring-manager-builder'


class RingBuildWorker(object):
    """Claim persistent build jobs and publish their immutable results."""

    def __init__(self, store, publisher, builder_id=None, logger=None,
                 time_func=NormalTimestamp.now,
                 lease_timeout=DEFAULT_BUILD_JOB_LEASE_TIMEOUT,
                 lease_refresh_interval=None):
        self.store = store
        self.publisher = publisher
        self.builder_id = builder_id or '%s:%s' % (
            socket.gethostname(), os.getpid())
        self.logger = logger or get_logger({}, log_route=USER_AGENT)
        self.time_func = time_func
        self.lease_timeout = float(lease_timeout)
        if self.lease_timeout <= 0:
            raise ValueError('build_job_lease_timeout must be positive')
        if lease_refresh_interval is None:
            lease_refresh_interval = min(60.0, max(
                1.0, self.lease_timeout / 3.0))
        self.lease_refresh_interval = float(lease_refresh_interval)
        if self.lease_refresh_interval <= 0:
            raise ValueError('build_job_lease_refresh_interval must be '
                             'positive')

    def _timestamp(self, timestamp=None):
        timestamp = self.time_func() if timestamp is None else timestamp
        return normal_timestamp(timestamp)

    def _refresh_lease(self, job):
        now = self._timestamp()
        return self.store.refresh_ring_build_lease(
            job['id'], self.builder_id, job['claimed_at'], now.internal,
            lease_timeout=self.lease_timeout)

    def _lease_renewer(self, job, stopped):
        while not stopped[0]:
            sleep(self.lease_refresh_interval)
            if stopped[0]:
                break
            try:
                if self._refresh_lease(job) is None:
                    return
            except Exception:
                self.logger.exception(
                    'Unable to refresh lease for ring build %s', job['id'])

    def _start_lease_renewer(self, job):
        stopped = [False]
        return stopped, spawn(self._lease_renewer, job, stopped)

    def _stop_lease_renewer(self, stopped, renewer):
        stopped[0] = True
        renewer.kill()

    def _update_claimed_job(self, job, updates, timestamp):
        updated = self.store.update_claimed_ring_build(
            job['id'], self.builder_id, job['claimed_at'], updates,
            timestamp.internal)
        if updated is None:
            self.logger.warning(
                'Ring build %(build_id)s lost its lease before %(state)s',
                {'build_id': job['id'], 'state': updates.get('state')})
        return updated

    def process_job(self, build_id=None):
        claimed_at = self._timestamp()
        job = self.store.claim_ring_build(
            self.builder_id, claimed_at.internal, build_id=build_id,
            lease_timeout=self.lease_timeout)
        if job is None:
            return None
        stopped, renewer = self._start_lease_renewer(job)
        try:
            try:
                result = self.publisher.publish(job.get('request') or {})
            except RingBuilderPublisherDeferred as err:
                now = self._timestamp()
                retry_after = max(0, int(err.retry_after or 0))
                self.logger.info(
                    'Ring build %(build_id)s deferred: %(error)s',
                    {'build_id': job['id'], 'error': err})
                return self._update_claimed_job(job, {
                    'state': 'deferred',
                    'deferred_at': now.internal,
                    'deferred_until': normal_timestamp(
                        float(now) + retry_after).internal,
                    'defer_reason': err.reason or 'deferred',
                    'deferred_ring_id': err.ring_id,
                    'error': str(err),
                }, now)
            except (RingNotFound, RingBuilderPublisherError,
                    RingBuilderManagerError, ValueError) as err:
                completed_at = self._timestamp()
                self.logger.error(
                    'Ring build %(build_id)s failed: %(error)s',
                    {'build_id': job['id'], 'error': err})
                return self._update_claimed_job(job, {
                    'state': 'failed',
                    'completed_at': completed_at.internal,
                    'error': str(err),
                }, completed_at)
            except Exception as err:
                completed_at = self._timestamp()
                self.logger.exception(
                    'Unexpected error while processing ring build %s',
                    job['id'])
                return self._update_claimed_job(job, {
                    'state': 'failed',
                    'completed_at': completed_at.internal,
                    'error': str(err),
                }, completed_at)

            completed_at = self._timestamp()
            updates = {
                'state': 'completed',
                'completed_at': completed_at.internal,
                'result': result,
            }
            if result.get('version') is not None:
                updates['version'] = result['version']
            return self._update_claimed_job(job, updates, completed_at)
        finally:
            self._stop_lease_renewer(stopped, renewer)

    def process_jobs(self, max_jobs=None):
        jobs = []
        while max_jobs is None or len(jobs) < max_jobs:
            job = self.process_job()
            if job is None:
                break
            jobs.append(job)
        return jobs


class RingManagerBuilder(Daemon):
    """Daemon that consumes persistent ring-manager build jobs."""

    def __init__(self, conf):
        conf = conf or {}
        self.conf = conf
        self.logger = get_logger(conf, log_route=USER_AGENT)
        self.state_dir = conf.get(
            'ring_manager_state_dir', DEFAULT_RING_MANAGER_STATE_DIR)
        self.ring_artifact_dir = conf.get(
            'ring_artifact_dir', DEFAULT_RING_ARTIFACT_DIR)
        self.ring_builder_dir = conf.get(
            'ring_builder_dir', DEFAULT_RING_BUILDER_DIR)
        self.interval = non_negative_float(
            conf.get('interval', DEFAULT_INTERVAL))
        self.concurrency = config_positive_int_value(
            conf.get('concurrency', DEFAULT_CONCURRENCY))
        self.builder_lock_timeout = non_negative_float(conf.get(
            'builder_lock_timeout', DEFAULT_BUILDER_LOCK_TIMEOUT))
        self.build_job_lease_timeout = config_positive_float_value(conf.get(
            'build_job_lease_timeout', DEFAULT_BUILD_JOB_LEASE_TIMEOUT))
        self.store = RingManagerStore(
            state_dir=self.state_dir,
            ring_artifact_dir=self.ring_artifact_dir)
        self.publisher = RingBuilderPublisher(
            self.store, ring_builder_dir=self.ring_builder_dir,
            ring_artifact_dir=self.ring_artifact_dir,
            builder_lock_timeout=self.builder_lock_timeout,
            logger=self.logger)
        self.worker = RingBuildWorker(
            self.store, self.publisher, logger=self.logger,
            lease_timeout=self.build_job_lease_timeout)

    def run_once(self, *args, **kwargs):
        processed = 0
        while True:
            pile = GreenPile(self.concurrency)
            for _junk in range(self.concurrency):
                pile.spawn(self.worker.process_jobs, 1)
            jobs = []
            for worker_jobs in pile:
                jobs.extend(worker_jobs)
            if not jobs:
                break
            for job in jobs:
                processed += 1
                self.logger.info(
                    'Processed ring build %(id)s with state %(state)s',
                    {'id': job['id'], 'state': job['state']})
        return processed

    def run_forever(self, *args, **kwargs):
        while True:
            if not self.run_once(*args, **kwargs):
                sleep(self.interval)


def main():
    conf_file, options = parse_options(once=True)
    run_daemon(RingManagerBuilder, conf_file, 'ring-manager-builder',
               **options)


if __name__ == '__main__':
    sys.exit(main())
