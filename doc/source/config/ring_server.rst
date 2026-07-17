=================================
Ring Manager Server Configuration
=================================

The sample configuration is ``etc/ring-manager-server.conf-sample``.
For an SAIO, start with
``doc/saio/swift/ring-manager-server.conf``.

Paste pipeline
==============

A minimal authenticated pipeline is::

    [pipeline:main]
    pipeline = catch_errors healthcheck ring-manager-auth recon ring-manager-server

    [app:ring-manager-server]
    use = egg:swift#ring_manager

    [filter:ring-manager-auth]
    use = egg:swift#ring_manager_auth

    [filter:healthcheck]
    use = egg:swift#healthcheck

    [filter:recon]
    use = egg:swift#recon

    [filter:catch_errors]
    use = egg:swift#catch_errors

Server options
==============

``bind_ip``
    Address on which the WSGI server listens.
    The normal default is ``0.0.0.0``; the SAIO seed uses ``127.0.0.1``.

``bind_port``
    TCP port for the service.
    The sample and SAIO seed use ``6205``.

``workers``
    Number of preforked WSGI worker processes.
    The SAIO seed uses one worker.

``log_requests``
    Whether the application writes native Swift access-log lines.
    The default is ``true``.

``ring_manager_mode``
    Service mode.
    The default ``primary`` accepts reads and writes.
    ``readonly`` and ``standby`` accept read-only routes and reject mutations.

``ring_manager_sync_freshness_threshold``
    Seconds after the last successful ``swift-ring-manager-sync`` before a
    ``readonly`` or ``standby`` server reports synchronized published state as
    stale in ``GET /api/v1/ring_manager/status/``.
    The default is ``300``.
    This is an observability and failover-precheck value, not an automatic
    promotion mechanism or a read-path availability gate.

``ring_manager_sync_trigger_command``
    Optional root-owned wrapper which starts ``swift-ring-manager-sync`` on a
    ``readonly`` or ``standby`` server when an authenticated operator sends a
    ``POST /api/v1/ring_manager/sync/trigger/`` request.
    Requests queue at most one follow-up run and return before the pull starts.
    The command is split with shell-like quoting and executed without a shell.
    It runs with ``ring_manager_state_dir`` as its working directory and
    receives ``RING_MANAGER_SYNC_TRIGGERED_AT``,
    ``RING_MANAGER_SYNC_TRIGGER_REASON``, and
    ``RING_MANAGER_SYNC_TRIGGER_EXPECTED_LATEST`` in its environment.
    Use a wrapper script to keep source URLs, credentials, and any multi-step
    recovery logic outside the service configuration.
    The trigger only requests a local pull; it cannot push state or promote a
    standby server.

``ring_manager_sync_trigger_timeout``
    Seconds to wait for the sync-trigger wrapper.
    The default is ``0``, which does not set a timeout.

``ring_manager_state_dir``
    Root of the directory-backed JSON state.
    The default is ``/etc/swift/ring-manager-state``.

``ring_manager_state_change_hook``
    Optional best-effort command to run after each state JSON write or delete.
    The command is split with shell-like quoting and executed without a shell.
    Use a wrapper script for multi-step work.

``ring_manager_state_change_hook_timeout``
    Seconds to wait for the state-change hook.
    The default is ``30``; use ``0`` for no timeout.

``ring_artifact_dir``
    Root directory for immutable ring artefacts referenced by release manifests.
    The default is ``/etc/swift/ring-manager-artifacts``.
    A manifest path must resolve below this directory, including after symbolic links are resolved.
    Local artefact paths are never included in public API responses.

``ring_builder_dir``
    Root directory for Swift builder files.
    The default is ``/etc/swift``.
    Standard account, container, and object builder names are derived here
    unless a ring names one explicit builder file.

``max_explicit_device_id``
    Largest explicit device ID accepted by add and replace operations.
    The default is ``1000000``.
    This bounds allocation in the builder's dense device list.

``builder_lock_timeout``
    Seconds to wait for a ring builder's exclusive mutation lock.
    The default is ``600``.

``ring_build_executor``
    Controls who consumes persistent build jobs.
    The default ``external`` requires ``swift-ring-manager-builder``.
    ``manager`` runs an in-process worker for development and tests only.

``ring_build_manager_workers``
    Number of in-process workers when ``ring_build_executor`` is ``manager``.
    The default is ``1``.

``build_job_lease_timeout``
    Seconds for which a claimed job remains owned without a worker refresh.
    Expired jobs are recovered safely into the queue.
    The default is ``3600``.

Builder worker configuration
----------------------------

``swift-ring-manager-builder`` reads ``ring-manager-builder.conf``.
Its state, artifact, builder-directory, builder-lock, lease, and state-hook
settings must match the server.
``concurrency`` controls concurrent claim attempts, while builder locks and
the scoped FIFO queue keep overlapping work safe.
``interval`` is the idle poll interval and defaults to ``5`` seconds.

``max_json_request_body_size``
    Maximum JSON request size in bytes for ring-manager API requests.
    The default is ``1048576``.

``max_partitions_at_risk_selectors``
    Maximum combined number of node IP, replication IP, and device ID
    selectors accepted by one ``partitions_at_risk`` request.
    The default is ``1000``.
    The request is rejected before a builder is loaded when this limit is
    exceeded.

StatsD metrics
==============

When ``log_statsd_host`` is configured, the ring-manager server, auth
middleware, and builder daemon emit low-cardinality metrics through the normal
Swift logger StatsD client.

Useful server and auth metrics include::

    requests
    requests.timing
    return_codes.2
    return_codes.4
    return_codes.5
    errors
    readonly.rejected_mutations
    auth.unauthorized
    auth.unavailable

Build and publication metrics include::

    ring_builds.queued
    ring_builds.manifest.queued
    ring_builds.artifact_only.queued
    ring_builds.claimed
    ring_builds.completed
    ring_builds.failed
    ring_builds.deferred
    ring_builds.queue.timing
    ring_builds.build.timing
    ring_builds.total.timing
    builders.lock.timeouts
    builders.rebalance.timing
    builders.parts_changed
    artifacts.written
    artifacts.bytes

State-change hook metrics include::

    state_change_hook.successes
    state_change_hook.failures
    state_change_hook.timeouts
    state_change_hook.timing

Sync-trigger wrapper metrics include::

    sync_trigger.requests
    sync_trigger.queued
    sync_trigger.already_pending
    sync_trigger.disabled
    sync_trigger.successes
    sync_trigger.failures
    sync_trigger.timeouts
    sync_trigger.timing

``swift-ring-manager-sync`` also emits metrics when invoked with
``--log-statsd-host``::

    sync.attempts
    sync.successes
    sync.failures
    sync.source.failures
    sync.timing
    sync.manifest_files.downloaded
    sync.manifest_files.unchanged
    sync.rings_synced
    sync.ring_versions_synced
    sync.ring_version_files.downloaded
    sync.ring_version_files.unchanged
    sync.builder_files.synced
    sync.builder_files.downloaded
    sync.builder_files.unchanged
    sync.builder_files.skipped_disabled
    sync.transaction.pending
    sync.transaction.recoveries
    sync.transaction.rollbacks
    sync.transaction.committed_cleanups
    sync.transaction.recovery_failures
    sync.bytes_downloaded

Metrics intentionally avoid build IDs, ring versions, URLs, device names, and
artefact file names.
Use recon and logs for high-cardinality detail.
Agent metrics arrive with their respective component.

State change hooks
==================

Ring-manager can call an operator-defined command after a state JSON file is
written or deleted.
This supports audit or history integrations without adding git or another
history backend as a Swift runtime dependency.
It does not run for immutable artefact files.

The hook is best effort.
Ring-manager logs invalid commands, launch failures, non-zero exits, and
timeouts, but does not roll back the completed state change.
It runs with ``ring_manager_state_dir`` as its current directory and receives:

``RING_MANAGER_STATE_ACTION``
    ``write`` or ``delete``.

``RING_MANAGER_STATE_DIR``
    Absolute configured state directory.

``RING_MANAGER_STATE_PATH``
    Absolute path to the changed JSON file.

``RING_MANAGER_STATE_RELPATH``
    Path to the changed JSON file relative to the state directory.

For example, a local wrapper can record git history after every state change::

    #!/bin/sh
    set -eu
    git add -- "$RING_MANAGER_STATE_RELPATH"
    git diff --cached --quiet -- "$RING_MANAGER_STATE_RELPATH" || \
        git commit -m "ring-manager state $RING_MANAGER_STATE_ACTION: $RING_MANAGER_STATE_RELPATH"

Configure the same wrapper on the server and builder daemon when both should
record one history::

    ring_manager_state_change_hook = /usr/local/bin/ring-manager-state-history
    ring_manager_state_change_hook_timeout = 30

.. _ring_manager_sync_options:

===================
[ring-manager-sync]
===================

``swift-ring-manager-sync`` can load options from a ``[ring-manager-sync]``
section in ``ring-manager-server.conf``.
This lets the tool share the service configuration file while keeping sync
settings separate from the server's local authentication filter.
Command-line options override values from this section.

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - Option
     - Default
     - Description
   * - ``source_urls``
     - unset
     - Comma-separated ring-manager source URLs. A ``primary`` source is
       accepted directly. A ``readonly`` or ``standby`` source is accepted
       only when its status reports fresh synchronized published state.
   * - ``ring_manager_state_dir``
     - unset
     - Local state directory to populate. Usually the same value used by the
       read-only or standby ``[app:ring-manager-server]`` section.
   * - ``ring_artifact_dir``
     - unset
     - Local artefact directory to populate. Usually the same value used by
       the read-only or standby ``[app:ring-manager-server]`` section.
   * - ``ring_builder_dir``
     - ``/etc/swift``
     - Local directory to write Swift builder files when
       ``sync_builder_files`` is enabled.
   * - ``sync_builder_files``
     - ``false``
     - Whether to sync enabled-ring builder files through admin-only
       ring-manager builder endpoints. This is intended for promotable
       standby servers and requires admin credentials.
   * - ``request_timeout``
     - ``30``
     - HTTP request timeout in seconds.
   * - ``sync_lock_timeout``
     - ``30``
     - Seconds to wait for the local ``swift-ring-manager-sync`` lock before
       failing the sync attempt. This prevents overlapping sync processes from
       committing mutable state concurrently.
   * - ``recon_cache_path``
     - ``/var/cache/swift``
     - Recon cache directory for ``ring-manager.recon``.
   * - ``recon_dump``
     - ``true``
     - Whether to dump the last sync attempt to recon. The command line can
       override this with ``--recon-dump`` or ``--no-recon-dump``.
   * - ``read_key_file`` / ``read_key``
     - unset
     - Read credential for upstream ring-manager sources. File-backed
       credentials are preferred for production.
   * - ``admin_key_file`` / ``admin_key``
     - unset
     - Admin credential for upstream ring-manager sources. Read-only syncers
       should normally use read credentials instead unless
       ``sync_builder_files`` is enabled.
   * - ``read_auth_token`` / ``auth_token``
     - unset
     - Optional auth token headers for upstream ring-manager sources.
   * - ``state_change_hook``
     - unset
     - Optional best-effort command to run after synchronized state JSON
       writes.
   * - ``state_change_hook_timeout``
     - ``30``
     - Seconds to wait for ``state_change_hook``. Use ``0`` for no timeout.

Upstream credentials are explicit to ``[ring-manager-sync]``.
The syncer never borrows credentials from ``[filter:ring-manager-auth]``,
which protects local clients rather than authenticating to an upstream source.

High availability modes
=======================

``primary`` is the authoritative writer and accepts every supported method.
``readonly`` and ``standby`` serve read-only routes from synchronized state
and artefact directories, but return ``403 Forbidden`` for mutations.
The bulk ``POST .../partitions_at_risk/`` analysis route is explicitly
read-only, so it remains available to these replicas.
This keeps ring authoring ordered while allowing clients to use replicas for
discovery, status, manifests, and immutable artefact downloads.

swift-ring-manager-sync pulls a primary's latest published release, or a
fresh replica's latest published release, into a read-only or standby server.
It validates ``/api/v1/ring_manager/status/`` before downloading.
A primary is accepted directly.
A read-only or standby source must report fresh, synced, non-stale published
state with a matching latest version and valid upstream sync timestamp.
This prevents old last-known-good data from a stale replica being recorded as
a fresh downstream sync.
It verifies declared byte counts and SHA-256 digests before recording each
immutable artefact, then writes the local release manifest and advances
latest_ring_version only after the full pull succeeds.

For example::

    swift-ring-manager-sync https://primary.example.com:6205 \
        https://ring-ro-1.example.com:6205 \
        --ring-manager-state-dir /etc/swift/ring-manager-state \
        --ring-artifact-dir /etc/swift/ring-manager-artifacts \
        --log-statsd-host 127.0.0.1 \
        --read-key-file /etc/swift/secrets/ring-manager-read.key \
        --state-change-hook /usr/local/bin/ring-manager-state-history

The syncer makes only read requests.
When ``--read-key``, ``--read-key-file``, or ``--read-auth-token`` is
supplied, it takes precedence over administrator credentials so replicas do
not need a credential that can change primary state.
The direct command also accepts ``--read-key-file`` and ``--admin-key-file``;
each file option is mutually exclusive with its inline equivalent.

One or more source URLs may be supplied as positional values or comma-separated
values.
The syncer tries sources in order.
Remote source failures are recorded and fall through to the next source.
Local state, index, artifact, or cleanup failures abort the attempt rather
than mixing a partially written local state with a later source.
Mutable state JSON and synced builder files are committed with rollback
backups. If that local commit fails, the previous mutable state is restored
and ``latest_ring_version`` is not advanced. Immutable artifact files may be
left on disk after a failed attempt because they are addressed by checksum and
are not visible through ``latest`` until the state commit succeeds. A pending
sync transaction journal makes status fail closed with
``sync_transaction_pending`` until the next sync run recovers it. Recon
includes a ``sync_transaction`` summary; recovery details mirror the
``sync.transaction.*`` StatsD counters when the utility observes or recovers
a pending transaction journal. The transaction ``action`` is ``none``,
``rollback``, ``cleanup``, or ``failed``. Overlapping sync processes are
serialized by a local
``ring-manager-sync`` lock.
When the selected source is a replica, the local state preserves its upstream
``last_synced_at`` rather than stamping the downstream replica with the
current time.
The command can instead receive a config file containing a
``[ring-manager-sync]`` section::

    swift-ring-manager-sync /etc/swift/ring-manager-server.conf

See :ref:`ring_manager_sync_options` for the available settings and
command-line precedence.
By default the utility only synchronizes published state and immutable
artefacts.
For a promotable standby, ``sync_builder_files = true`` or
``--sync-builder-files`` also downloads enabled-ring builder files from
admin-only endpoints.
Builder sync requires admin credentials, verifies the byte count, SHA-256
digest, and Swift builder loadability, and writes files below
``ring_builder_dir`` before the local ``latest_ring_version`` advances.
Synced builder files are committed together with mutable state JSON using
rollback backups; if the local commit fails, the previous builder files and
state JSON are restored and ``latest`` is not advanced.
Disabled-ring builders are skipped by default.
The command records its latest attempt in ring-manager.recon below the
recon cache directory.
Place ring-manager-auth before recon so the ring-manager recon entries use the
same read-key boundary as control-plane reads.

Authentication options
======================

``admin_key``
    Inline administrator key.
    It authorises every request.

``admin_key_file``
    File containing the administrator key.
    It is mutually exclusive with ``admin_key``.

``read_key``
    Inline key for ordinary GET and HEAD requests, except paths reserved for
    builder downloads.

``read_key_file``
    File containing the read key.
    It is mutually exclusive with ``read_key``.

``allow_unauthenticated``
    Disable authentication for local development.
    The default is ``false`` and this option should not be used in production.

Credential files are read at service startup.
They must be regular files owned by root or the effective service user and
must not allow group or other permissions.
Modes such as ``0400`` or ``0600`` are suitable.

Process management
==================

The service is registered with ``swift-init`` and can be managed by its full
name or through the control-plane group::

    swift-init ring-manager-server start
    swift-init ring-manager-server reload
    swift-init control status

The server supports graceful and seamless reloads.
It is excluded from ``swift-init ... once`` because it is a long-running WSGI
service rather than a one-pass background daemon.
