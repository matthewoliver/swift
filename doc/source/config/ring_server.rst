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
    pipeline = catch_errors healthcheck recon ring-manager-auth ring-manager-server

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

High availability modes
=======================

``primary`` is the authoritative writer and accepts every supported method.
``readonly`` and ``standby`` serve read-only routes from synchronized state
and artefact directories, but return ``403 Forbidden`` for mutations.
The bulk ``POST .../partitions_at_risk/`` analysis route is explicitly
read-only, so it remains available to these replicas.
This keeps ring authoring ordered while allowing clients to use replicas for
discovery, status, manifests, and immutable artefact downloads.

swift-ring-manager-sync pulls one primary's latest published release into
a read-only or standby server.
It verifies the declared byte count and SHA-256 digest before recording each
immutable artefact, then writes the local release manifest and advances
latest_ring_version only after the full pull succeeds.

For example::

    swift-ring-manager-sync https://primary.example.com:6205 \
        --ring-manager-state-dir /etc/swift/ring-manager-state \
        --ring-artifact-dir /etc/swift/ring-manager-artifacts \
        --admin-key changeme \
        --state-change-hook /usr/local/bin/ring-manager-state-history

The source URL is supplied explicitly in this initial utility.
Source failover and configuration-file support are separate follow-on work.
The command records its latest attempt in ring-manager.recon below the
recon cache directory.
Place recon before ring-manager-auth to expose /recon/ring_manager without an
operator credential.

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
