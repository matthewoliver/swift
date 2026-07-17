.. _ring-manager-agent-config:

--------------------------------
Ring Manager Agent Configuration
--------------------------------

This document describes the configuration options available for the
ring-manager agent. The agent runs on Swift storage nodes and installs the
latest published ring files from one or more ring-manager servers.

An example configuration can be found at
``etc/ring-manager-agent.conf-sample`` in the source code repository.

The service is managed by ``swift-init`` as ``ring-manager-agent``. For
example::

    swift-init ring-manager-agent start
    swift-init ring-manager-agent once

The agent is included in the ``all`` and ``rest`` service groups. It is a
standalone service, so it reads ``ring-manager-agent.conf`` rather than
``ring-manager-server.conf``.

*********
[DEFAULT]
*********

The ring-manager agent uses the common Swift daemon options, including
``user``, logging, StatsD, ``nice_priority``, and ``ionice_*``.

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Option
     - Default
     - Description
   * - ``swift_dir``
     - ``/etc/swift``
     - Directory where ring files are installed.

********************
[ring-manager-agent]
********************

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Option
     - Default
     - Description
   * - ``ring_manager_urls``
     -
     - Comma-separated list of ring-manager base URLs. The agent tries each
       URL in order until one sync succeeds.
   * - ``ring_manager_url``
     -
     - Single-URL form. This is accepted for small deployments or tests.
   * - ``shuffle_ring_manager_urls``
     - ``false``
     - If true, randomize the configured URL order before each sync pass.
       This can spread storage-node pulls across equivalent read-only
       ring-manager servers.
   * - ``allow_ring_version_rollback``
     - ``false``
     - Refuse an older latest manifest than the one in local agent state.
       Enable only for a deliberate operator-managed fleet rollback.
   * - ``read_key``
     -
     - Optional value for ``X-Ring-Manager-Read-Key``. Prefer this for
       storage-node agents so nodes do not hold the write-capable admin key.
   * - ``read_key_file``
     -
     - File containing the ``read_key`` value. Mutually exclusive with
       ``read_key``. The file is read once at startup.
   * - ``ring_manager_read_key``
     -
     - Alias for ``read_key``.
   * - ``ring_manager_read_key_file``
     -
     - Alias for ``read_key_file``.
   * - ``read_auth_token``
     -
     - Optional read-only value for ``X-Auth-Token``.
   * - ``admin_key``
     -
     - Optional value for ``X-Ring-Manager-Admin-Key``. This is retained for
       backward compatibility; prefer ``read_key`` for agents.
   * - ``admin_key_file``
     -
     - File containing the ``admin_key`` value. Mutually exclusive with
       ``admin_key``. Prefer ``read_key_file`` for agents.
   * - ``ring_manager_admin_key``
     -
     - Alias for ``admin_key``.
   * - ``ring_manager_admin_key_file``
     -
     - Alias for ``admin_key_file``.
   * - ``auth_token``
     -
     - Optional value for ``X-Auth-Token``.
   * - ``interval``
     - ``300``
     - Seconds between sync attempts.
   * - ``jitter``
     - ``30``
     - Additional random delay in seconds. Jitter is applied before the first
       pass and after each subsequent pass.
   * - ``request_timeout``
     - ``30``
     - HTTP request timeout in seconds.
   * - ``recon_cache_path``
     - ``/var/cache/swift``
     - Directory used for ``ring-manager-agent.recon`` and the default state
       file.
   * - ``state_file``
     - ``/var/cache/swift/ring-manager-agent-state.json``
     - Local JSON record of the last installed manifest and files.

Secret files must be regular files owned by the service user and are read once
when the agent starts. Operators should install them with restrictive
permissions such as ``0400`` or ``0600``. Root-owned files only work if the
process reads them before dropping privileges. Empty files, loose group/other
permissions, symlinks, directories, control characters, embedded newlines, and
configured inline/file pairs fail closed at startup.

The agent writes recon data under ``/recon/ring_manager_agent``. A successful
payload includes the selected source URL, the latest ring version, installed
file counts, timing information, and ``swift_dir``. A failed payload includes
the attempted sources and per-source error messages.

When ``log_statsd_host`` is configured, the agent also emits low-cardinality
StatsD metrics:

.. code-block:: text

    agent.sync.attempts
    agent.sync.successes
    agent.sync.failures
    agent.sync.timing
    agent.source.failures
    agent.files.downloaded
    agent.files.unchanged
    agent.files.installed
    agent.bytes_downloaded
    agent.checksum_failures
    agent.install_failures
    agent.operator_attention
    agent.operator_attention.journal
    agent.operator_attention.backup_files
    agent.operator_attention.local_failures

Recon remains the best source for the current installed version and detailed
per-source error text; ``operator_attention`` identifies leftover journals,
rollback backups, or local failures needing node-local follow-up.
StatsD is intended for rates, timings, and alerting.
