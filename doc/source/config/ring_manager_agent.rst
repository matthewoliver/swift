.. _ring-manager-agent-config:

--------------------------------
Ring Manager Agent Configuration
--------------------------------

This document describes the configuration options available for the
ring-manager agent. The agent runs on Swift storage nodes. In ``enforce``
mode it installs the desired ring files from one or more ring-manager servers.
In ``observe`` mode it inventories local ring files without using
ring-manager. In ``validate-only`` mode it compares local ring files with one
selected release without downloading or installing artifacts.

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
   * - ``mode``
     - ``enforce``
     - Agent operating mode. ``enforce`` polls ring-manager and installs ring
       files. ``observe`` reads local ``*.ring.gz`` files, validates their
       Swift ring format, and reports inventory without contacting
       ring-manager or writing under ``swift_dir``. ``validate-only`` fetches
       one selected release manifest and reports local file convergence
       without downloading artifacts or writing under ``swift_dir``.
   * - ``ring_manager_urls``
     -
     - Comma-separated list of ring-manager base URLs. Required in
       ``enforce`` and ``validate-only`` modes and unused in ``observe`` mode.
       The agent tries each URL in order until one pass succeeds.
   * - ``ring_manager_url``
     -
     - Single-URL form. This is accepted for small deployments or tests.
   * - ``release``
     -
     - Release selector required in ``validate-only`` mode. Use an immutable
       release id for a stable known baseline. The special value ``latest``
       explicitly follows the moving latest pointer. This option is rejected
       in ``enforce`` and ``observe`` modes.
   * - ``shuffle_ring_manager_urls``
     - ``false``
     - If true, randomize the configured URL order before each sync pass.
       This can spread storage-node pulls across equivalent read-only
       ring-manager servers.
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
   * - ``allow_ring_version_rollback``
     - ``false``
     - In ``enforce`` mode, allow installation of a desired manifest older
       than the last installed manifest. This does not affect
       ``validate-only``; selecting an older release for comparison never
       installs it.
   * - ``lock_timeout``
     - ``10``
     - Maximum time to wait for the local install lock in ``enforce`` mode.
       Observe and validate-only do not take the install lock.
   * - ``recon_cache_path``
     - ``/var/cache/swift``
     - Directory used for ``ring-manager-agent.recon`` and the default state
       file.
   * - ``state_file``
     - ``/var/cache/swift/ring-manager-agent-state.json``
     - Local JSON record of the last installed manifest and files in
       ``enforce`` mode. Validate-only does not read or write this file.

Secret files must be regular files owned by the service user and are read once
when the agent starts. Operators should install them with restrictive
permissions such as ``0400`` or ``0600``. Root-owned files only work if the
process reads them before dropping privileges. Empty files, loose group/other
permissions, symlinks, directories, control characters, embedded newlines, and
configured inline/file pairs fail closed at startup.

The agent writes recon data under ``/recon/ring_manager_agent``. Every payload
includes ``mode``. A successful ``enforce`` payload includes the selected
source URL, ``desired_ring_version``, installed file counts, timing
information, and ``swift_dir``. The local state file records the same concrete
release as ``installed_ring_version``. A successful ``observe`` payload
includes each local ring file's size, modification time, SHA-256 checksum,
Swift ring version, part power, replica count, and validation status. A
successful ``validate-only`` payload includes the configured selector,
resolved release, source,
convergence result, comparison counts, and per-file expected and local
metadata. Per-file comparison states
are ``matching``, ``stale``, ``missing``, ``unknown``, ``extra``, and
``error``. ``unknown`` means the selected manifest did not provide a usable
SHA-256 checksum, so matching bytes cannot be proved. A completed comparison
is a successful pass even when it is not converged; differences set
``operator_attention``. The selected manifest must contain at least one
``*.ring.gz`` file. A failed payload includes the attempted sources and
per-source error messages when a source was used.

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
    agent.observe.attempts
    agent.observe.successes
    agent.observe.failures
    agent.observe.timing
    agent.observe.files
    agent.observe.files_valid
    agent.observe.files_invalid
    agent.validate.attempts
    agent.validate.successes
    agent.validate.failures
    agent.validate.timing
    agent.validate.converged
    agent.validate.not_converged
    agent.validate.files_matching
    agent.validate.files_stale
    agent.validate.files_missing
    agent.validate.files_unknown
    agent.validate.files_extra
    agent.validate.files_error

Recon remains the best source for the current installed version and detailed
per-source error text; StatsD is intended for rates, timings, and alerting.
