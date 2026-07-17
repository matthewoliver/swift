================
Ring Manager API
================

The ring-manager API is a Swift control-plane API.
It is not part of the Object Storage v1 API and does not share the proxy data
path.

Versioning and response format
==============================

Versioned resources are rooted at ``/api/v1/``.
The unversioned root provides discovery only.
Successful discovery and status responses use JSON with an
``application/json`` content type.
Unexpected application errors use a JSON object with an ``error`` field.
Routing errors use Swift's normal HTTP error responses.

Authentication
==============

Administrator requests may use either of these headers::

    X-Ring-Manager-Admin-Key: <key>
    X-Auth-Token: <key>

Ordinary GET and HEAD requests may instead use::

    X-Ring-Manager-Read-Key: <key>

Requests other than GET and HEAD require the administrator key, except for
the read-only ``POST .../partitions_at_risk/`` bulk analysis request.
GET and HEAD paths ending in ``/builder`` or ``/builder/file`` also require
the administrator key.
OPTIONS and ``/healthcheck`` do not require a key.

Discovery
=========

``GET /``
---------

Returns the service name, Swift version, supported API versions, service mode,
and a link to the status resource.

Example response::

    {
      "api_versions": [
        {"id": "v1", "url": "/api/v1/"}
      ],
      "links": {
        "desired_ring_version": "/api/v1/rings/releases/desired/",
        "desired_ring_version_manifest": "/api/v1/rings/releases/desired/manifest/",
        "latest_ring_version": "/api/v1/rings/releases/latest/",
        "ring_builds": "/api/v1/rings/builds/",
        "ring_versions": "/api/v1/rings/releases/",
        "rings": "/api/v1/rings/",
        "status": "/api/v1/ring_manager/status/",
        "sync_trigger": "/api/v1/ring_manager/sync/trigger/"
      },
      "mode": "primary",
      "service": "ring-manager-server",
      "version": "<swift version>",
      "writable": true
    }

Service modes
=============

``primary`` accepts every supported request method.
``readonly`` and ``standby`` accept each route's read-only methods and reject
other supported methods with ``403 Forbidden``.
This permits replicas to serve discovery, status, published releases, and
artifact downloads while retaining one authoritative writer.
The bulk ``POST .../partitions_at_risk/`` analysis route is explicitly
read-only and therefore remains available in these modes.
The ``POST /api/v1/ring_manager/sync/trigger/`` route is also explicitly
read-only: it can only request a configured local pull from a replica and
cannot change published state or promote the server.

Ring resources
==============

Ring JSON stores logical metadata only.
Builder-owned settings and devices are read from and written to the Swift
builder file.
Mutating a ring or its devices does not rebalance or publish it.
Set ``disabled: true`` on a logical ring to keep it editable while excluding it
from cluster release manifests.
Partition power on an existing object builder changes only through the
explicit lifecycle actions described below.

``GET /api/v1/rings/schema/``
------------------------------------

Returns the available ring fields and the HTTP methods supported by the list
and detail resources.
Computed identity and builder-summary fields such as ``id``,
``resource_uri``, ``builder_version``, ``device_count``, and ``devices_url``
are marked read-only in the schema.

``GET /api/v1/rings/``
------------------------------

Lists ring metadata in the standard collection envelope.
Use the optional ``cluster_id`` query parameter to select one cluster
reference.
Builder-owned settings are not loaded for list responses.

``POST /api/v1/rings/``
-------------------------------

Creates logical ring metadata and, when builder settings are supplied, its
Swift builder.
The normal object-ring identifier is derived from
``storage_policy_index``.
Account and container identifiers are derived from ``ring_type``.

A builder-creating request supplies ``part_power``, ``num_replicas``, and
``min_part_hours`` together.
``overload`` is optional.
For example::

    {
      "name": "Policy 1",
      "ring_type": "object",
      "storage_policy_index": 1,
      "policy_type": "replication",
      "part_power": 10,
      "num_replicas": 3,
      "min_part_hours": 1,
      "overload": 0.0
    }

The response has status ``201`` and includes the settings loaded back from
the builder.
A duplicate identifier returns ``409 Conflict``.
If builder validation or saving fails, the new JSON resource is removed.

Immutable release downloads
===========================

Release manifests are read from ``ring_manager_state_dir/releases``.
They describe immutable ring files below ``ring_artifact_dir`` without exposing local paths.
Publishing creates a durable job; ``swift-ring-manager-builder`` performs the
rebalance and records the immutable complete enabled-ring snapshot.

Persistent build jobs
=====================

``GET /api/v1/rings/builds/``
--------------------------------

Lists persistent build jobs in monotonic ``sequence`` order.
Jobs expose their state, request, attempt count, and result or failure detail.
The optional ``retry_of`` query parameter selects direct retries of one build.
The optional ``retry_root`` parameter selects every descendant retry in one
retry chain.
Both filters retain terminal history so an operator can inspect every earlier
attempt before retrying again.

``GET /api/v1/rings/builds/<build_id>/``
------------------------------------------

Returns one queued, building, deferred, completed, failed, or cancelled job.

Workers claim jobs under a durable lease.
An expired claim is recovered into FIFO order, while a worker with an old
claim cannot update or refresh a newer claim.
A deferred job blocks later overlapping scopes but permits disjoint explicit
ring builds to proceed.

``POST /api/v1/rings/builds/<build_id>/cancel/``
----------------------------------------------------

Cancels a queued or deferred job and records its terminal ``cancelled`` state.
An optional JSON ``reason`` becomes an operator-visible cancellation reason.
Repeating cancellation returns the existing cancelled record without changing
its original reason or timestamp.
Building, completed, and failed jobs return ``409 Conflict``.
The operation never interrupts an active ``RingBuilder.rebalance`` or
publication attempt.

``POST /api/v1/rings/builds/<build_id>/retry/``
---------------------------------------------------

Retries a failed or cancelled job by creating a fresh queued job at the end of
the FIFO sequence.
The original job remains immutable terminal history.
The new job records ``retry_of``, ``retry_root``, ``retry_count``, source state,
and the optional JSON retry ``reason``.
Retry validates the stored request through the normal enqueue path, including
the release-version admission check.
Queued, deferred, building, and completed jobs return ``409 Conflict``.

``GET /api/v1/rings/releases/``
--------------------------------

Lists known releases in the standard collection envelope.
The optional ``cluster_id`` query parameter filters the collection.
File entries include a download URL, byte count, MD5 digest when present, and SHA-256 integrity digest when present.
``latest`` identifies the most recently published release, while ``desired``
identifies the known release selected as the installation target.
Both booleans are calculated from mutable index pointers and are not written
into immutable release manifests.

``POST /api/v1/rings/releases/``
---------------------------------

Creates a persistent job for the selected ``rings`` and returns
``202 Accepted`` with a ``Location`` header for that job.
The worker builds one immutable release manifest when it claims the job.
When ``rings`` is omitted, all enabled rings are rebuilt.
When it is supplied, unchanged enabled rings are carried forward from their
latest per-ring artifact versions so the manifest remains complete.
Disabled rings are omitted and cannot be selected for a release build.
Every selected ring and every carry-forward artifact is validated before the
first builder is modified.
The worker acquires every selected builder lock in deterministic path order and
holds them through format preflight, rebalance, and artifact writes.
``format_version`` is optional: ring-manager writes v1 when device IDs fit the
legacy 2-byte format and automatically writes v2 when a builder needs wider
device IDs.
An explicit ``format_version: 1`` fails for a builder that needs wider IDs;
use ``format_version: 2`` to force v2 output.
Reusing a published release version returns ``400 Bad Request``.
An active job for the same explicit release version returns ``409 Conflict``.
Normal publish requests update both ``latest`` and ``desired``.
Set ``set_desired`` to false to stage a known release without selecting it.
The persistent job records the current desired value as ``expected_desired``;
the worker changes the pointer only if that value still matches.
This prevents a stale queued job from overwriting a newer operator selection.

``GET /api/v1/rings/releases/<version>/``
------------------------------------------------

Returns one immutable release record.
Local ``path`` and ``artifact_dir`` values are removed from the response.

``GET /api/v1/rings/releases/<version>/manifest/``
---------------------------------------------------------

Returns the public manifest for one release.
The response includes release metadata and public file entries without local paths.

``GET /api/v1/rings/releases/latest/``
---------------------------------------

Returns the release selected by ``latest_ring_version`` in the state index, or by a release marked ``latest`` when no index value exists.
The corresponding ``.../latest/manifest/`` resource returns its public manifest.

``GET /api/v1/rings/releases/desired/``
-------------------------------------------------

Returns the known release selected for installation.
The response includes ``desired_updated_at`` and ``desired_reason`` when they
were recorded with the pointer.
It returns ``404 Not Found`` until an operator selects a desired release.

``PUT /api/v1/rings/releases/desired/``
-------------------------------------------------

Selects an existing known release as desired.
The request must include ``expected_desired`` so concurrent operators and
queued release builds cannot silently overwrite newer intent.
Use JSON ``null`` to assert that no desired release is currently selected.

.. code-block:: json

    {
      "version": "release-42",
      "expected_desired": "release-41",
      "reason": "validated canary rollout"
    }

A matching selection is idempotent.
An unknown release returns ``404 Not Found`` and an outdated expectation
returns ``409 Conflict`` without changing the pointer.
The selector is writable, so readonly and standby servers reject it.

``GET /api/v1/rings/releases/desired/manifest/`` returns the selected public
manifest with ``desired: true``.
It also returns ``404 Not Found`` until a desired release exists.

The storage-node agent and replica sync continue using ``latest`` in this
slice.
Desired-aware agent consumption and pointer replication are separate work.

``GET /api/v1/rings/releases/<version>/files/<file_name>``
-----------------------------------------------------------------

Streams one immutable artefact from disk.
The server resolves the manifest path below ``ring_artifact_dir`` and rejects lexical or symbolic-link escapes with ``404 Not Found``.
The response uses the file's MD5 digest as the HTTP ETag and exposes its SHA-256 digest in ``X-Checksum-Sha256``.
HEAD, ``If-None-Match``, and single or multiple byte ranges use Swift's normal conditional response handling.
The ``.../latest/files/<file_name>`` selector redirects to the concrete immutable version URL.
The corresponding ``.../desired/files/<file_name>`` selector redirects to the
selected immutable version, or returns ``404 Not Found`` when none is set.

Replica pull workflow
=====================

swift-ring-manager-sync pulls the primary's
/api/v1/rings/releases/latest/manifest/ document, each immutable release
artefact, ring metadata, and the referenced per-ring version records.
It validates the supplied byte counts and SHA-256 digests.
The command records local paths rather than source URLs, writes the release
manifest, and advances the mutable latest_ring_version pointer only after the
pull completes.
Desired-pointer replication follows separately, so this sync workflow still
tracks ``latest`` only.
Existing valid artefacts use If-None-Match and may receive 304 Not Modified.

One or more source URLs may be supplied directly or through the dedicated
``[ring-manager-sync]`` config section.
The syncer accepts a primary directly and uses a read-only or standby source
only when its status advertises fresh synchronized published state.
Remote source failures fall through in order, while local write failures stop
the attempt rather than mixing local state from sources.
By default the pull contains only published state and immutable artefacts.
When ``sync_builder_files`` is enabled, it also downloads enabled-ring builder
files through the admin-only endpoints using an administrator credential.
It verifies byte count, SHA-256, and Swift builder loadability before writing
local ring metadata or advancing the latest pointer.
Synced builder files are committed together with mutable state JSON using
rollback backups; if the local commit fails, the previous builder files and
state JSON are restored and ``latest`` is not advanced.
Disabled-ring builders are skipped by default.
Its latest success or error is available from /recon/ring_manager when the
recon middleware is configured.
The recon result includes a ``sync_transaction`` summary, with recovery
details when the utility observes or recovers a pending journal.
It also includes ``operator_attention`` on success and failure.
This is ``needed=true`` for failed sync attempts, fallback source errors, and
failed transaction recovery, with matching ``sync.operator_attention`` and
``sync.operator_attention.<reason>`` StatsD counters.
State and recon timestamp fields use Swift ``NormalTimestamp.internal``
strings, such as ``1700000000.00000``; elapsed sync time remains numeric
seconds.
Use ``--state-change-hook`` to run the same best-effort local state-history
command used by the server and builder daemon after synchronized JSON writes.

Per-ring artifact builds
========================

``GET /api/v1/rings/<ring_id>/versions/``
------------------------------------------

Lists immutable artifact versions built for one logical ring.

``POST /api/v1/rings/<ring_id>/versions/``
-------------------------------------------

Creates a persistent artifact-only job and returns ``202 Accepted`` with its
status URL.
The worker builds one ring artifact without creating a cluster release manifest
or changing the top-level ``latest`` release pointer.
This is useful for validating a builder and is also allowed for disabled
rings.
The resulting artifact version is the Swift builder version, not a caller
supplied release version.
``format_version`` is optional: ring-manager writes v1 when device IDs fit the
legacy 2-byte format and automatically writes v2 when a builder needs wider
device IDs.
An explicit ``format_version: 1`` fails for a builder that needs wider IDs.

``GET /api/v1/rings/<ring_id>/versions/<version>/``
-----------------------------------------------------

Returns one per-ring artifact record.
The matching ``files/<file_name>`` resource streams its immutable artifact.
The ``latest`` selectors resolve to a concrete artifact version before serving
the file.

Builder downloads
=================

``GET /api/v1/rings/<ring_id>/builder/``
------------------------------------------------

Returns builder version, partition-power lifecycle state, and download metadata for the ring's current Swift builder.
The response includes byte count, MD5, SHA-256, and the public file URL without exposing the local builder path.

``GET /api/v1/rings/<ring_id>/builder/file/``
-----------------------------------------------------

Streams a private snapshot of the current builder so a concurrent builder replacement cannot change a response in progress.
The response supports HEAD, conditional requests, and byte ranges, and includes ``X-Ring-Builder-Version``.
Both builder resources require the administrator key.
Builder paths are resolved below ``ring_builder_dir`` and path or symbolic-link escapes are rejected before the file is loaded.

``GET /api/v1/rings/<ring_id>/``
----------------------------------------

Returns logical metadata plus current builder-owned settings,
``builder_version``, ``device_count``, and ``devices_url``.
Object-ring responses also report ``next_part_power``,
``partition_power_increase_state``, and
``allowed_partition_power_actions`` from the current builder.
A missing builder is valid for metadata-only rings, so builder fields are
omitted until a builder is created.

``PUT or PATCH /api/v1/rings/<ring_id>/``
-------------------------------------------------

PUT replaces logical metadata and PATCH updates selected fields.
Both methods may set ``num_replicas``, ``min_part_hours``, and ``overload``
on the builder.
Numeric strings are accepted when they represent valid values.
Partition power cannot be changed on an existing builder through these
ordinary update methods.
Invalid builder settings return ``400 Bad Request`` without replacing the
builder.

``DELETE /api/v1/rings/<ring_id>/``
-------------------------------------------

Deletes the logical ring resource and returns ``204 No Content``.
This operation does not delete, rebalance, or publish the builder file.

Partition power increase
========================

``POST /api/v1/rings/<ring_id>/partition_power_increase/prepare/``
------------------------------------------------------------------

Prepares an object ring for a one-step partition power increase under the
builder-file lock.
The ring must have no active build job whose scope overlaps it.

``POST /api/v1/rings/<ring_id>/partition_power_increase/increase/``
-------------------------------------------------------------------

Applies a prepared increase and moves the builder to
``cleanup_pending``.
The ring must have no active overlapping build job when this forward
transition starts.

``POST /api/v1/rings/<ring_id>/partition_power_increase/cancel/``
-----------------------------------------------------------------

Cancels a prepared increase and moves the builder to
``cleanup_pending`` so object-server cleanup can finish.
This recovery action remains available when an active build record exists.

``POST /api/v1/rings/<ring_id>/partition_power_increase/finish/``
-----------------------------------------------------------------

Finishes an increase or cancellation after cleanup and clears
``next_part_power``.
This recovery action remains available when an active build record exists.

Each successful action returns the hydrated ring resource with ``action`` and
``requires_publish: true``.
The lifecycle states are ``idle``, ``prepared``, ``cleanup_pending``, and
``invalid``.
The ``allowed_partition_power_actions`` field lists the valid next actions.
These lifecycle fields are read-only and cannot be changed with ordinary ring
POST, PUT, or PATCH requests.
Invalid transitions return ``409 Conflict``.
Non-object rings return ``400 Bad Request``.
Active overlapping builds return ``409 Conflict`` for ``prepare`` and
``increase``.

.. warning::

    These actions mutate the builder only.
    They do not rebalance or publish a ``.ring.gz`` file, restart object
    services, run ``swift-object-relinker``, wait for ring distribution, or
    enforce the object-server ``reclaim_age`` window.
    Follow :ref:`modify_part_power` on every object node before applying the
    next lifecycle action.
    Only the prepared state can be cancelled; a published increase cannot be
    reverted.

Read-only ring analysis
=======================

The analysis resources load Swift builders without saving, rebalancing, or
publishing them.

``GET /api/v1/rings/<ring_id>/parts/``
----------------------------------------------

Returns the total assigned replica parts and a count grouped by device name.

``GET /api/v1/rings/<ring_id>/rebalance/``
--------------------------------------------------

Returns balance, dispersion, whether rebalance is requested by metadata, and
the elapsed and minimum remaining rebalance times.
This is a status calculation and does not run a rebalance.

``GET /api/v1/rings/<ring_id>/dispersion/``
---------------------------------------------------

Returns the builder dispersion graph grouped at ``region``, ``zone``, ``ip``,
or ``device`` level.
The ``level`` query parameter defaults to ``zone``.

``GET /api/v1/rings/<ring_id>/at_risk/``
-----------------------------------------------

Returns per-replication-IP totals for assigned, dispersed, and at-risk parts.

``GET /api/v1/rings/<ring_id>/count_parts/``
--------------------------------------------------

Counts partitions with at least ``risk_count`` replicas on the selected
``replication_ip`` values.
Repeat the query parameter or supply a comma-separated value.
The effective risk count is at least two.
Unplaced and removed device assignments are ignored.

``GET or POST /api/v1/rings/<ring_id>/partitions_at_risk/``
-----------------------------------------------------------------

Analyses the impact of taking selected nodes or devices down.
GET accepts repeated or comma-separated ``node_ip``, ``ip``,
``replication_ip``, and ``device_id`` query parameters.
POST accepts the scalar or plural forms in a JSON object, for example::

    {
      "node_ips": ["10.0.0.10", "10.0.0.11"],
      "replication_ips": ["10.1.0.10"],
      "device_ids": [70000],
      "risk_count": 2,
      "details": true
    }

At least one down selector is required.
``risk_count`` must be a non-negative integer and is normalised to at least
two.
``details`` defaults to false; when true, each builder summary includes the
partition numbers grouped by down-replica count.
The response identifies matched devices by builder so account and container
builders cannot collide on device ID.
It also reports affected partitions, partitions meeting the risk threshold,
and the maximum replicas lost from one partition.

``max_partitions_at_risk_selectors`` limits the combined selector count before
any builder is loaded.
``max_json_request_body_size`` bounds POST bodies before JSON decoding.
A missing required builder returns ``409 Conflict`` and names only the missing
builder file, not its local directory.
Invalid selectors or request values return ``400 Bad Request``.
The POST form is read-only and may use the read key.

Device resources
================

``GET /api/v1/rings/<ring_id>/devices/``
------------------------------------------------

Lists active devices from the builder in device-ID order.
``limit`` bounds the returned page and ``marker`` continues after one device
ID.
The response includes ``next_marker`` through the collection ``meta.next``
link when another page exists.
Use ``include_removed=true`` to include devices pending removal.

``PUT /api/v1/rings/<ring_id>/devices/``
------------------------------------------------

Replaces the desired active device set.
The complete request is normalised and validated before the builder is
mutated, so one invalid entry rejects the entire operation.
Devices omitted from the request are marked for removal by ``RingBuilder``.

``POST /api/v1/rings/<ring_id>/devices/add/``
-----------------------------------------------------

Adds validated devices that are not already active.
A direct payload uses a ``devices`` list::

    {
      "devices": [{
        "region": 1,
        "zone": 2,
        "ip": "node2.example.com",
        "port": 6000,
        "replication_ip": "2001:db8::2",
        "replication_port": 6003,
        "device": "sdb",
        "weight": 100,
        "label": "node2:sdb"
      }]
    }

The ``nodes`` form may place common region, zone, address, port, and weight
values on a node and list its devices by name.

``POST /api/v1/rings/<ring_id>/devices/remove/``
--------------------------------------------------------

Marks matching active devices for removal.
Selectors may use ``id``, ``label``, or an ``ip``/``port``/``device`` tuple.
The response reports devices with ``pending_removal: true``.

Validation and durability
=========================

Device requests require non-negative integer regions and zones, TCP ports
from 1 through 65535, valid Swift device names, valid addresses, and finite
non-negative weights.
Replication address and port default to the primary values when omitted.
Explicit IDs must not exceed ``max_explicit_device_id`` and must not collide
with another desired or existing device.

Each builder mutation holds the builder-specific lock for at most
``builder_lock_timeout`` seconds.
Builder files are replaced atomically and durably after validation succeeds.
The APIs only change desired builder state; an operator must run a later
rebalance and publication workflow before storage nodes receive a new ring.

``GET /api/v1/rings/membership/device/<device_id>/``
-----------------------------------------------------------

This reserved discovery route currently returns ``501 Not Implemented``.

``GET /api/v1/``
----------------

Returns the same discovery document with ``api_version`` set to ``v1``.

Service status
==============

``GET /api/v1/ring_manager/status/``
------------------------------------

Returns the service mode, latest published release, desired installation
target, executor mode, build queue summary, and replica synchronization
freshness information.

Example response::

    {
      "service": "ring-manager-server",
      "mode": "readonly",
      "status": "ok",
      "version": "<swift version>",
      "writable": false,
      "latest_ring_version": "release-42",
      "desired_ring_version": "release-41",
      "ring_manager_sync": {
        "applicable": true,
        "source": "https://primary.example.com:6205",
        "latest_ring_version": "release-42",
        "desired_ring_version": "release-41",
        "last_synced_at": "1700000000.00000",
        "age_seconds": 12.4,
        "freshness_threshold": 300.0,
        "latest_matches_local": true,
        "desired_matches_local": true,
        "synced": true,
        "fresh": true,
        "stale": false,
        "can_serve_published_reads": true,
        "published_state_promote_ready": false,
        "sync_transaction_pending": false,
        "sync_transaction": {
          "pending": false
        },
        "promotion_blockers": ["mode_not_standby"],
        "reasons": []
      },
      "sync_trigger": {
        "configured": true,
        "active": false,
        "pending": false
      },
      "operator_attention": {
        "needed": false,
        "reasons": [],
        "sync": {
          "needed": false,
          "reasons": []
        },
        "promotion": {
          "needed": false,
          "blockers": []
        }
      }
    }

For ``primary`` servers, ``ring_manager_sync.applicable`` is ``false`` because
replica freshness does not apply.
For ``readonly`` and ``standby`` servers, the object summarizes the most
recent successful ``swift-ring-manager-sync`` recorded in local state.
Missing, malformed, stale, future-dated, or version-mismatched metadata fails
closed for freshness and promotion reporting with ``fresh: false`` and
``stale: true``. A pending ``swift-ring-manager-sync`` transaction journal
also fails closed with the ``sync_transaction_pending`` reason and promotion
blocker until the next sync run recovers or cleans up the journal. The same
state is also exposed as the low-cardinality
``ring_manager_sync.sync_transaction.pending`` status field.
The sync record includes both the latest publication pointer and the desired
installation pointer, and they must each match local state before a replica is
considered fresh enough to serve as a published-state source.

Stale sync metadata does not disable the read API.
A read-only or standby server continues serving its last successfully
synchronized release and immutable artifacts from local state.
Staleness only means that replica is no longer current enough for the status
endpoint to recommend it for published-state reads or promotion consideration.

``can_serve_published_reads`` indicates fresh published-state metadata only.
It does not re-open every artifact on each status request and it is not an
availability gate for artifact ``GET`` requests.
``published_state_promote_ready`` is only a narrow standby precheck.
``GET /api/v1/ring_manager/status/?promotion=true`` adds a
``promotion_readiness`` object for a standby failover runbook.
It combines the published-state precheck with local builder-file readiness for
each enabled ring.
The object reports the checked and missing builder counts, invalid or version
mismatched builders, and the aggregate blocker codes without exposing local
builder paths.
Disabled rings are normally skipped, but a disabled ring still referenced by
the latest release manifest is a fail-closed blocker.
A builder newer than its published ring artifact is also a blocker until the
operator publishes its changes or restores the builder to the published state.
When the query is present, ``operator_attention.promotion.blockers`` reflects
the complete promotion precheck rather than only the published-state result.
``swift-ring-manager status --promotion`` requests this view, prints a concise
summary unless ``--json`` is used, and exits non-zero unless the standby is
ready.
Neither response fences the old primary, redirects writers, or performs
automatic failover.

``sync_trigger`` reports whether a local sync-trigger wrapper is configured,
currently running, or waiting as the one permitted follow-up run.
It contains no command, source URL, credential, or local path details.

``operator_attention`` summarizes conditions that should page or stop an HA
runbook without requiring it to interpret every status field.
It is ``needed=true`` when synchronized state is not fresh enough to prefer
for current reads, when a sync transaction is pending recovery, or when the
standby's published-state promotion precheck fails.
The server emits ``operator_attention``, ``operator_attention.sync``,
``operator_attention.promotion``, and ``operator_attention.<reason>`` StatsD
counters when this state is present.

Standby sync trigger
====================

``POST /api/v1/ring_manager/sync/trigger/``
------------------------------------------------

Queues a configured local pull on a ``readonly`` or ``standby`` server.
The request is accepted only when ``ring_manager_sync_trigger_command`` is
configured; a primary server or a replica without that command receives
``409 Conflict``.
The route is subject to the normal ring-manager authentication policy even
though it is available in read-only modes.

The optional JSON body, or equivalent query parameters, may specify a
``reason`` and ``expected_latest``.
The server passes these as
``RING_MANAGER_SYNC_TRIGGER_REASON`` and
``RING_MANAGER_SYNC_TRIGGER_EXPECTED_LATEST`` to the wrapper, with
``RING_MANAGER_SYNC_TRIGGERED_AT`` set to a Swift normal timestamp.
The command runs without a shell in ``ring_manager_state_dir``.

The response is ``202 Accepted`` and reports whether a run was newly
``queued`` or an existing queued follow-up was retained.
The bounded queue permits one follow-up while a command is active, so repeated
notifications do not start overlapping sync processes.
The endpoint returns before the wrapper starts and does not transfer bytes,
advance local release pointers, or perform standby promotion.
The wrapper must perform the pull and any operator-specific retry policy.

Method negotiation
==================

GET resources also accept HEAD.
HEAD returns the same response headers without a response body.
OPTIONS returns ``200`` with an ``Allow`` header.
An unsupported method on a matched resource returns ``405 Method Not
Allowed`` and the same ``Allow`` header.
An unknown path returns ``404 Not Found``.

Unexpected failures return::

    {"error": "Internal server error"}

The detailed exception is written to the service log and is not included in
the response.
