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

Requests other than GET and HEAD require the administrator key.
GET and HEAD paths ending in ``/builder`` or ``/builder/file`` also require
the administrator key.
OPTIONS and ``/healthcheck`` do not require a key.

Discovery
=========

``GET /``
---------

Returns the service name, Swift version, supported API versions, and a link to
the status resource.

Example response::

    {
      "api_versions": [
        {"id": "v1", "url": "/api/v1/"}
      ],
      "links": {
        "rings": "/api/v1/rings/",
        "status": "/api/v1/ring_manager/status/"
      },
      "service": "ring-manager-server",
      "version": "<swift version>"
    }

Ring resources
==============

Ring JSON stores logical metadata only.
Builder-owned settings and devices are read from and written to the Swift
builder file.
Mutating a ring or its devices does not rebalance or publish it.

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

``GET /api/v1/rings/<ring_id>/``
----------------------------------------

Returns logical metadata plus current builder-owned settings,
``builder_version``, ``device_count``, and ``devices_url``.
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

Returns the service name, Swift version, and basic process status.

Example response::

    {
      "service": "ring-manager-server",
      "status": "ok",
      "version": "<swift version>"
    }

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
