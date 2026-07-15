==========================
Ring Manager Control Plane
==========================

Ring manager is an optional Swift-style WSGI control-plane service.
It gives ring operations a dedicated process and API boundary without placing
ring management in the Object Storage v1 API or the client data path.

The service framework provides:

* unversioned service discovery at ``/``;
* versioned API discovery at ``/api/v1/``;
* service status at ``/api/v1/ring_manager/status/``;
* ring metadata and builder-backed device management at
  ``/api/v1/rings/``;
* separate read and administrator authentication keys;
* directory-backed JSON state with locked, atomic, durable writes; and
* normal Swift process management through ``swift-init``.

Ring-specific resources are owned by an internal controller.
This keeps HTTP dispatch and process lifecycle in the WSGI application while
allowing each ring capability to add its routes and state together.

Architecture
============

.. image:: images/ring-manager-architecture.svg
   :alt: Ring manager service foundation
   :align: center

An operator or automation client sends a control-plane request to the
ring-manager port.
The authentication middleware classifies the request as a read or an
administrator operation before it reaches the application.
The WSGI application owns discovery, status, method negotiation, error
mapping, and request logging.
Ring-specific routes are delegated through ``RingController.routes()``.
``RingBuilderManager`` performs locked updates to Swift builder files for the
controller.
Durable service state is stored below ``ring_manager_state_dir``.
Builder-owned topology remains in Swift builder files below
``ring_builder_dir``.

The proxy, account, container, and object services continue to use installed
Swift ring files in the normal data path.
Client object requests do not pass through ring manager.

Request and error boundary
==========================

The public API is rooted at ``/api/v1/``.
The unversioned root is discovery only and advertises the supported API
version.
GET routes also accept HEAD, and every matched route accepts OPTIONS and
returns an ``Allow`` header.

Expected request errors use normal HTTP responses.
Unexpected exceptions are logged by the service and returned as a generic
JSON ``500 Internal Server Error`` response.
Tracebacks and internal exception text are not exposed to clients.

Authentication
==============

The ``ring-manager-auth`` Paste filter accepts two credential classes:

* ``admin_key`` authorises every request; and
* ``read_key`` authorises ordinary GET and HEAD requests only, except for
  paths reserved for builder downloads.

Keys may be supplied inline or through ``admin_key_file`` and
``read_key_file``.
The two forms for one credential are mutually exclusive.
Secret files are opened without following symlinks, must be regular files
owned by root or the effective service user, and must not grant group or other
permissions.
Secrets are loaded once when the middleware starts.

If no suitable key is configured, the middleware returns ``503 Service
Unavailable`` rather than allowing the request.
An incorrect key returns ``401 Unauthorized``.
OPTIONS and ``/healthcheck`` bypass authentication so middleware negotiation
and service monitoring remain available.

State storage
=============

``ring_manager_state_dir`` defaults to
``/etc/swift/ring-manager-state``.
State is stored as independent JSON resources rather than one large document.
Identifiers are percent-encoded before they are used as file names.

State mutations use a lock where a shared index is involved.
Each JSON write creates a unique temporary file in the destination directory,
flushes and fsyncs its content, atomically renames it into place, and fsyncs
the directory.
New directory parents and deletes are fsynced as well.
This keeps prior state intact when a write fails before the rename and makes
completed updates durable across a host crash.

Ring resources and builder authority
====================================

A ring resource combines small logical metadata with a live view of its Swift
builder.
Logical metadata such as the ring name, cluster reference, policy type, and
storage policy index is stored as JSON below ``ring_manager_state_dir``.
Devices, weights, regions, zones, ports, partition power, replica count,
``min_part_hours``, overload, and builder version remain authoritative in the
builder file.
They are not copied into the JSON resource.

The API derives standard builder names for account, container, and object
rings, or accepts one explicit ``builder_files`` entry on a ring.
A ring response hydrates builder-owned fields from the current builder file.
This means a builder changed by established Swift tooling is reflected in the
next ring-manager read.

Creating a ring with builder settings creates its builder file under an
exclusive lock.
PATCH may change the replica count, ``min_part_hours``, and overload through
Swift's ``RingBuilder`` methods.
Ordinary PUT or PATCH cannot change partition power on an existing builder.
Device add, replace, and remove requests validate the complete input before
saving a new durable builder file.
These operations change desired builder state only: they do not rebalance the
ring or publish a ``.ring.gz`` file.

Device topology validation accepts IPv4, IPv6, and structured host names.
Regions, zones, device IDs, and ports use guarded ASCII-decimal parsing;
weights and builder numeric settings must be finite and within their allowed
ranges.
Explicit device IDs are bounded because ``RingBuilder.devs`` is a dense list.
Invalid multi-device requests leave the existing builder unchanged.
Builder saves use a unique temporary file, fsync the file, atomically rename
it, preserve an existing file mode, and fsync the parent directory.

Operations
==========

Ring manager is registered as the ``ring-manager-server`` Swift service.
It belongs to both the ``all`` and ``control`` groups and supports graceful
and seamless reloads like other WSGI services.
It is not a background daemon and is excluded from ``swift-init ... once``.

Common commands are::

    swift-init ring-manager-server start
    swift-init ring-manager-server status
    swift-init ring-manager-server reload
    swift-init control status

The SAIO seed is
``doc/saio/swift/ring-manager-server.conf``.
The SAIO reset helper removes the ring-manager state directory so a full reset
does not retain control-plane state from the previous environment.

Configuration and API references
================================

See :doc:`config/ring_server` for the server and authentication options.
See :doc:`api/ring_manager` for ring resources, device operations, and
response conventions.
