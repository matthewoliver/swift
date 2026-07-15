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
Durable service state is stored below ``ring_manager_state_dir``.

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
See :doc:`api/ring_manager` for the foundation API resources and response
conventions.
