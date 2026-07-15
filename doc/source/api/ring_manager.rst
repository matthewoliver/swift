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
        "status": "/api/v1/ring_manager/status/"
      },
      "service": "ring-manager-server",
      "version": "<swift version>"
    }

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
