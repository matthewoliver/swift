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
    pipeline = catch_errors healthcheck ring-manager-auth ring-manager-server

    [app:ring-manager-server]
    use = egg:swift#ring_manager

    [filter:ring-manager-auth]
    use = egg:swift#ring_manager_auth

    [filter:healthcheck]
    use = egg:swift#healthcheck

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

``ring_manager_state_dir``
    Root of the directory-backed JSON state.
    The default is ``/etc/swift/ring-manager-state``.

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

``max_json_request_body_size``
    Maximum JSON request size in bytes for ring mutations.
    The default is ``1048576``.

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
