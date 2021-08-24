
Platform API
============

* A set of web APIs that client applications can use to talk to and control the TRAC platform
* Built around the TRAC :doc:`metadata model <./metadata_model>`_ (object definitions and tags)
* Typical use cases include:

  - Bespoke GUI clients for your organisation or workflow (web and/or desktop)
  - System integrations
  - Plugins for cataloguing, reporting and control systems
  - Executing instructions from scheduling and orchestration tools
  - Monitoring targets

There is one public API for each of the services in the TRAC platform, all are available in gRPC, gRPC-Web and
REST/JSON formats. There is also a JavaScript API package which is the recommended option for building web clients.
For a ful listing of the services, methods and data structures in the platform API, refer to the
:doc:`platform API listing <./autoapi/trac/api/index>`.

Before reading about the platform API, make sure you are familiar with the
:doc:`metadata model <./metadata_model>`.

Metadata API
------------

.. autoapiclass:: trac.api.TracMetadataApi
    :noindex:
