
######################
Lesson 1 - Hello World
######################

This lesson is based on the *hello_world.js* example, which can be found in the
`TRAC GitHub Repository <https://github.com/Accenture/trac>`_
under *examples/apps/javascript*.


Installing the API
------------------

The easiest way to build web client applications for TRAC is using the web API package,
It is available to install with NPM::

    npm install --save trac-web-api

Web apps built with this package can run in a browser using gRPC-Web to connect directly
to the TRAC platform. There is no need to install anything else, or to deploy intermediate
servers such as Node.js or Envoy proxy (although these configurations are supported if required).


Setting up a dev environment
----------------------------

The easiest way to get a local development instance of TRAC is to clone the
`TRAC GitHub Repository <https://github.com/Accenture/trac>`_
and follow the instructions in the main
`README <https://github.com/Accenture/trac#readme>`_
file.

To make a browser-based app that talks to TRAC, the platform and the app should be served under the same origin.
In production this can be handled by proxy servers that take care of routing and other network concerns.
For development, the TRAC Gateway provides the capabilities needed to test against a local TRAC instance.

A sample TRAC Gateway config is provided in the TRAC repository under etc/ and includes an
example of a route to proxy a client app. Change the target to point at your normal dev server
(e.g. WebPack dev server or an embedded server in your IDE). The path under the "match" section
is where your app will appear under gateway.

.. code-block:: yaml
    :caption: etc/trac-gateway-devlocal.yaml

      - routeName: Local App
        routeType: HTTP

        match:
          host: localhost
          path: /local/app

        target:
          scheme: http
          host: localhost
          port: 9090
          path: /

In this example, if the gateway is running on port 8080 over http, you would be able to access your app at
*http://localhost:8080/local/app*.


Connecting to TRAC
------------------

Start by importing the TRAC API package:

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 17 - 17
    :linenos:
    :lineno-start: 17

We need two things to create the TRAC connection, an RPC connector and an instance of the
TRAC API class. You can use trac.setup to create an RPC connector that works in the browser.

.. code-block:: javascript
    :linenos:
    :lineno-start: 19

    // Use trac.setup to create an RPC connector pointed at your TRAC server
    // The browser RPC connector will send all requests to the page origin server
    const metaApiRpcImpl = trac.setup.rpcImplForBrowser(trac.api.TracMetadataApi);

This assumes you have set up routing through the TRAC gateway as described in the previous section.

One you have an RPC connector, you can use it to create the a TRAC API object.
Note that each API class needs its own RPC connector, the RPCs are specific to the APIs they serve.
In this example we only need the metadata API.

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 23 - 24
    :linenos:
    :lineno-start: 23


Running outside a browser
"""""""""""""""""""""""""

The web API can also be used to build apps that run on a Node.js server or as standalone JavaScript applications.
In this case, you'll need to create an RPC connector pointed at the address of your TRAC instance:

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 19, 21

You'll also need to supply the global XMLHttpRequest object, which is not normally available outside a
browser environment. The example code sets this up in the run_examples.js host script, using the 'xhr2'
package available on NPM:

.. literalinclude:: ../../../examples/apps/javascript/run_examples.js
    :language: JavaScript
    :lines: 18 - 20


Creating and saving objects
---------------------------

Suppose we want to create a schema, that we can use to describe some customer account data.
(It is not always necessary to create schemas in this way, but we'll do it for an example).

First we need to build the :class:`SchemaDefinition<trac.metadata.SchemaDefinition>` object.
In real-world applications schemas would be created by automated tools (for example TRAC
generates schemas during data import jobs), but for this example we can define a simple one
in code.

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 27 - 57
    :linenos:
    :lineno-start: 27

The web API package provides structured classes for every type and enum in the ``trac.metadata``
package. A ``.create()`` method is available for every type, which provides auto-complete and
type hints in IDEs that support it. Enums are set using the constants defined in the API package.
All enum types are available in the trac namespace, so ``trac.SchemaType`` is shorthand for
``trac.metadata.SchemaType`` and so on. The basic types in the TRAC type system are also
available, so ``trac.STRING`` is a shorthand for ``trac.metadata.BasicType.STRING``.

Now we want to save the schema into the TRAC metadata store.
To do that, we use a :class:`MetadataWriteRequest<trac.api.MetadataWriteRequest>`.
Request objects from the ``trac.api`` package can be created the same way metadata objects from
``trac.metadata``.

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 59 - 75
    :linenos:
    :lineno-start: 59

There are several things to call out here. TRAC is a multi-tenant platform and every API
request includes a tenant code. By default resources are separated between tenants, so
tenant A cannot access a resource created in tenant B. For this example we have a single
tenant called "ACME_CORP".

Objects are the basic resources held in the platform, each object is described by an
:class:`ObjectDefinition<trac.metadata.ObjectDefinition>` which can hold one of a number
of types of object. Here we are creating a SCHEMA object, we build the object definition
using the schema created earlier.

We also want to tag our new schema with some informational attributes. These attributes
describe the schema object and will allow us to find it later using metadata searches.
Tags can be applied using :class:`TagUpdate<trac.metadata.TagUpdate>` instructions when
objects are created. Here we are applying three tags to the new object, two are categorical
tags and one is descriptive.

The last step is to call :meth:`createObject()<trac.api.TracMetadataApi.createObject>`,
to send our request to the TRAC metadata service.

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 77 - 84
    :linenos:
    :lineno-start: 77

The :meth:`createObject()<trac.api.TracMetadataApi.createObject>` method returns the ID of
the newly created schema as a :class:`TagHeader<trac.metadata.TagHeader>`, which includes the
object type, ID, version and timestamps.

All the API methods in the web API package are available in both future and callback form.
This example uses the future form, which allows chaining of ``.then()``, ``.catch()`` and
``.finally()`` blocks. The equivalent call using a callback would be:

.. code-block:: JavaScript

    metaApi.createObject(request, (err, header) => {

        // Handle error or response
    });


Loading objects
---------------

Now the schema has been saved into TRAC, at some point we will want to retrieve it.
We can do this using a :class:`MetadataReadRequest<trac.api.MetadataReadRequest>`.

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 86 - 92
    :linenos:
    :lineno-start: 86

All that is needed is the tenant code and a :class:`TagSelector<trac.metadata.TagSelector>`.
Tag selectors allow different versions of an object or tag to be selected according to various
criteria. In this example we already have the tag header, which tells us the exact version that
should be loaded. The web API package allows headers to be used as selectors, doing this will
create a selector for the version identified in the header.

Finally we use :meth:`readObject()<trac.api.TracMetadataApi.readObject>` to send the read request.

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 94
    :linenos:
    :lineno-start: 94

The :meth:`readObject()<trac.api.TracMetadataApi.readObject>`
method returns a :class:`Tag<trac.metadata.Tag>`,
which includes the :class:`TagHeader<trac.metadata.TagHeader>`
and all the tag attributes, as well as the
:class:`ObjectDefinition<trac.metadata.ObjectDefinition>`.
Since we used the future form of the call, it will return a promise for the tag.


Putting it all together
-----------------------

In a real-world situation these calls would be built into the framework of an application.
The example scripts all include ``main()`` functions so they can be tried out easily from
a console or IDE. In this example we just create the schema and then load it back from TRAC.

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 96 -
    :linenos:
    :lineno-start: 96

The last call to ``JSON.stringify()`` will provide a human-readable representation of the
TRAC object, that can be useful for debugging.
