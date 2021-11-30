
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
    const metaApiRpcImpl = trac.setup.rpcImplForBrowser(trac.api.TracMetadataApi)

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

Create a definition object

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 27 - 45
    :linenos:
    :lineno-start: 27

Create a MetadataWriteRequest

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 47 - 63
    :linenos:
    :lineno-start: 47

Call the metadata API

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 65 - 72
    :linenos:
    :lineno-start: 16


Loading objects
---------------

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 74 - 83
    :linenos:
    :lineno-start: 74


Putting it all together
-----------------------

.. literalinclude:: ../../../examples/apps/javascript/src/hello_world.js
    :language: JavaScript
    :lines: 85 -
    :linenos:
    :lineno-start: 85
