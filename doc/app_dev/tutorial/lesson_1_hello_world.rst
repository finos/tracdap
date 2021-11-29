
######################
Lesson 1 - Hello World
######################


Installing the API
------------------

The easiest way to build web client applications for TRAC is using the web API package.
It is available to install with NPM::

    npm install --save trac-web-api

The API package takes care of all the dependencies for working with gRPC and gRPC-Web,
so there is no need to install anything else.


Connecting to TRAC
------------------

.. literalinclude:: ../../../examples/client_api/javascript/hello_world/hello_world.js
    :language: JavaScript
    :caption: hello_world.js
    :name: hello_world.js
    :lines: 17
    :linenos:
    :lineno-start: 1

Using from the command line

.. literalinclude:: ../../../examples/client_api/javascript/hello_world/hello_world.js
    :language: JavaScript
    :lines: 19 - 21
    :linenos:
    :lineno-start: 3

Setting up the API

.. literalinclude:: ../../../examples/client_api/javascript/hello_world/hello_world.js
    :language: JavaScript
    :lines: 24 - 29
    :linenos:
    :lineno-start: 8


Creating and saving objects
---------------------------

.. literalinclude:: ../../../examples/client_api/javascript/hello_world/hello_world.js
    :language: JavaScript
    :lines: 32 - 76
    :linenos:
    :lineno-start: 16


Loading an object
-----------------

.. literalinclude:: ../../../examples/client_api/javascript/hello_world/hello_world.js
    :language: JavaScript
    :lines: 78 - 87
    :linenos:
    :lineno-start: 106


Searching the metadata store
----------------------------

.. literalinclude:: ../../../examples/client_api/javascript/hello_world/hello_world.js
    :language: JavaScript
    :lines: 89 - 131
    :linenos:
    :lineno-start: 62


Putting it all together
-----------------------

.. literalinclude:: ../../../examples/client_api/javascript/hello_world/hello_world.js
    :language: JavaScript
    :lines: 133 -
    :linenos:
    :lineno-start: 117