
##########################
Chapter 5 - Error Handling
##########################

This tutorial is based on the *error_handling.js* example, which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/apps/javascript*.

Error handling basics
---------------------

The TRAC web API bindings support both futures and callback-style API calls. The same
error handling capabilities are available in both styles.

As a basic example, we can send an invalid request to the server and it will respond
with a validation error. Here is what that looks like using futures:

.. literalinclude:: ../../../examples/apps/javascript/src/error_handling.js
    :language: JavaScript
    :lines: 63 - 70
    :linenos:
    :lineno-start: 63

The *error.message* property contains a short human-readable description of the error.
You can also access the gRPC status code using the *error.code* property. Both of these
properties are available even if communication with the server fails, for example in the
case of network errors.

.. note::

    A list of gRPC status codes is available in public documentation for gRPC:
    https://grpc.io/docs/guides/status-codes/

Exactly the same error handling capability is available using callbacks:

.. literalinclude:: ../../../examples/apps/javascript/src/error_handling.js
    :language: JavaScript
    :lines: 79 - 88
    :linenos:
    :lineno-start: 79


TRAC Error Details
------------------

Sometimes it is helpful to get more detailed information about an error. For example,
in the case of validation failures, there can sometimes be multiple issues and we want
to know exactly what those issues are and where in the input they occurred. Fortunately,
TRAC has a means to provide this information.

.. literalinclude:: ../../../examples/apps/javascript/src/error_handling.js
    :language: JavaScript
    :lines: 97 - 104
    :linenos:
    :lineno-start: 97

Use *getErrorDetails()* to get a :class:`TracErrorDetails<tracdap.api.TracErrorDetails>`
object. The *details.message* and *details.code* properties are the same as for basic
error handling and are always available. However, if the server provided more detailed
information, this can be accessed by looking at the individual error items:

.. literalinclude:: ../../../examples/apps/javascript/src/error_handling.js
    :language: JavaScript
    :lines: 105 - 108
    :linenos:
    :lineno-start: 105

For each item, *item.detail* is a human readable description of the error


Errors in streaming calls
-------------------------

TODO

