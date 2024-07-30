
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

Use ``getErrorDetails()`` to get a :class:`TracErrorDetails<tracdap.api.TracErrorDetails>`
object. The *details.message* and *details.code* properties are the same as for basic
error handling and are always available. However, if the server provided more detailed
information, this can be accessed by looking at the individual error items:

.. literalinclude:: ../../../examples/apps/javascript/src/error_handling.js
    :language: JavaScript
    :lines: 105 - 108
    :linenos:
    :lineno-start: 105

For each item, *item.detail* is a human readable description of the error. For errors related
to the request (mostly validation errors), *item.location* is the field path for the input field
that caused the problem.


Errors in streaming calls
-------------------------

.. seealso::
    See :doc:`./chapter_4_streaming` for more details on the streaming data API.

**Errors in upload streams**

With upload streams, the client sends a series of messages to the server and receives a single message
or error in response. The error handler can be attached when you send the first message to the stream:

.. code-block:: javascript

    stream.createDataset(request0)
        .then(_ => {...})  // Handle upload success
        .catch(error => {
            // Handle upload error
            const details = tracdap.utils.getErrorDetails(error);
            ...
        });

    // Now send the rest of the messages to the stream
    stream.createDataset(msg1);
    stream.createDataset(msg2);
    ...

Similarly if you are using callback style, a callback is only needed for the first message of the stream.

**Errors in download streams**

For download streams, the client sends a single request to the server and receives a stream of messages in
response. An error can occur at any point in the stream and has to be handled using a stream event handler.

Because errors are processed using stream events, the futures / callback handlers should be explicitly set
to no-op functions to avoid unhandled or duplicate events.

.. code-block:: javascript

    // Handle the download stream
    stream.on("data", msg => {...});
    stream.on("end", () => {...});

    stream.on("error", error => {
        // Handle download error
        const details = tracdap.utils.getErrorDetails(error);
        ...
    });

    // Disable future / callback processing, because we are using stream events
    stream.readDataset(request)
        .then(_ => {})
        .catch(_ => {});

**Using promises for streaming operations**

In :doc:`./chapter_4_streaming`, the streaming operations are wrapped up into promises and errors are
passed directly to the promise *resolve()* method. Once the operation is wrapped up into a promise,
errors can be processed using a regular ``.catch()``.

For example, the streaming download API :meth:`readDataset() <tracdap.api.TracDataApi.readDataset>` is
wrapped up into a promise by ``loadStreamingData()`` and handled as shown in this example, hiding the
details of the stream event processing:

.. literalinclude:: ../../../examples/apps/javascript/src/error_handling.js
    :language: JavaScript
    :lines: 145 - 151
    :linenos:
    :lineno-start: 145
