
Chapter 4 - Streaming
#####################

This tutorial is based on the *streaming.js* example, which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/apps/javascript*.


Data transport
--------------

The gRPC transport provided by Google in grpc-web does not yet support streaming uploads[1].
Fortunately, TRAC provides a gRPC transport that does support streaming for both uploads
and downloads, using a web sockets implementation based on the work by Improbable Eng.

The TRAC transport is available as an option in the transport setup:

.. code-block:: javascript
    :linenos:
    :lineno-start: 19

    // Create the Data API
    const transportOptions = {transport: "trac"};
    const dataTransport = tracdap.setup.transportForBrowser(tracdap.api.TracDataApi, transportOptions);
    const dataApi = new tracdap.api.TracDataApi(dataTransport);

Or to run outside a browser:

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 19 - 23
    :linenos:
    :lineno-start: 19

The options for the *transport* parameter are *trac* or *google*, the default is *google*. It is fine
to use the *trac* transport for the data service and *google* transport for everything else,
this is the recommended approach. The *trac* transport is only needed for streaming upload calls,
if you are only downloading data you can use the default *google* transport.


Streaming upload
----------------

To upload a data stream we are going to send a series of messages in one streaming upload call.
The first message contains all the settings needed in a :class:`DataWriteRequest<tracdap.api.DataWriteRequest>`,
but no content. The following messages contain content only with no other settings,
this content will usually come from a streaming source.

Let's create the streaming source first. The example code uses the Node *fs* module:

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 146
    :linenos:
    :lineno-start: 146

In a browser application you could use the fetch API on a selected file. Since TRAC uses the Protobuf.js
EventEmitter pattern, you will need to convert the ReadableStream return by fetch() into an EventEmitter.
TRAC provides a utility function to help with this:

.. code-block:: javascript
    :linenos:
    :lineno-start: 146

        const csvFetch = fetch("file://local/file.csv");
        const csvStream = csvFetch.then(response => tracdap.util.emitterForReadableStream(response.body))

TODO: write the emitter converter util

Once the source is open, create the initial message. We are going to send data in CSV format,
to send the content directly from the file stream without transformation.
This example uses an embedded schema, but a schemaId for an external schema is also fine.
Tag attributes can be set as normal.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 49 - 63
    :linenos:
    :lineno-start: 49

To set up the streaming call, we need to use the *newStream()* method in the web API
setup module. It is important to call this method for every new streaming call and
each stream can only be used once, otherwise messages from different calls will be mixed
in a single stream.

We're going to create a promise for the stream, which will complete when the streaming upload finishes.
Although we are sending a stream of messages to the server there will only be a single reply,
which can be a success or failure.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 64 - 73
    :linenos:
    :lineno-start: 64

After creating the stream, start by sending the initial message. The API call returns a future,
which can be used to handle the result of the whole stream upload operation:

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 74 - 80
    :linenos:
    :lineno-start: 74

Now we can start sending content, by relaying data events on the source stream.
In this example the stream event we want to relay is "data", and we are putting
each chunk directly into a :class:`DataWriteRequest<tracdap.api.DataWriteRequest>`
as content. This is by far the simplest approach.

If you do want to perform any transformations they can be applied here, remember
chunks can break at any point including inside a record or even inside a multi-byte
character in a string.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 81 - 90
    :linenos:
    :lineno-start: 81

The last thing we need to do is handle errors and completion on the source stream.
For our example using the *fs* stream source, that looks like this:

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 91 - 98
    :linenos:
    :lineno-start: 91

Streaming download
------------------

To download a data stream we make a single request and get back a stream of messages.

TODO

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 101 - 141
    :linenos:
    :lineno-start: 101