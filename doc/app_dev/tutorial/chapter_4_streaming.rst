
#####################
Chapter 4 - Streaming
#####################

This tutorial is based on the *streaming.js* example, which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/apps/javascript*.

.. note::

    This example shows how to create and read a dataset using streaming upload and download operations.
    The same approach can be used to update datasets or to create, update and read files.

Data transport
--------------

The gRPC transport provided by Google in grpc-web does not yet support streaming uploads.
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

First let's create the initial request message. We are going to send data in CSV format,
so that we can stream data straight from a file to the TRAC data API without any transformation.
This example uses an embedded schema, but a schemaId for an external schema is also fine.
Tag attributes can be set as normal. The initial request message goes through the same validation
as a request to :meth:`readSmallDataset() <tracdap.api.TracDataApi.readSmallDataset>`,
except that the content can be empty.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 49 - 63
    :linenos:
    :lineno-start: 49

Now let's create the streaming source. The example code uses the *fs* module from Node.js
to create an input stream, then passes the stream into the upload function:

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 156
    :linenos:
    :lineno-start: 156

In a browser application, your source is most likely to be an HTML file input control.
The file input control supports streaming using the
`web streams API <https://developer.mozilla.org/en-US/docs/Web/API/Streams_API>`_,
which is different from the event streams used in Node.js and Protobuf.js.
TRAC provides a utility function to create an event stream, using a web ReadableStream as the source.

.. code-block:: javascript
    :linenos:
    :lineno-start: 156

        const csvInput = document.getElementById("input_id");
        const csvFile = csvInput.files[0];
        const csvStream = tracdap.utils.streamToEmitter(csvFile.stream());

.. note::

    To stream data from memory you can use
    `Blob.stream() <https://developer.mozilla.org/en-US/docs/Web/API/Blob/stream>`_
    with *streamToEmitter()*.

We're going to create a promise for the stream, which will complete when the streaming upload finishes.
Although we are sending a stream of messages to the server there will only be a single reply,
which can be a success or failure.

To set up the streaming call, we need to use the *newStream()* method in the web API
setup module. It is important to call this method for every new streaming call and
each stream can only be used once, otherwise messages from different calls will be mixed
in a single stream.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 64 - 73
    :linenos:
    :lineno-start: 64

After creating the stream, start by sending the initial message.
This will start the streaming upload operation.
This initial API call returns a future which holds the result of the whole operation,
so we can use this to complete the promise.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 74 - 80
    :linenos:
    :lineno-start: 74

Now the upload stream is open, we need to relay data from the source stream.
To do this we can handle the "data" event on the source stream which supplies
chunks of data from the input source. To send them to the upload stream,
each chunk needs to be wrapped in a :class:`DataWriteRequest<tracdap.api.DataWriteRequest>`.
The "end" event signals that the source stream is complete.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 81 - 94
    :linenos:
    :lineno-start: 81

The last thing is to handle any errors that occur on the source stream.
These are different from errors in the upload stream, which were handled earlier by *.catch(reject)*.

If there is an error in the source stream, we need to cancel the upload operation.
Calling *cancel()* will eventually produce an error on the upload stream,
but this will be an "operation cancelled" error with no information about what went wrong in the source.
Instead we want to reject the promise explicitly, to pass on the error information from the source stream.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 95 - 104
    :linenos:
    :lineno-start: 95

Streaming download
------------------

To download a data stream we make a single request and get back a stream of messages.
The first message in the stream will contain all the metadata and no content.
Subsequent messages will contain only content.

.. note::

    This example shows how to use a download stream and collect the result in memory.
    It is a useful approach for datasets that are too big to download with
    :meth:`readSmallDataset() <tracdap.api.TracDataApi.readSmallDataset>`,
    but where you still want to keep the whole dataset to display, sort, filter etc.

To start you need to create a :class:`DataReadRequest <tracdap.api.DataReadRequest>`.
This is exactly the same as the request used to call
:meth:`readSmallDataset() <tracdap.api.TracDataApi.readSmallDataset>`.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 110 - 116
    :linenos:
    :lineno-start: 110

Since we are going to collect the response data into a single message,
we can set up the streaming operation as a promise just like the upload operation.
The promise will complete once all the data is collected and aggregated.
If there are any errors during the operation, the promise will be rejected.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 118 - 127
    :linenos:
    :lineno-start: 118

The next step is to set up event handlers for the download stream.
There are three events to process: "data", "end" and "error".
In this example we will just collect the response messages from the
"data" events until they have all been received, and then use a TRAC
utility function to aggregate them into a single
:class:`DataReadResponse <tracdap.api.DataReadResponse>`.

.. note::

    The *aggregateStreamContent()* function works for both
    :class:`DataReadResponse <tracdap.api.DataReadResponse>` and
    :class:`FileReadResponse <tracdap.api.FileReadResponse>` messages.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 129 - 142
    :linenos:
    :lineno-start: 129

Now everything is ready, the final step is to make an API call to start
the download stream. Since we are using stream event processing, we need
to turn off processing of future results or callbacks by supplying no-op
handlers, to prevent JavaScript warnings about unhandled results / errors.

.. literalinclude:: ../../../examples/apps/javascript/src/streaming.js
    :language: JavaScript
    :lines: 144 - 153
    :linenos:
    :lineno-start: 144

.. note::

    The future / callback style of processing results works for streaming
    upload calls, because there is only a single response message for the
    whole operation. Download operations produce a stream of messages, so
    it is not possible to use a single handler and stream events are needed.
