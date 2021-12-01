
#####################
Lesson 3 - Using Data
#####################

This lesson is based on the *using_data.js* example, which can be found in the
`TRAC GitHub Repository <https://github.com/Accenture/trac>`_
under *examples/apps/javascript*.


Connecting to the data API
==========================

In order to use the data API, we will need an RPC connector and an instance of the API class.
Here is how to set them up for a browser-based app:

.. code-block:: JavaScript
    :linenos:
    :lineno-start: 22

    // Create the Data API
    const dataApiRpcImpl = trac.setup.rpcImplForBrowser(trac.api.TracDataApi);
    const dataApi = new trac.api.TracDataApi(dataApiRpcImpl);

For a Node.js or standalone environment, create a connector pointing at your TRAC instance:

.. literalinclude:: ../../../examples/apps/javascript/src/using_data.js
    :language: JavaScript
    :lines: 22 - 24
    :linenos:
    :lineno-start: 22


Saving data from files
======================

Suppose the user has a data file on disk that they want to load into the platform.
For simplicity, let's assume it is already in CSV format. If the file is reasonably
small we can load it into memory as a blob, using either the ``FileReader`` API in a
browser or the ``fs`` API for Node.js environments.

In order to save data to TRAC we will need to supply a schema. This can either be done
by providing a full :class:`SchemaDefinition<trac.metadata.SchemaDefinition>` to be embedded
with the dataset, or by providing the ID of an existing schema in the TRAC metadata store.
In this example we use the latter approach, see lessons 1 & 2 for examples of how to create
a schema and search for its ID.

Once both schema and data are available, we can create a
:class:`DataWriteRequest<trac.api.DataWriteRequest>`.

.. literalinclude:: ../../../examples/apps/javascript/src/using_data.js
    :language: JavaScript
    :lines: 27 - 43
    :linenos:
    :lineno-start: 27

Here, ``schemaId`` is the :class:`TagHeader<trac.metadata.TagHeader>`
(or :class:`TagSelector<trac.metadata.TagSelector>`) for a schema created earlier.
The ``format`` field must be a MIME type for a supported data format and the ``content``
field contains binary data encoded in that format. Since our ``csvData`` blob contains
data loaded from a CSV file, we know it is already in the right format.

When data is saved the platform will create a DATA object in the metadata store to describe
the dataset. This DATA object can be indexed and searched for, so we use
:class:`TagUpdate<trac.metadata.TagUpdate>` instructions to apply tag attributes just
like any other type of object.

Now the data API can be used to send the new dataset to the platform:

.. literalinclude:: ../../../examples/apps/javascript/src/using_data.js
    :language: JavaScript
    :lines: 45 - 51
    :linenos:
    :lineno-start: 45

Here we used :meth:`createSmallDataset()<trac.api.TracDataApi.createSmallDataset>`, which
assumes the content of the dataset is small enough to be sent as a single blob in the ``content``
field. Since the data has already been loaded into memory, this approach avoids the complexity
of using streaming calls. (An equivalent client-streaming method,
:meth:`createDataset()<trac.api.TracDataApi.createDataset>`, is available in the platform API
but not currently supported over gRPC-Web).

The :meth:`createSmallDataset()<trac.api.TracDataApi.createSmallDataset>` method returns the ID of
the newly created dataset as a :class:`TagHeader<trac.metadata.TagHeader>`, which includes the
object type, ID, version and timestamps. In this example we used the promise form of the method,
the equivalent call using a callback would be:

.. code-block:: JavaScript

    dataApi.createSmallDataset(request, (err, dataId) => {

        // Handle error or response
    });


Loading data from TRAC
======================

Now suppose we want to get the dataset back out of TRAC, for example to display it in a web page.
To do this we use a :class:`DataReadRequest<trac.api.DataReadRequest>`.

.. literalinclude:: ../../../examples/apps/javascript/src/using_data.js
    :language: JavaScript
    :lines: 53 - 62
    :linenos:
    :lineno-start: 53

As well as supplying a selector for the dataset (in this case the ``dataId`` created earlier), we use the
``format`` field to say what format the data should come back in, which must be a MIME type
for a data format supported by the platform. In this case the request ask for data in JSON format.

Now let's send the request to the data API:

.. literalinclude:: ../../../examples/apps/javascript/src/using_data.js
    :language: JavaScript
    :lines: 64 - 72
    :linenos:
    :lineno-start: 64

Again, by using :meth:`readSmallDataset()<trac.api.TracDataApi.readSmallDataset>` we are assuming
that the content of the dataset can fit as single blob in one response message. For relatively small
datasets that will be displayed in a web page, this approach avoids the complexity of streaming calls.
An equivalent server-streaming call, :meth:`readDataset()<trac.api.TracDataApi.readDataset>`, is
available and supported in the web API package.

In order to use the data that comes back, it needs to be decoded. Since the data is in JSON format
this is easily done using a ``TextDecoder`` and ``JSON.parse()``, which will create an array of objects.
The response also includes the full schema of the dataset, in this example we are returning both the
schema and the decoded data.

Exactly how the data is rendered on the screen will depend on the application framework being used.
One common approach is to use an "accessor" method, which allows a renderer to access elements of the
dataset that it needs to display by row and column index. We can create an accessor function for the
decoded dataset like this:

.. literalinclude:: ../../../examples/apps/javascript/src/using_data.js
    :language: JavaScript
    :lines: 74 - 84
    :linenos:
    :lineno-start: 74

Here rows are looked up by index, for columns we must find the field name for the column and then
do a dictionary lookup.

Saving data from memory
=======================

To continue the example, let's suppose the data has been displayed on screen, the user has edited
some values and now wants to save their changes as a new version of the dataset. The modified data
exists in memory as an array of JavaScript objects, so we need to encoded it before it can be sent
back. To encode the data using JSON:

.. literalinclude:: ../../../examples/apps/javascript/src/using_data.js
    :language: JavaScript
    :lines: 135 - 139
    :linenos:
    :lineno-start: 135

Now we need to create a :class:`DataWriteRequest<trac.api.DataWriteRequest>` for the update:

.. literalinclude:: ../../../examples/apps/javascript/src/using_data.js
    :language: JavaScript
    :lines: 141 - 158
    :linenos:
    :lineno-start: 141

Since we are updating an existing dataset, the ``priorVersion`` field must be used to specify the original
object ID. This works the same way as for metadata updates: only the latest version of a dataset can be updated.
So, if we are trying to update version 1 to create version 2, our update will fail if someone has already created
version 2 before us. In this case, the user would have to reload the latest version and try the update again.

The ``schemaId``, ``format`` and ``content`` fields are specified as normal, in this example the schema has not
changed, so the schema ID will be the same. (Schema changes are restricted between dataset versions to ensure
each version is backwards-compatible with the versions that came before).

Since this is an update operation, the existing tags will be carried forward onto the new version of the object.
We only need to specify tags we want to change in the ``tagUpdates`` field. In this example we add one new
tag to describe the change.

To send the update to the platform, we use :meth:`updateSmallDataset()<trac.api.TracDataApi.readSmallDataset>`:

.. literalinclude:: ../../../examples/apps/javascript/src/using_data.js
    :language: JavaScript
    :lines: 160 - 166
    :linenos:
    :lineno-start: 160


Again we are assuming that the content of the dataset can be sent as single blob in one message.
(An equivalent client-streaming method, :meth:`updateDataset()<trac.api.TracDataApi.createDataset>`,
is available in the platform API but not currently supported over gRPC-Web).

The method returns the ID for the updated version of the dataset as a :class:`TagHeader<trac.metadata.TagHeader>`.
