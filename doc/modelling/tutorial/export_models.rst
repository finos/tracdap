*************
Export Models
*************

This tutorial is based on example code which can be found in the |examples_repo|.


The placements in :doc:`exporting_data` write datasets to external storage exactly as they are.
When an export needs custom logic, such as reshaping data for a downstream system, adding fields or
writing a non-standard format, it can be done with an **export model**. An export model runs as a
node in a flow. It receives datasets from the flow like any other model, and it has write access
to the external storage locations the job grants it.

This tutorial adds an export model to the flow from :doc:`chaining`.

.. note::
    Export models are part of the experimental API in :py:mod:`tracdap.rt.api.experimental`,
    which may change in future versions of TRAC.


Writing an export model
-----------------------

Export models extend :py:class:`TracDataExport <tracdap.rt.api.experimental.TracDataExport>`
instead of :py:class:`TracModel <tracdap.rt.api.TracModel>`. Parameters and inputs are defined in
the usual way. Export models do not have outputs, everything they produce is written to external
storage.

.. literalinclude:: ../../../examples/models/python/src/tutorial/data_export.py
    :caption: src/tutorial/data_export.py
    :name: data_export_py_definition
    :language: python
    :lines: 16 - 34
    :linenos:
    :lineno-start: 16

The model is given the storage key to write to as a parameter, *storage_key*. In
:py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>`, the context object is a
:py:class:`TracDataContext <tracdap.rt.api.experimental.TracDataContext>`, which has a
:py:meth:`get_file_storage() <tracdap.rt.api.experimental.TracDataContext.get_file_storage>`
method to access a storage location by its key. The
:py:class:`TracFileStorage <tracdap.rt.api.experimental.TracFileStorage>` interface it returns
can create directories, check for existing files and write files as streams:

.. literalinclude:: ../../../examples/models/python/src/tutorial/data_export.py
    :name: data_export_py_run_model
    :language: python
    :class: container
    :lines: 36 - 73
    :linenos:
    :lineno-start: 36


Adding the export to a flow
---------------------------

The flow is the same as the one in :doc:`chaining`, with one extra node for the export model.
The node sets *modelType* to *DATA_EXPORT_MODEL*, and its input is connected to the
*profit_by_region* output of the aggregation model:

.. literalinclude:: ../../../examples/models/python/config/chaining_export_flow.yaml
    :caption: config/chaining_export_flow.yaml
    :name: chaining_export_flow_yaml
    :language: yaml
    :lines: 2-
    :linenos:
    :lineno-start: 2

The job config adds the export model and its parameters to the chaining job. The *exportStorageAccess*
list says which external storage locations export models in the flow are allowed to write to. A storage
key passed to an export model has to be in this list, or the model will not be able to access it.

.. literalinclude:: ../../../examples/models/python/config/chaining_export.yaml
    :caption: config/chaining_export.yaml
    :name: chaining_export_yaml
    :language: yaml
    :lines: 2-
    :linenos:
    :lineno-start: 2

The flow can be launched in the same way as in :doc:`chaining`:

.. literalinclude:: ../../../examples/models/python/src/tutorial/data_export.py
    :name: data_export_py_launch
    :language: python
    :class: container
    :lines: 76 - 78
    :linenos:
    :lineno-start: 76


Recording the job
-----------------

A downstream system that loads exported data often needs to know where it came from, for example to
link a report back to the job that produced its data. From TRAC 0.12, an export model can read the
metadata of the job it is running in with
:py:meth:`get_job_metadata() <tracdap.rt.api.experimental.TracDataContext.get_job_metadata>`.
It returns a :py:class:`RuntimeMetadata <tracdap.rt.api.RuntimeMetadata>`, the same type
:py:meth:`get_metadata() <tracdap.rt.api.TracContext.get_metadata>` returns for model inputs.
Its *objectId* is the ID of the job, and its *attributes* are the attributes the job was
submitted with, along with the ones TRAC records, such as who submitted the job and when.

The example model adds the job ID and the job's *name* attribute as columns of the data it exports:

.. literalinclude:: ../../../examples/models/python/src/tutorial/data_export.py
    :name: data_export_py_job_metadata
    :language: python
    :class: container
    :lines: 63 - 73
    :linenos:
    :lineno-start: 63
    :emphasize-lines: 5, 10 - 11

With the job ID, the downstream system can read the rest of the job's record through the TRAC
metadata API: its definition, its inputs and parameters, and its result once the job completes.
The export runs part-way through the job, so a system that needs final data should check the
job's status before it treats the export as complete.

Where the downstream system needs the data file to stay exactly as it is, the job metadata can be
written to a separate file next to it instead, such as a small JSON manifest holding the job ID,
the attributes the downstream system uses and the export model's parameters. Write the manifest
after the data, so a reader that finds the manifest knows the data is already complete.

.. note::
    Only import and export models can read the job metadata. A regular model's outputs must be
    the same each time it runs with the same inputs, so its results can't depend on which job
    it ran in.


.. seealso::
    Full source code is available for the
    :example:`Export model example on GitHub <src/tutorial/data_export.py>`.
