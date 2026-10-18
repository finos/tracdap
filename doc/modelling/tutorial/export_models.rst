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
    :lines: 36 - 70
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
    :lines: 73 - 75
    :linenos:
    :lineno-start: 73


.. seealso::
    Full source code is available for the
    :example:`Export model example on GitHub <src/tutorial/data_export.py>`.
