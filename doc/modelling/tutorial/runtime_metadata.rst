
****************
Runtime Metadata
****************

This tutorial is based on example code which can be found in the |examples_repo|.


Models can look up the TRAC D.A.P. metadata associated with their inputs while they run, rather
than relying only on the data itself. This example builds a Markdown report describing each input
dataset, using both the runtime metadata and the schema.


Accessing metadata at runtime
--------------------------------

:py:meth:`get_metadata() <tracdap.rt.api.TracContext.get_metadata>` returns a
:py:class:`RuntimeMetadata <tracdap.rt.api.RuntimeMetadata>` object for a named input, giving access
to its TRAC object ID and any metadata attributes that were recorded when the dataset was created.

.. literalinclude:: ../../../examples/models/python/src/tutorial/runtime_metadata.py
    :caption: src/tutorial/runtime_metadata.py
    :name: runtime_metadata_py_run_model
    :language: python
    :lines: 45 - 61
    :linenos:
    :lineno-start: 45

.. note::
    Metadata is only available for inputs that come from real TRAC objects. Calling
    :py:meth:`get_metadata() <tracdap.rt.api.TracContext.get_metadata>` for a model parameter, an
    output before the job completes, or an input supplied as an intermediate result from another
    model in a flow, will return ``None`` rather than raising an error.


Building a report
--------------------

Combining :py:meth:`get_metadata() <tracdap.rt.api.TracContext.get_metadata>` with
:py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>` gives enough information to describe
a dataset without needing to know its structure in advance - the object ID and attributes come from
the metadata, while the field names and types come from the schema. The report itself is written out
using :py:meth:`put_file_stream() <tracdap.rt.api.TracContext.put_file_stream>`, the same file output
method used in :doc:`file_io`.

.. literalinclude:: ../../../examples/models/python/src/tutorial/runtime_metadata.py
    :name: runtime_metadata_py_report
    :language: python
    :class: container
    :lines: 63 - 97
    :linenos:
    :lineno-start: 63

This pattern - reading metadata and schema alongside the data itself - is useful any time a model
needs to behave generically across a range of datasets, such as building data quality reports or
generic monitoring tools, without hard-coding assumptions about what any particular input contains.


.. seealso::
    Full source code is available for the
    :example:`Runtime Metadata example on GitHub <src/tutorial/runtime_metadata.py>`.
