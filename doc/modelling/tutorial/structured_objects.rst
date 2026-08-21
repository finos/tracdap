
*************
Using STRUCTS
*************

This tutorial is based on example code which can be found in the |examples_repo|.


Not all data is naturally tabular. Structured data lets a model define an input or output
as a Python dataclass (or Pydantic model), including nested objects, enums and dictionaries,
rather than a flat table of fields.


Defining a struct schema
--------------------------

A struct schema is built from an ordinary Python dataclass. Structs can be nested inside one
another and can contain enums, dictionaries and other common Python types.

.. literalinclude:: ../../../examples/models/python/src/tutorial/structured_objects.py
    :caption: src/tutorial/structured_objects.py
    :name: structured_objects_py_types
    :language: python
    :lines: 16 - 45
    :linenos:
    :lineno-start: 16

To turn a dataclass into a schema, use :py:func:`define_struct() <tracdap.rt.api.define_struct>`.
The runtime validates the supplied type and builds a matching
:py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`, which can be used with
:py:func:`define_input() <tracdap.rt.api.define_input>` in the usual way. For outputs, the
shorthand :py:func:`define_output_struct() <tracdap.rt.api.define_output_struct>` combines both
steps.

.. literalinclude:: ../../../examples/models/python/src/tutorial/structured_objects.py
    :name: structured_objects_py_io
    :language: python
    :class: container
    :lines: 56 - 67
    :linenos:
    :lineno-start: 56


Reading and writing structured data
--------------------------------------

Structured inputs and outputs are read and written with
:py:meth:`get_struct() <tracdap.rt.api.TracContext.get_struct>` and
:py:meth:`put_struct() <tracdap.rt.api.TracContext.put_struct>`, passing the dataclass type to use for
the result. The runtime returns (and validates) an instance of that type, so the rest of the model
code can work with ordinary Python objects and attribute access, rather than looking up fields by name.

.. literalinclude:: ../../../examples/models/python/src/tutorial/structured_objects.py
    :name: structured_objects_py_run_model
    :language: python
    :class: container
    :lines: 69 - 81
    :linenos:
    :lineno-start: 69

.. note::
    The type passed to :py:meth:`get_struct() <tracdap.rt.api.TracContext.get_struct>` does not have
    to be the exact type used in :py:meth:`define_inputs() <tracdap.rt.api.TracModel.define_inputs>`,
    so long as the schema is compatible the runtime will perform the conversion. In practice, models
    should normally use the same type in both places, as this example does with ``RunConfig``.

This model reads a run configuration struct, adds a new stress scenario to it, and saves the
modified configuration as an output - which could then be picked up as the input to another
model further down a flow.


.. seealso::
    Full source code is available for the
    :example:`Structured Objects example on GitHub <src/tutorial/structured_objects.py>`.
