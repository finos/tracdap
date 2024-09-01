
############################
Chapter 3 - Inputs & Outputs
############################

This tutorial is based on example code which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/models/python*.

Optional Inputs & Outputs
-------------------------

Optional inputs and outputs provide a way for a model to react to the available data.
If an input is marked as optional then it may not be supplied, the model code must check
at runtime to see if it is available. When an output is marked as optional the model can
choose whether to provide that output or not, for example in response to the input data
or a boolean flag supplied as a model parameter.

Here is an example of defining an optional input, using schemas read from schema files:

.. literalinclude:: ../../../examples/models/python/src/tutorial/optional_io.py
    :caption: src/tutorial/optional_io.py
    :language: python
    :name: optional_io_part_1
    :lines: 38 - 48
    :linenos:
    :lineno-start: 38

Schemas defined in code can also be marked as optional, let's use that approach to define an
optional output:

.. literalinclude:: ../../../examples/models/python/src/tutorial/optional_io.py
    :language: python
    :name: optional_io_part_2
    :lines: 50 - 66
    :linenos:
    :lineno-start: 50

Now let's see how to use optional inputs and outputs in :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>`.
Since the input is optional we will need to check if it is available before we can use it.
TRAC provides the :py:meth:`has_dataset() <tracdap.rt.api.TracContext.has_dataset>`
method for this purpose. If the optional dataset exists we will use it to apply
some filtering to the customer accounts list, then produce the optional output
dataset with some stats on the filtered accounts. Here is what that looks like:

.. literalinclude:: ../../../examples/models/python/src/tutorial/optional_io.py
    :language: python
    :name: optional_io_part_3
    :lines: 76 - 85
    :linenos:
    :lineno-start: 76

In this example the optional output is only produced when the optional input is
supplied - that is not a requirement and the model can decide whether to
provide optional outputs based on whatever criteria are appropriate.
If an optional output is not going to be produced, then simply do not output the
dataset and TRAC will understand it has been omitted. If an optional output is
produced then it is subject to all the same validation rules as any other dataset.

.. seealso::
    Full source code is available for the
    `Optional IO example on GitHub <https://github.com/finos/tracdap/tree/main/examples/models/python/src/tutorial/schema_files.py>`_
