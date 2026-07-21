
**************************************
Chapter 5 - Optional Inputs & Outputs
**************************************

This tutorial is based on example code which can be found in the |examples_repo|.


Optional inputs and outputs provide a way for a model to react to the available data.
If an input is marked as optional then it may not be supplied, the model code must check
at runtime to see if it is available. When an output is marked as optional the model can
choose whether to provide that output or not, for example in response to the input data
or a boolean flag supplied as a model parameter.

Here is an example of defining an optional input, using schemas read from schema files:

.. literalinclude:: ../../../examples/models/python/src/tutorial/optional_io.py
    :caption: src/tutorial/optional_io.py
    :name: optional_io_part_1
    :language: python
    :lines: 39 - 49
    :linenos:
    :lineno-start: 39

Schemas defined in code can also be marked as optional, let's use that approach to define an
optional output:

.. literalinclude:: ../../../examples/models/python/src/tutorial/optional_io.py
    :name: optional_io_part_2
    :language: python
    :class: container
    :lines: 51 - 67
    :linenos:
    :lineno-start: 51

Now let's see how to use optional inputs and outputs in :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>`.
Since the input is optional we will need to check if it is available before we can use it.
The runtime provides the :py:meth:`has_dataset() <tracdap.rt.api.TracContext.has_dataset>`
method for this purpose. If the optional dataset exists we will use it to apply
some filtering to the customer accounts list, then produce the optional output
dataset with some stats on the filtered accounts. Here is what that looks like:

.. literalinclude:: ../../../examples/models/python/src/tutorial/optional_io.py
    :name: optional_io_part_3
    :language: python
    :class: container
    :lines: 77 - 86
    :linenos:
    :lineno-start: 77

In this example the optional output is only produced when the optional input is
supplied - that is not a requirement and the model can decide whether to
provide optional outputs based on whatever criteria are appropriate.
If an optional output is not going to be produced, then simply do not output the
dataset and the runtime will understand it has been omitted. If an optional output is
produced then it is subject to all the same validation rules as any other dataset.

.. seealso::
    Full source code is available for the
    :example:`Optional IO example on GitHub <src/tutorial/optional_io.py>`.
