
************
Using Polars
************

This tutorial is based on example code which can be found in the |examples_repo|.


The runtime supports Polars as an alternative to Pandas for reading and writing tabular
data. This tutorial builds the same PnL aggregation model from :doc:`using_data`, using
Polars instead of Pandas, to show how the two compare.


Reading and writing Polars tables
----------------------------------

To work with Polars instead of Pandas, use :py:meth:`get_polars_table() <tracdap.rt.api.TracContext.get_polars_table>`
and :py:meth:`put_polars_table() <tracdap.rt.api.TracContext.put_polars_table>` in place of the Pandas
equivalents. Everything else about defining inputs and outputs is unchanged - the schema, parameters
and job config are all identical to the Pandas version of this model.

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_polars.py
    :caption: src/tutorial/using_polars.py
    :name: using_polars_py_run_model
    :language: python
    :lines: 89 - 101
    :linenos:
    :lineno-start: 89

The calculation itself uses Polars' lazy API. Building the query with :py:meth:`.lazy() <polars.DataFrame.lazy>`
defers execution until the result is actually needed, which allows Polars to optimise the whole
sequence of operations rather than running each step eagerly. Calling
:py:meth:`.collect() <polars.LazyFrame.collect>` evaluates the lazy query and returns a materialised
dataframe, which is what the runtime expects when saving an output.

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_polars.py
    :name: using_polars_py_calculation
    :language: python
    :class: container
    :lines: 24 - 50
    :linenos:
    :lineno-start: 24


Model attributes
-----------------

.. note::
    Model attributes are an experimental API that is not yet stabilised, expect changes in
    future versions of TRAC D.A.P.

Models can define a set of attributes to catalogue and describe themselves, by implementing
:py:meth:`define_attributes() <tracdap.rt.api.TracModel.define_attributes>`. Attributes are defined using
:py:func:`define_attributes() <tracdap.rt.api.define_attributes>`, which takes a number of individual
attributes built with :py:func:`A() <tracdap.rt.api.A>` (a shorthand alias for
:py:func:`define_attribute() <tracdap.rt.api.define_attribute>`).

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_polars.py
    :name: using_polars_py_attributes
    :language: python
    :class: container
    :lines: 55 - 61
    :linenos:
    :lineno-start: 55

A name and value are always required for each attribute. Attribute type is optional for single-valued
attributes but required for multivalued attributes, such as the *classifiers* attribute in this example.
The categorical flag can be applied to STRING attributes, to mark them for use in categorical searches
and filters.


.. seealso::
    Full source code is available for the
    :example:`Using Polars example on GitHub <src/tutorial/using_polars.py>`.
