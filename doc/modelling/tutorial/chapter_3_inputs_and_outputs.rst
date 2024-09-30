
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
    `Optional IO example on GitHub <https://github.com/finos/tracdap/tree/main/examples/models/python/src/tutorial/optional_io.py>`_


Dynamic Inputs & Outputs
------------------------

Dynamic inputs and outputs allow a model to work with data when the schema is not known
in advance. This allows one model to work with a wide range of data inputs, which can be
very useful if you have common requirements that need to be satisfied across a large data
estate. Common examples include scenario generation, post-processing outputs, model
monitoring and data quality reporting.

.. tip::

    Only use dynamic schemas when they are really needed. Static schemas are more robust and
    allow the platform to make several optimisations that are not possible with dynamic schemas.

.. admonition:: Good to know

    TRAC normally performs schema validation before a job is executed, to make sure the models
    and data are compatible. When models are defined with dynamic schemas validation is delayed
    until runtime. This means validation errors will be reported when the job executes, rather
    than before it starts. For jobs with a mix of static and dynamic schemas TRAC will still
    validate the static schemas up front, validation is only delayed for the dynamic schemas.


Schema Inspection
^^^^^^^^^^^^^^^^^

This example use a dynamic input to get some information about the schema of an unknown dataset.
You could use this approach to build common reports for a large collection of datasets, because
the same model can run on .....

.. note::
    The TRAC metadata store already holds an entry for every dataset on the platform,
    you don't need to write a model just to get the schema of a dataset!
    However, the technique illustrated here can be used to build more detailed reports,
    such as data quality reports, model monitoring reports etc.

First let's define the model, it should take a generic dataset as an input and produce
some basic information about the schema and content:

.. literalinclude:: ../../../examples/models/python/src/tutorial/dynamic_io.py
    :caption: src/tutorial/dynamic_io.py
    :language: python
    :name: dynamic_io_schema_inspection_1
    :lines: 21 - 39
    :linenos:
    :lineno-start: 21

The source data is defined as a dynamic input. Notice that there are no fields in the
schema definition - dynamic inputs or outputs cannot define any fields, doing so will
result in a validation error. Since we know what data we want to collect about the
incoming dataset, the output schema can be defined as normal. This is a common pattern
for inspecting generalised data - the source schema will be dynamic but the expected
output is known.

Now let's see how to use these datasets in the model.

.. literalinclude:: ../../../examples/models/python/src/tutorial/dynamic_io.py
    :language: python
    :name: dynamic_io_schema_inspection_2
    :lines: 41 - 60
    :linenos:
    :lineno-start: 41

The model gets the source dataset as normal, but it also gets the schema of the dataset using
:py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>` which returns aTRAC
:py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>` object. The schema will
agree precisely with the contents of the dataset, including the field order and casing, so we
can use the information in the schema to operate on the dataset. In this example we just perform
a simple null check on all the columns and produce the output as a normal Pandas dataframe.

You can use the information held in the schema to look for columns with particular attributes
to decide how to process each column. Here are some examples of matching columns based on
type, nullability and the categorical flag::

    float_columns = [col.fieldName for col in columns if col.fieldType == trac.FLOAT]
    nullable_columns = [col.fieldName for col in columns if col.notNull != True]
    categorical_columns = [col.fieldName for col in columns if col.categorical]

.. note::
    Calling :py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>` returns a copy of
    the TRAC schema object. If you want to manipulate it in your model code, for example to add
    or remove fields, that is perfectly fine and will not cause any adverse effects. This can be
    useful to create a dynamic output schema based on the contents of a dynamic input. Calling
    :py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>` a second time at some later
    point will return a new copy of the schema, without any modifications made by the model code.

**Using dynamic inputs locally**

TRAC holds schema information for every dataset and passes this information on to models when
they run on the platform, which provides the schema for a dynamic input. When models run locally
in the IDE, TRAC has to do schema inference so the input files need to hold schema information.
Today, the most popular format for storing data files with schema information is Parquet.

Here is a sample job configuration for this model, using a Parquet file as the dynamic input.
A small sample data file is included with the tutorial, but you can use any Parquet file and
the model will tell you about its schema.

.. literalinclude:: ../../../examples/models/python/config/dynamic_io.yaml
    :caption: config/dynamic_io.yaml
    :language: yaml
    :name: dynamic_io_schema_inspection_config
    :linenos:

.. note::
    TRAC does not currently allow using CSV files as dynamic inputs when running locally. This is
    because of the need to do schema inference, which is not reliable for CSV files. You can create
    Parquet files to test dynamic inputs by running a model with the output defined as a .parquet file
    in the job configuration.


Data Generation
^^^^^^^^^^^^^^^

Another use for dynamic schemas is to generate datasets based on some criteria. In this example
the model receives one input which is a list of columns, and produces a output dataset which contains
those columns. Depending on the input that is supplied, the schema of the output will be different.

Let's see how to define this model:

.. literalinclude:: ../../../examples/models/python/src/tutorial/dynamic_io.py
    :language: python
    :name: dynamic_io_data_generation_1
    :lines: 63 - 80
    :linenos:
    :lineno-start: 63

In real life more parameters would often be needed to control the generated data, e.g. range limits
or distribution parameters, but for this simple example those are not needed.

Now let's look at the model code:

.. literalinclude:: ../../../examples/models/python/src/tutorial/dynamic_io.py
    :language: python
    :name: dynamic_io_data_generation_2
    :lines: 82 - 100
    :linenos:
    :lineno-start: 82

The model creates a :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>` and adds a
:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` for each column. The same helper functions
that are available for defining static schemas can be used to build dynamic schemas, in this example
we use :py:func:`trac.F() <tracdap.rt.api.F>` to define each field. You can also load schemas from
schema files using :py:func:`load_schema() <tracdap.rt.api.load_schema>`, then make changes to those
schemas in code and use the result as your dynamic output schema.

The model creates a data dictionary with some generated values for each column, so the output dataset
will match the generated schema. Before saving the dataset, the model calls
:py:meth:`put_schema() <tracdap.rt.api.TracContext.put_schema>` which sets the schema for a dynamic
output. Trying to save a dynamic output before its schema is set will cause a runtime validation error.
Only dynamic outputs can have their schema set in this way and each schema can only be set once.
If the model is updating an existing dataset, the schema must be compatible. The schema will be validated
as part of the call to :py:meth:`put_schema() <tracdap.rt.api.TracContext.put_schema>`.

.. note::
    Calling :py:meth:`put_schema() <tracdap.rt.api.TracContext.put_schema>` creates a copy of
    the TRAC schema object. Any changes made to the schema after it is saved will noo be picked
    up by TRAC. Calling :py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>` after
    a schema has been set will always return the schema as it was saved.

Once the schema is set the output can be saved as normal and TRAC will validate against the new schema.

Dynamic Filtering
^^^^^^^^^^^^^^^^^

Lastly, let's see how to use dynamic schemas to create a generic data filtering model. This model
will exclude records from a dataset, based on some filter criteria passed in as parameters. There
might be a lot of datasets we want to filter in this way, all with different schemas and we want a
single filtering model that will work for all of them.

Let's see an example model definition that can help us do that:

.. literalinclude:: ../../../examples/models/python/src/tutorial/dynamic_io.py
    :language: python
    :name: dynamic_io_dynamic_filtering_1
    :lines: 103 - 117
    :linenos:
    :lineno-start: 63

Here both the input and the output are dynamic, and the model is controlled only by the two filter
parameters. Now let's see the implementation:

.. literalinclude:: ../../../examples/models/python/src/tutorial/dynamic_io.py
    :language: python
    :name: dynamic_io_dynamic_filtering_2
    :lines: 119 - 130
    :linenos:
    :lineno-start: 130

The original input schema is used directly as the output schema, so the schema of the input
and output will be the same.


.. seealso::
    Full source code is available for the
    `Dynamic IO examples on GitHub <https://github.com/finos/tracdap/tree/main/examples/models/python/src/tutorial/dynamic_io.py>`_
