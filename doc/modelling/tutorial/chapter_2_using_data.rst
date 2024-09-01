
######################
Chapter 2 - Using Data
######################

This tutorial is based on example code which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/models/python*.


Wrap existing code
------------------

In the previous tutorial, model code was written directly in the :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>`
method of the model class. An alternative approach is to put the model code in a separate class or function,
which can be called by :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>`. This can be useful
if you have a library of existing model code that you want to wrap with the TRAC model API.

If you are wrapping code in this way, it is important that all the required inputs are passed to
the top-level class or function as parameters, as shown in this example.


.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :caption: src/tutorial/using_data.py
    :name: using_data_py_part_1
    :language: python
    :lines: 15-51
    :linenos:
    :lineno-start: 15


Defining model requirements
---------------------------

Now let's write the TRAC model wrapper class. The previous tutorial showed how to define parameters
so we can use the same syntax. We'll define the three parameters needed by the model function:

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_2
    :language: python
    :lines: 52-67
    :linenos:
    :lineno-start: 52

The example model function has one data input, which is a table called *customer_loans*.
The function :py:func:`define_output_table() <tracdap.rt.api.define_output_table>` in the
TRAC API allows us to define a tabular dataset for use as a model input, which is exactly
what is needed. Each field is defined using the shorthand function :py:func:`trac.F() <tracdap.rt.api.F>`.
This approach works well for small models with simple schemas (the next tutorial discusses
managing more complex models using schema files).

Every field must have a name, type and label. Only scalar types are allowed for fields in table
schemas - it is not possible define a field which has a compound type such as
:py:attr:`MAP <tracdap.rt.metadata.BasicType.MAP>` or :py:attr:`ARRAY <tracdap.rt.metadata.BasicType.ARRAY>`.

In this example the dataset has a natural business key, so we can mark this in the schema.
Business key fields cannot contain nulls or duplicate records. Defining a business key is
optional, if the dataset doesn't have a natural business key there is no need to create one.
There are two categorical fields in this dataset which can be marked in the schema as well.
Setting business key and categorical flags will allow for more meaningful outputs, for example
by making information available to a UI for sorting and filtering. TRAC may also perform some
optimisations using these flags. As a general rule, define business key or categorical fields
where they are a natural expression of the data.

When the *customer_loans* dataset is accessed at runtime, TRAC will guarantee the dataset is supplied
with exactly this arrangement of columns: the order, case and data types will be exactly as defined.
Order and case are treated leniently - if the incoming dataset has a different field order or casing,
the fields will be reordered and renamed. Any extra fields will be dropped. Data types are also guaranteed
to match what is in the schema.

For models running locally, the *--dev-mode* option will enable a more lenient handling of data types.
In this mode, TRAC will attempt to convert data to use the specified field types, for example by parsing
dates stored as strings or casting integers to floats. Conversions that fail or lose data will not be allowed.
If the conversion succeeds, the dataset presented to the model is guaranteed to match the schema.
This option can be very useful for local development if data is held in CSV files. Models launched using
:py:func:`launch_model() <tracdap.rt.launch.launch_model()>` run in dev mode by default and will use
lenient type handling for input files.

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_3
    :language: python
    :lines: 68-78
    :linenos:
    :lineno-start: 68

To define the model outputs we can use :py:func:`define_output_table() <tracdap.rt.api.define_output_table>`,
which is identical to :py:func:`define_input_table() <tracdap.rt.api.define_input_table>` save for the fact it
returns an output schema. There are a few special cases where input and output schemas need to be treated
differently, but in the majority of cases they are the same.

Models are free to define multiple outputs if required, but this example only has one.

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_4
    :language: python
    :lines: 79-86
    :linenos:
    :lineno-start: 79

Now the parameters, inputs and outputs of the model are defined, we can implement the
:py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>` method.

Running the model
-----------------

To implement the :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>` method first we get
the three model parameters, which will come back with the correct Python types - *eur_usd_rate* and
*default_weighting* will be floats, *filter_defaults* will have type bool.

To get the input dataset we use the method :py:meth:`get_pandas_table() <tracdap.rt.api.TracContext.get_pandas_table>`.
The dataset name is the same name we used in :py:meth:`define_inputs() <tracdap.rt.api.TracModel.define_inputs>`.
This will create a Pandas dataframe, with column layout and data types that match what we defined in the
schema for this input.

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_5
    :language: python
    :lines: 87-94
    :linenos:
    :lineno-start: 87

Once all the inputs and parameters are available, we can call the model function. Since all the inputs
and parameters are supplied using the correct native types there is no further conversion necessary,
they can be passed straight into the model code.

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_6
    :language: python
    :lines: 95-98
    :linenos:
    :lineno-start: 95

The model code has produced a Pandas dataframe that we want to record as an output. To do this, we can use
:py:meth:`put_pandas_table() <tracdap.rt.api.TracContext.put_pandas_table>`. The dataframe should match
exactly with what is defined in the output schema. If any columns are missing or have the wrong data type,
TRAC will throw an error. When considering data types for outputs TRAC does provide some leniency. For example,
if a timestamp field is supplied with the wrong precision, or an integer column is supplied in place of decimals,
TRAC will perform conversions. Any conversion that would result in loss of data (e.g. values outside the allowed
range) will result in an error. The output dataset passed on to the platform is guaranteed to have the correct
data types as specified in :py:meth:`define_outputs() <tracdap.rt.api.TracModel.define_outputs>`.

If the column order or casing is wrong, or if there are extra columns, the output will be allowed but a
warning will appear in the logs. Columns will be reordered and converted to the correct case, any extra
columns will be dropped.

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_7
    :language: python
    :lines: 99-101
    :linenos:
    :lineno-start: 90

The model can be launched locally using :py:func:`launch_model() <tracdap.rt.launch.launch_model()>`.

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_8
    :language: python
    :lines: 102-
    :linenos:
    :lineno-start: 102

Configure local data
--------------------

To pass data into the local model, a little bit more config is needed in the *sys_config* file
to define a storage bucket. In TRAC storage buckets can be any storage location that can hold
files. This would be bucket storage on a cloud platform, but you can also use local disks or other
storage protocols such as network storage or HDFS, so long as the right storage plugins are available.

This example sets up one storage bucket called *example_data*. Since we are going to use a local disk,
the storage protocol is *LOCAL*. The *rootPath* property says where this storage bucket will be on disk -
a relative path is taken relative to the *sys_config* file by default, or you can specify an absolute path
here to avoid confusion.

The default bucket is also where output data will be saved. In this example we have only one storage
bucket configured, which is used for both inputs and outputs, so we mark that as the default.

.. literalinclude:: ../../../examples/models/python/config/sys_config.yaml
    :caption: config/sys_config.yaml
    :name: sys_config.yaml
    :language: yaml
    :lines: 2-12

In the *job_config* file we need to specify what data to use for the model inputs and outputs. Each
input named in the model must have an entry in the inputs section, and each output in the outputs
section. In this example we are using CSV files and just specify a simple path for each input
and output.

Input and output paths are always relative to the data storage location, it is not possible to use
absolute paths for model inputs and outputs in a job config. This is part of how the TRAC framework
operates, data is always accessed from a storage location, with locations defined in the system config.

The model parameters are also set in the job config, in the same way as the previous tutorial.

.. literalinclude:: ../../../examples/models/python/config/using_data.yaml
    :caption: config/using_data.yaml
    :name: using_data.yaml
    :language: yaml
    :lines: 2-

These simple config files are enough to run a model locally using sample data in CSV files.
Output files will be created when the model runs, if you run the model multiple times outputs
will be suffixed with a number.

.. seealso::
    Full source code is available for the
    `Using Data example on GitHub <https://github.com/finos/tracdap/tree/main/examples/models/python/src/tutorial/using_data.py>`_

Schema files
------------

For small models like this example defining schemas in code is simple, however for more complex
models in real-world situations the schemas are often quite large and can be reused across a set
of related models. To cater for more complex schemas, TRAC allows schemas to be defined in schema
files.

A schema file is just a CSV file that lists the field names, types and labels for a dataset as well as
any other optional flags. Here are the schema files for the input and output datasets of this model,
as you can see they provide the same information that was defined in code earlier.

.. csv-table:: customer_loans.csv
   :file: ../../../examples/models/python/src/tutorial/schemas/customer_loans.csv
   :header-rows: 1

.. csv-table:: profit_by_region.csv
   :file: ../../../examples/models/python/src/tutorial/schemas/profit_by_region.csv
   :header-rows: 1

The default values for the field flags are categorical = false, business_key = false and not_null = true
if business_key = true, otherwise not_null = false. The TRAC platform ignores the format_code field,
but it can be used to describe how data is displayed in client applications.

To use schema files, they must be included as part of your Python package structure. That means they
must be in the source tree with your Python code, in a package with an *__init__.py* file. If you are
building your model packages as Python Wheels or Conda packages the schema files must be included as
part of the build.

To add the schema files into the example project we can create a sub-package called "tutorial.schemas",
which would look like this::

    examples-project
    ├── config
    │   ├── sys_config.yaml
    │   ├── using_data.yaml
    │   └── ...
    ├── src
    │   └── tutorial
    │       ├── __init__.py
    │       ├── using_data.py
    │       └── schemas
    │           ├── __init__.py
    │           ├── customer_loans.csv
    │           └── profit_by_region.csv
    ├── test
    │   ├── test_using_data_model.py
    │   └── ...
    ├── requirements.txt
    ├── setup.py
    └── ...

Now we can re-write our model to use the new schema files. First we need to import the schemas package:

.. literalinclude:: ../../../examples/models/python/src/tutorial/schema_files.py
    :caption: src/tutorial/schema_files.py
    :name: using_data_part_9
    :language: python
    :lines: 19
    :linenos:
    :lineno-start: 19

Then we can load schemas from the schemas package in the
:py:meth:`define_inputs() <tracdap.rt.api.TracModel.define_inputs>` and
:py:meth:`define_outputs() <tracdap.rt.api.TracModel.define_outputs>` methods:

.. literalinclude:: ../../../examples/models/python/src/tutorial/schema_files.py
    :name: using_data_part_10
    :language: python
    :lines: 46 - 56
    :linenos:
    :lineno-start: 46

Notice that the :py:func:`load_schema() <tracdap.rt.api.load_schema>` method is the same
for input and output schemas, so we need to use
:py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>` and
:py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>` explicitly.

.. seealso::
    Full source code is available for the
    `Schema Files example on GitHub <https://github.com/finos/tracdap/tree/main/examples/models/python/src/tutorial/schema_files.py>`_
