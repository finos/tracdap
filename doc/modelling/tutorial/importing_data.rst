**************
Importing Data
**************

This tutorial is based on example code which can be found in the |examples_repo|.


Data that arrives from outside TRAC - files dropped by upstream systems, extracts from other
platforms, or reference data supplied by a business team - is brought in with an import job.
An import job **captures** files from an external storage location. No model code is needed:
the runtime reads each file once, keeps the original bytes as a FILE object and decodes the
content into a DATA object. The FILE is an unchanged copy of what was received, so every dataset
brought in this way can be traced back to the exact file it came from.


External storage
----------------

Files are captured from external storage locations, which are declared as resources in the
*sys_config* file with the resource type *EXTERNAL_STORAGE*. The example config has two external
locations for imports, using local folders:

.. literalinclude:: ../../../examples/models/python/config/sys_config.yaml
    :caption: config/sys_config.yaml
    :name: importing_data_sys_config
    :language: yaml
    :lines: 17-21,29-33

External storage is kept separate from the internal storage TRAC uses for its own datasets.
On the platform, external locations are set up by an administrator and each one is referred to
by its resource key.


Capturing files
---------------

Here is an import job that captures two files:

.. literalinclude:: ../../../examples/models/python/config/capture_job.yaml
    :caption: config/capture_job.yaml
    :name: capture_job_yaml
    :language: yaml
    :lines: 2-
    :linenos:
    :lineno-start: 2

Each entry under *captures* names one file. The *location* gives the storage key of an external
storage location and the path of a single file within it. The file format is taken from the file
extension - CSV, Parquet and Arrow files are supported.

Every capture produces two outputs, which must both be listed in the job config:

* **<capture name>** - the decoded dataset (a DATA object)
* **<capture name>_file** - the original file, byte for byte (a FILE object)

To run the job locally, use :py:func:`launch_job() <tracdap.rt.launch.launch_job>`, since there is
no model to pass in:

.. code-block:: python

    import tracdap.rt.launch as launch
    launch.launch_job("config/capture_job.yaml", "config/sys_config.yaml", dev_mode=True)


Schemas for captured data
-------------------------

The *schemaSource* setting says where the schema for each captured dataset comes from, and must
always be set:

* **CAPTURE_SCHEMA_DECLARED** - the schema is declared in the job. CSV files carry no type
  information, so they always need a declared schema.
* **CAPTURE_SCHEMA_FILE** - the schema is read from the file itself. This works for Parquet and
  Arrow files, which store their own schema.

A declared schema is applied the same way as a model input schema (see :doc:`using_data`). In this
example the currency file has an upper-case column name and an extra *rate_source* column that is not
in the schema: the column is renamed to match the schema and the extra column is dropped from the
dataset. The FILE output still holds the original file, extra column and all.

.. note::
    When running locally, a declared schema can be given as a schema file, as in this example. On the
    platform a declared schema must be a SCHEMA object that has already been loaded into TRAC,
    referred to with *schemaId*.


Capture size and attributes
---------------------------

Captured files are read into memory, so the runtime enforces a size limit for each capture. The
default is 100 MB, set with the *runtime.limit.captureSize* property (in MB).

On the platform, each capture can also set *dataAttrs* and *fileAttrs*, which are attributes
applied to the DATA and FILE objects it creates. These are applied by the platform when the job
results are saved, so they have no effect when running locally.


.. seealso::
    Full source code is available for the
    :example:`Capture job example on GitHub <config/capture_job.yaml>`.
