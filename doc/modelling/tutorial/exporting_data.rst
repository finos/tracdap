**************
Exporting Data
**************

This tutorial is based on example code which can be found in the |examples_repo|.


An export job sends datasets from TRAC to an external storage location, for example to feed a
downstream system or to deliver a report. Like an import, a simple export needs no model code:
each dataset is **placed** in external storage as a data file. Everything an export job writes
comes straight from data already held in TRAC, so an export can always be reproduced.

The external storage locations used for exports are set up in the *sys_config* file in the same
way as for imports, see :doc:`importing_data`. The example config has a location called
*exported_data* for this purpose.


Placing datasets
----------------

Here is an export job that places the same dataset in external storage three times, in three
different formats:

.. literalinclude:: ../../../examples/models/python/config/data_export.yaml
    :caption: config/data_export.yaml
    :name: data_export_yaml
    :language: yaml
    :linenos:

Each entry under *placements* names one file to write. The *dataId* says which dataset to place
and the *location* gives the storage key and the path of the file to write. The file format is taken
from the extension of the path - CSV, Parquet and Arrow files are supported.

When running locally, *dataId* gives the path of a data file in internal storage and its schema.
On the platform it selects a DATA object that is already held in TRAC.

To run the job locally, use :py:func:`launch_job() <tracdap.rt.launch.launch_job>`:

.. code-block:: python

    import tracdap.rt.launch as launch
    launch.launch_job("config/data_export.yaml", "config/sys_config.yaml", dev_mode=True)


Files that already exist
------------------------

The *placementConflict* setting says what happens when a file already exists at a placement's path.
It applies to every placement in the job:

* **PLACEMENT_SUFFIX** - write the file with a numbered suffix, using the first free name
  (*name-2.ext*, *name-3.ext* and so on). This is the default.
* **PLACEMENT_OVERWRITE** - replace the existing file.
* **PLACEMENT_FAIL** - fail the job. Existing files are checked before anything is written,
  so a failed job does not leave a partial export behind.

The example uses *PLACEMENT_OVERWRITE*, so the job can be run repeatedly and always leaves the
latest files in place.


Custom exports
--------------

Placements write a dataset to a file exactly as it is. If an export needs custom logic - such as
reshaping the data for a downstream system, adding fields or writing a non-standard format - it can
be done with an export model, which runs as part of a flow. See :doc:`export_models`.


.. seealso::
    Full source code is available for the
    :example:`Data export example on GitHub <config/data_export.yaml>`.
