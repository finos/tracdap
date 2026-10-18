***************
Model Resources
***************

This tutorial is based on example code which can be found in the |examples_repo|.


Models sometimes need small static files that belong with the model code - an image for a report,
a template, a lookup table or some text. These can be included in the model's Python package as
**model resources**, and loaded at runtime. Because resources are part of the model code, they are
versioned with the model and the same resources are always used for a given model version.

.. note::
    Model resources are not the same as the resources declared in
    :py:meth:`define_resources() <tracdap.rt.api.TracModel.define_resources>`, which are
    connections to external systems.


Adding resources to a package
-----------------------------

Resource files live inside a Python package, which is a folder with an *__init__.py* file.
In the example project the resources are kept in their own sub-package, *tutorial.resources*:

.. code-block::
    :class: container

    examples-project
    ├── config
    │   └── ...
    ├── src
    │   └── tutorial
    │       ├── __init__.py
    │       ├── load_resources.py
    │       └── resources
    │           ├── __init__.py
    │           └── tracdap_banner_logo.png
    └── ...

When a model runs on the platform, TRAC checks out the model code from the repository at the model's
version. Resource files must be committed to the repository along with the code - files that exist only
in your local project will work when you run the model locally, but will be missing on the platform.


Loading a resource
------------------

To load a resource, import the package it lives in and pass the package and file name to one of the
resource functions in the runtime API:

* :py:func:`load_resource() <tracdap.rt.api.load_resource>` returns the content as bytes
* :py:func:`load_text_resource() <tracdap.rt.api.load_text_resource>` returns the content as a string
* :py:func:`load_resource_stream() <tracdap.rt.api.load_resource_stream>` and
  :py:func:`load_text_resource_stream() <tracdap.rt.api.load_text_resource_stream>` return a readable
  stream, which must be used in a ``with`` block

This example builds the same sales chart as the :doc:`file_io` tutorial, with a logo loaded from the
resources package shown above the chart. First the resources package is imported:

.. literalinclude:: ../../../examples/models/python/src/tutorial/load_resources.py
    :caption: src/tutorial/load_resources.py
    :name: load_resources_py_imports
    :language: python
    :lines: 23 - 24
    :linenos:
    :lineno-start: 23

Then the logo is read as a stream and passed straight to Matplotlib:

.. literalinclude:: ../../../examples/models/python/src/tutorial/load_resources.py
    :name: load_resources_py_run_model
    :language: python
    :class: container
    :lines: 46 - 77
    :linenos:
    :lineno-start: 46


Size limits
-----------

Resources are meant for small files. Large files slow down every checkout of the model repository,
so the runtime enforces a size limit when a resource is loaded. The default limit is 256 KB, and on
the platform it can be changed per tenant with the *runtime.limit.resourceSize* property (in KB).
The limit is checked when the resource is loaded, which is normally while the model runs, so a
resource over the limit causes the job to fail.

Larger files should be passed to the model as file inputs instead, see :doc:`file_io`.


.. seealso::
    Full source code is available for the
    :example:`Model Resources example on GitHub <src/tutorial/load_resources.py>`.
