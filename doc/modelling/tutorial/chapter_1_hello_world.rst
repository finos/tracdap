
#######################
Chapter 1 - Hello World
#######################

This tutorial is based on the *hello_world.py* example, which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/models/python*.


Requirements
------------

.. include:: ../../../tracdap-runtime/python/README.md
    :start-after: ## Requirements
    :end-before: ## Installing the runtime


Installing the runtime
----------------------

The TRAC runtime package can be installed directly from PyPI::

    pip install trac-runtime

The TRAC runtime depends on Pandas and PySpark, so these libraries will be pulled in as
dependencies. If you want to target particular versions, install them explicitly first.


Writing a model
---------------

To write a model, start by importing the TRAC API package and inheriting from the
:py:class:`TracModel<trac.rt.api.TracModel>` base class. This class is the entry point
for running code in TRAC, both on the platform and using the local development sandbox.

.. literalinclude:: ../../../examples/models/python/hello_world/hello_world.py
    :caption: examples/models/python/hello_world/hello_world.py
    :name: hello_world_py_part_1
    :lines: 15 - 20
    :linenos:
    :lineno-start: 1

The model can define any parameters it is going to need. In this example there is only a
single parameter so it can be declared in code (more complex models may wish to manage
parameters in a parameters file). TRAC provides helper functions to ensure parameters
are defined in the correct format.

.. literalinclude:: ../../../examples/models/python/hello_world/hello_world.py
    :name: hello_world_py_part_2
    :lines: 21 - 27
    :linenos:
    :lineno-start: 7

The model can also define inputs and outputs. In this case since all we are going to do
is write a message in the log, no inputs and outputs are needed. Still, these methods are
required in order for the model to be valid.

.. literalinclude:: ../../../examples/models/python/hello_world/hello_world.py
    :name: hello_world_py_part_3
    :lines: 28 - 33
    :linenos:
    :lineno-start: 14

To write the model logic, implement the :py:meth:`run_model()<trac.rt.api.TracModel.run_model>` method.
When :py:meth:`run_model()<trac.rt.api.TracModel.run_model>` is called it receives a
:py:class:`TracContext<trac.rt.api.TracContext>` object which allows models to interact with the
TRAC platform.

.. literalinclude:: ../../../examples/models/python/hello_world/hello_world.py
    :name: hello_world_py_part_4
    :lines: 34 - 40
    :linenos:
    :lineno-start: 20

There are two useful features of :py:class:`TracContext<trac.rt.api.TracContext>`
that can be seen in this example:

    *   The :py:meth:`log()<trac.rt.api.TracContext.log>` method returns a standard Python logger
        that can be used for writing model logs. When models run on the platform, TRAC will capture
        any logs written to this logger and make them available with the job outputs as searchable
        datasets. Log outputs are available even if a job fails so they can be used for debugging.

    *   :py:meth:`get_parameter()<trac.rt.api.TracContext.get_parameter>` allows models to access any
        parameters defined in the :py:meth:`define_parameters()<trac.rt.api.TracModel.define_parameters>`
        method. They are returned as native Python objects, so integers use the Python integer type,
        date and time values use the Python datetime classes and so on.


Supplying config
----------------

To run the model, we need to supply two configuration files:

    *   **Job config**, which includes everything related to the models and the data and
        parameters that will be used to execute them.

    *   **System config**, which includes everything related to storage locations, repositories,
        execution environment and other system settings.

When models are deployed to run on the platform, TRAC generates the job configuration according to scheduled
instructions and/or user input. A full set of metadata is assembled for every object and setting that goes
into a job, so that execution can be strictly controlled and validated. In development mode most of this
configuration can be inferred, so the config needed to run models is kept short and readable.

For our Hello World model, we only need to supply a single parameter in the job configuration:

.. literalinclude:: ../../../examples/models/python/hello_world/hello_world.yaml
    :caption: examples/models/python/hello_world/hello_world.yaml
    :name: hello_world_job_config
    :lines: 2-

Since this model is not using a Spark session or any storage, there is nothing that needs
to be configured in the system config. We still need to supply a config file though:

.. code-block:: yaml
    :caption: sys_config.yaml
    :name: hello_world_sys_config

    # No system config needed!


Run the model
-------------

The easiest way to launch a model during development is to call
:py:meth:`launch_model()<trac.rt.launch.launch_model>`
from the TRAC launch package. Make sure to guard the launch by checking __name__ == "__main__", to
prevent launching a local config when the model is deployed to the platform (TRAC will not allow
this, but the model will fail to deploy)!

.. literalinclude:: ../../../examples/models/python/hello_world/hello_world.py
    :caption: examples/models/python/hello_world/hello_world.py
    :name: hello_world_py_launch
    :lines: 42-
    :linenos:
    :lineno-start: 28

Paths for the system and job config files are resolved in the following order:

    1. If absolute paths are supplied, these take top priority
    2. Resolve relative to the current working directory
    3. Resolve relative to the directory containing the Python module of the model

Now you should be able to run your model script and see the model output in the logs:

.. code-block:: text
    :name: hello_world_log_output

    2021-09-08 12:36:37,715 [engine] INFO trac.rt.exec.engine.NodeProcessor - START [Model]: HelloWorldModel / job=26ba932d-2904-4ac7-af45-a99b6d7e41fd
    2021-09-08 12:36:37,715 [engine] INFO __main__.HelloWorldModel - Hello world model is running
    2021-09-08 12:36:37,715 [engine] INFO __main__.HelloWorldModel - The meaning of life is 42
    2021-09-08 12:36:37,715 [engine] INFO trac.rt.exec.engine.NodeProcessor - DONE [Model]: HelloWorldModel / job=26ba932d-2904-4ac7-af45-a99b6d7e41fd


.. seealso::
    The full source code for this example is
    `available on GitHub <https://github.com/finos/tracdap/tree/main/examples/models/python/hello_world>`_
