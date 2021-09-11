
######################
Lesson 1 - Hello World
######################


Requirements
------------

.. include:: ../../../trac-runtime/python/README.md
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

To write a model, start by inheriting from the :py:class:`TracModel<trac.rt.api.TracModel>`
base class and implementing the abstract methods. Here is the Hello World example:

.. literalinclude:: ../../../examples/models/python/hello_world/hello_world.py
    :caption: examples/models/python/hello_world/hello_world.py
    :name: hello_world_py_model
    :lines: 15-42


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
    :lines: 44-

TRAC will resolve the paths for the system and job config files by looking in the following places:

    1. If absolute paths are supplied, these have top priority
    2. The current working directory
    3. The directory containing the model

Now you should be able to run your model script and see the model output in the logs:

.. code-block:: text
    :name: hello_world_log_output

    2021-09-08 12:36:37,715 [engine] INFO trac.rt.exec.engine.NodeProcessor - START [Model]: HelloWorldModel / job=26ba932d-2904-4ac7-af45-a99b6d7e41fd
    2021-09-08 12:36:37,715 [engine] INFO __main__.HelloWorldModel - Hello world model is running
    2021-09-08 12:36:37,715 [engine] INFO __main__.HelloWorldModel - The meaning of life is 42
    2021-09-08 12:36:37,715 [engine] INFO trac.rt.exec.engine.NodeProcessor - DONE [Model]: HelloWorldModel / job=26ba932d-2904-4ac7-af45-a99b6d7e41fd

.. seealso::
    The full source code for this example is available in the
    `TRAC GitHub repository <https://github.com/Accenture/trac/tree/main/examples/models/python/hello_world>`_
