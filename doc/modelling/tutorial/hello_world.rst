
***********
Hello World
***********

This tutorial is based on example code which can be found in the |examples_repo|.


Writing a model
----------------

To write a model, start by importing the runtime's API package and inheriting from the
:py:class:`TracModel <tracdap.rt.api.TracModel>` base class. This class is the entry point
for running code with the runtime, both on the platform and when running locally.

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :caption: src/tutorial/hello_world.py
    :name: hello_world_py_part_1
    :language: python
    :lines: 16 - 20
    :linenos:
    :lineno-start: 16

The model can define any parameters it is going to need. In this example there is only a
single parameter so it can be declared in code (more complex models may wish to manage
parameters in a parameters file). The runtime provides helper functions to ensure parameters
are defined in the correct format.

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :name: hello_world_py_part_2
    :language: python
    :class: container
    :lines: 22 - 27
    :linenos:
    :lineno-start: 22

The model can also define inputs and outputs. In this case since all we are going to do
is write a message in the log, no inputs and outputs are needed. Still, these methods are
required in order for the model to be valid.

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :name: hello_world_py_part_3
    :language: python
    :class: container
    :lines: 29 - 33
    :linenos:
    :lineno-start: 29

To write the model logic, implement the :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>` method.
When :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>` is called it receives a
:py:class:`TracContext <tracdap.rt.api.TracContext>` object which allows models to interact with the
runtime.

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :name: hello_world_py_part_4
    :language: python
    :class: container
    :lines: 35 - 40
    :linenos:
    :lineno-start: 35

There are two useful features of :py:class:`TracContext <tracdap.rt.api.TracContext>`
that can be seen in this example:

    *   The :py:meth:`log() <tracdap.rt.api.TracContext.log>` method returns a standard Python logger
        that can be used for writing model logs. When models run on the platform, it will capture
        any logs written to this logger and make them available with the job outputs as searchable
        datasets. Log outputs are available even if a job fails so they can be used for debugging.

    *   :py:meth:`get_parameter() <tracdap.rt.api.TracContext.get_parameter>` allows models to access any
        parameters defined in the :py:meth:`define_parameters() <tracdap.rt.api.TracModel.define_parameters>`
        method. They are returned as native Python objects, so integers use the Python integer type,
        date and time values use the Python datetime classes and so on.


Supplying config
----------------

To run the model, we need to supply two configuration files:

    *   **Job config**, which includes everything related to the models and the data and
        parameters that will be used to execute them.

    *   **System config**, which includes everything related to storage locations, repositories,
        execution environment and other system settings.

When models are deployed to run on the platform, the runtime generates the job configuration according to scheduled
instructions and/or user input. A full set of metadata is assembled for every object and setting that goes
into a job, so that execution can be strictly controlled and validated.

Running locally uses **dev mode**, where the runtime infers and fills in this metadata itself instead of
requiring it up front - object IDs, storage definitions and schemas are generated from whatever is available
(parameters, file paths, the model class itself), rather than assembled from a governed catalog. This is why
the config needed to run a model locally can be kept short and readable, at the cost of the strict validation
a platform deployment enforces. :py:func:`launch_model() <tracdap.rt.launch.launch_model>` runs in dev mode by
default; the command-line launcher used in later tutorials (see :doc:`chaining`) needs the ``--dev-mode`` flag
to enable it explicitly.

For our Hello World model, we only need to supply a single parameter in the job configuration:

.. literalinclude:: ../../../examples/models/python/config/hello_world.yaml
    :caption: config/hello_world.yaml
    :name: hello_world_job_config
    :language: yaml
    :lines: 2-

Since this model is not using a Spark session or any storage, there is nothing that needs
to be configured in the system config. We still need to supply a config file though:

.. code-block:: yaml
    :caption: config/sys_config.yaml
    :name: hello_world_sys_config

    # The file can be empty, but you need to supply it!


Run the model
-------------

The easiest way to launch a model during development is to call
:py:meth:`launch_model() <tracdap.rt.launch.launch_model>`
from the runtime's launch package. Make sure to guard the launch by checking __name__ == "__main__", to
prevent launching a local config when the model is deployed to the platform (the runtime will not allow
this, but the model will fail to deploy)!

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :caption: src/tutorial/hello_world.py
    :name: hello_world_py_launch
    :language: python
    :lines: 43-
    :linenos:
    :lineno-start: 43

Paths for the system and job config files are resolved in the following order:

    1. If an absolute path is supplied, this takes priority
    2. Resolve relative to the current working directory
    3. Search relative to parents of the current directory
    4. Resolve relative to the directory containing the model
    5. Search relative to parents of the directory containing the model

Now you should be able to run your model script and see the model output in the logs:

.. code-block:: text
    :name: hello_world_log_output
    :class: container

    2022-05-31 12:19:36,104 [engine] INFO tracdap.rt.exec.engine.NodeProcessor - START RunModel [HelloWorldModel] / JOB-92df0bd5-50bd-4885-bc7a-3d4d95029360-v1
    2022-05-31 12:19:36,104 [engine] INFO __main__.HelloWorldModel - Hello world model is running
    2022-05-31 12:19:36,104 [engine] INFO __main__.HelloWorldModel - The input number is 42
    2022-05-31 12:19:36,104 [engine] INFO tracdap.rt.exec.engine.NodeProcessor - DONE RunModel [HelloWorldModel] / JOB-92df0bd5-50bd-4885-bc7a-3d4d95029360-v1


.. seealso::
    Full source code is available for the
    :example:`Hello World example on GitHub <src/tutorial/hello_world.py>`.
