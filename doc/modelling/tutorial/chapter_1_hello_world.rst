
#######################
Chapter 1 - Hello World
#######################

This tutorial is based on example code which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/models/python*.


Requirements
------------

.. include:: ../../../tracdap-runtime/python/README.md
    :start-after: ## Requirements
    :end-before: ## Installing the runtime

Setting up a new project
------------------------

If you are starting a project from scratch, it's a good idea to follow the standard
Python conventions for package naming and folder layout. If you are working on an
existing project or are already familiar with the Python conventions, then you can
:ref:`skip this section <modelling/tutorial/chapter_1_hello_world:Installing the runtime>`

For this example we will create a project folder called example-project. Typically
this will be a Git repository. You will also want to create a Python virtual environment
for the project. Some IDEs will be able to do this for you, or you can do it from the
command line using these commands:

.. tab-set::

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            mkdir example-project
            cd example-project
            git init
            python -m venv .\venv
            venv\Scripts\activate

    .. tab-item:: macOS / Linux
        :sync: platform_linux

        .. code-block:: shell

            mkdir example-project
            cd example-project
            git init
            python -m venv ./venv
            . venv/bin/activate

For this tutorial we want a single Python package that we will call "tutorial". By convention
Python source code goes in a folder called either "src" or the name of your project - we will
use "src". We are going to need some config files, those should be outside the source folder.
We will also need a folder for tests and a few other common project files.  Here is a very
standard example of what that looks like::

    examples-project
    ├── config
    │   ├── hello_world.yaml
    │   └── sys_config.yaml
    ├── src
    │   └── tutorial
    │       ├── __init__.py
    │       └── hello_world.py
    ├── test
    │   └── tutorial_tests
    │       ├── __init__.py
    │       └── test_hello_world_model.py
    ├── venv
    │   └── ...
    ├── .gitignore
    ├── README.txt
    └── ...

Let's quickly run through what these files are. First the src folder and the tutorial package.
In this example "tutorial" is our root package, which means any import statements in our code
should start with "import tutorial." or "from tutorial.xxx import yyy". To make the folder called
"tutorial" into a Python package we have to add the special __init__.py file, initially this
should be empty. We have created one module, hello_world, in the tutorial package and this is
where we will add the code for our model.

It is important to note that the "src" folder is not a package, rather it is the folder where our
packages live. This means that other folders and files (e.g. config, the .gitignore file and
everything else) do not get muddled into the Python package tree. If you see code that says
"import src.xxx" or "from src.xxx import yyy" then something has gone wrong!

The test folder contains our test code which is also arranged as a package. Notice that the package
name is not the same (tutorial_test instead of tutorial) - Python will not allow the same package
to be defined in two places. Putting the test code in a separate test folder stops it getting mixed
in with the code in src/, which is important when it comes to releasing code to production.

TRAC uses a few simple config files to control models during local development, so we have set up a
config folder to put those in. The contents of these files is discussed later in the tutorial.

The venv/ folder is where Python puts any libraries your project uses, including the TRAC runtime library.
Typically you want to ignore this folder in Git by adding it to the .gitignore file. Your IDE might
do this automatically, otherwise you can create a file called .gitignore and add this line to it:

.. code-block::

    venv/**

The README.txt file is not required but it is usually a good idea to have one. You can add a brief
description of the project, instructions for build and running the code etc. if you are using
GitHub the contents of this file will be displayed on the home page for your repository.


Installing the runtime
----------------------

The TRAC runtime package can be installed directly from PyPI::

    pip install tracdap-runtime

The TRAC runtime depends on Pandas and PySpark, so these libraries will be pulled in as
dependencies. If you want to target particular versions, you can install them explicitly::

    pip install "pandas == 2.1.4"

Alternatively, you can create *requirements.txt* in the root of your project folder and record
projects requirements there.

.. note::

    TRAC supports both Pandas 1.X and 2.X. Models written for 1.X might not work with 2.X and vice versa.
    From TRAC 0.6 onward, new installations default to Pandas 2.X. To change the version of Pandas in your
    sandbox environment, you can use the pip install command::

        pip install "pandas == 1.5.3"

Writing a model
---------------

To write a model, start by importing the TRAC API package and inheriting from the
:py:class:`TracModel <tracdap.rt.api.TracModel>` base class. This class is the entry point
for running code in TRAC, both on the platform and using the local development sandbox.

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :caption: src/tutorial/hello_world.py
    :name: hello_world_py_part_1
    :language: python
    :lines: 15 - 20
    :linenos:
    :lineno-start: 15

The model can define any parameters it is going to need. In this example there is only a
single parameter so it can be declared in code (more complex models may wish to manage
parameters in a parameters file). TRAC provides helper functions to ensure parameters
are defined in the correct format.

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :name: hello_world_py_part_2
    :language: python
    :lines: 21 - 27
    :linenos:
    :lineno-start: 21

The model can also define inputs and outputs. In this case since all we are going to do
is write a message in the log, no inputs and outputs are needed. Still, these methods are
required in order for the model to be valid.

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :name: hello_world_py_part_3
    :language: python
    :lines: 28 - 33
    :linenos:
    :lineno-start: 28

To write the model logic, implement the :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>` method.
When :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>` is called it receives a
:py:class:`TracContext <tracdap.rt.api.TracContext>` object which allows models to interact with the
TRAC platform.

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :name: hello_world_py_part_4
    :language: python
    :lines: 34 - 40
    :linenos:
    :lineno-start: 34

There are two useful features of :py:class:`TracContext <tracdap.rt.api.TracContext>`
that can be seen in this example:

    *   The :py:meth:`log() <tracdap.rt.api.TracContext.log>` method returns a standard Python logger
        that can be used for writing model logs. When models run on the platform, TRAC will capture
        any logs written to this logger and make them available with the job outputs as searchable
        datasets. Log outputs are available even if a job fails so they can be used for debugging.

    *   :py:meth:`get_parameter() <tracdap.rt.api.TracContext.get_parameter>` allows models to access any
        parameters defined in the :py:meth:`define_parameters()<tracdap.rt.api.TracModel.define_parameters>`
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
from the TRAC launch package. Make sure to guard the launch by checking __name__ == "__main__", to
prevent launching a local config when the model is deployed to the platform (TRAC will not allow
this, but the model will fail to deploy)!

.. literalinclude:: ../../../examples/models/python/src/tutorial/hello_world.py
    :caption: src/tutorial/hello_world.py
    :name: hello_world_py_launch
    :language: python
    :lines: 42-
    :linenos:
    :lineno-start: 42

Paths for the system and job config files are resolved in the following order:

    1. If absolute paths are supplied, these take top priority
    2. Resolve relative to the current working directory
    3. Resolve relative to the directory containing the Python module of the model

Now you should be able to run your model script and see the model output in the logs:

.. code-block:: text
    :name: hello_world_log_output

    2022-05-31 12:19:36,104 [engine] INFO tracdap.rt.exec.engine.NodeProcessor - START RunModel [HelloWorldModel] / JOB-92df0bd5-50bd-4885-bc7a-3d4d95029360-v1
    2022-05-31 12:19:36,104 [engine] INFO __main__.HelloWorldModel - Hello world model is running
    2022-05-31 12:19:36,104 [engine] INFO __main__.HelloWorldModel - The meaning of life is 42
    2022-05-31 12:19:36,104 [engine] INFO tracdap.rt.exec.engine.NodeProcessor - DONE RunModel [HelloWorldModel] / JOB-92df0bd5-50bd-4885-bc7a-3d4d95029360-v1


.. seealso::
    Full source code is available for the
    `Hello World example on GitHub <https://github.com/finos/tracdap/tree/main/examples/models/python/src/tutorial/schema_files.py>`_
