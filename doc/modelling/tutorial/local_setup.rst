
***********************
Chapter 1 - Local Setup
***********************

This tutorial is based on example code which can be found in the |examples_repo|.


Requirements
------------

The TRAC D.A.P. runtime for Python has these requirements:

* Python: 3.10 or later
* Pandas: 1.3 or later (optional)
* NumPy: 1.22 or later (optional, required by Pandas)
* Polars: 1.X (optional)

3rd party libraries may impose additional constraints on supported versions of key libraries.
For example, Pandas 1.5 is not available for Python 3.12 or 3.13, while NumPy 2.0 is only
compatible with Pandas 2.1 and later.


Setting up a new project
------------------------

If you are starting a project from scratch, it's a good idea to follow the standard
Python conventions for package naming and folder layout. If you are working on an
existing project or are already familiar with the Python conventions, then you can
:ref:`skip this section <installing-the-runtime>`.

For this example we will create a project folder called example-project. Typically
this will be a Git repository. You will also want to create a Python virtual environment
for the project. Some IDEs will be able to do this for you, or you can do it from the
command line using these commands:

.. tab-set::

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch
            :class: container

            mkdir example-project
            cd example-project
            git init
            python -m venv .\venv
            venv\Scripts\activate

    .. tab-item:: macOS / Linux
        :sync: platform_linux

        .. code-block:: shell
            :class: container

            mkdir example-project
            cd example-project
            git init
            python -m venv ./venv
            . venv/bin/activate

For this tutorial we want a single Python package that we will call "tutorial". By convention
Python source code goes in a folder called either "src" or the name of your project - we will
use "src". We are going to need some config files, those should be outside the source folder.
We will also need a folder for tests and a few other common project files.  Here is a very
standard example of what that looks like:

.. code-block::
    :class: container

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

The runtime uses a few simple config files to control models during local development, so we have set up a
config folder to put those in. The contents of these files is discussed later in the tutorial.

The venv/ folder is where Python puts any libraries your project uses, including the runtime library.
Typically you want to ignore this folder in Git by adding it to the .gitignore file. Your IDE might
do this automatically, otherwise you can create a file called .gitignore and add this line to it:

.. code-block::
    :class: container

    venv/**

The README.txt file is not required but it is usually a good idea to have one. You can add a brief
description of the project, instructions for build and running the code etc. if you are using
GitHub the contents of this file will be displayed on the home page for your repository.


.. _installing-the-runtime:

Installing the runtime
----------------------

The runtime package can be installed directly from PyPI:

.. code-block::
    :class: container

    pip install tracdap-runtime

The runtime depends on Pandas and PySpark, so these libraries will be pulled in as
dependencies. If you want to target particular versions, you can install them explicitly:

.. code-block::
    :class: container

    pip install "pandas == 2.1.4"

Alternatively, you can create *requirements.txt* in the root of your project folder and record
projects requirements there.

.. note::

    The runtime supports both Pandas 1.X and 2.X. Models written for 1.X might not work with 2.X and vice versa.
    From TRAC D.A.P. 0.6 onward, new installations default to Pandas 2.X. To change the version of Pandas in your
    sandbox environment, you can use the pip install command:

    .. code-block::
        :class: container

        pip install "pandas == 1.5.3"
