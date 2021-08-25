# TRAC Model Runtime for Python

*A next-generation data and analytics platform for use in highly regulated environments*

This is a beta version of the Python model runtime, to give early sight of what it is
like to build and run models in TRAC. The current version works with Pandas data, there
are notable gaps around validation and type conversion. These gaps will be filled in the
second beta, along with PySpark support.

## Requirements

The Python Runtime has these requirements:

* Python: 3.7 or later
* Pandas: 1.0 or later
* PySpark 2.4.x or 3.0.x

Our CI builds test the oldest and newest version of each requirement.
Not every combination of versions will work, e.g. PySpark 3 requires Python 3.8.

Since Pandas has gone to a 1.0 release in January 2020 and removed several
deprecated APIs from the 0.x series, TRAC requires Pandas 1.0 as a baseline.
Support for Spark and Python starts with the oldest versions that are actively
supported.

## Building the Python Runtime

Until the TRAC runtime is published to PyPI, it is necessary to build a local
package.

To build the runtime on Linux or macOS:

    python -m venv --system-site-packages ./venv 
    . venv/bin/activate
    pip install -r requirements.txt
    
    python codegen/protoc-ctrl.py --domain
    python codegen/protoc-ctrl.py --proto
    
    python setup.py sdist bdist_wheel

To build the runtime on Windows:

    python -m venv --system-site-packages .\venv 
    venv\Scripts\activate
    pip install -r requirements.txt
    
    python codegen\protoc-ctrl.py --domain
    python codegen\protoc-ctrl.py --proto
    
    python setup.py sdist bdist_wheel

The build process will create outputs and intermediates under this folder, which
is a requirement of Python setuptools.

## Writing TRAC Models

Create a new Python project using your favourite IDE and set it up with a new venv.
With the venv activated, you can install the TRAC runtime package: 

    pip install path/to/trac/trac-runtime/python/dist/trac_runtime-<version>.whl

The TRAC runtime depends on Pandas, PySpark and Protobuf, if you do not have
these installed they will be pulled in as dependencies. If you want to target a
particular version of these libraries, install them explicitly first.

Once the runtime is installed, you can write your first TRAC model. Start by
inheriting the TracModel base class:

    import trac.rt.api as trac

    class SampleModel(trac.TracModel):
    
Your IDE will be able to generate stubs for the model API (for PyCharm on Windows,
press *ctrl + i*). Fill in the stubs to define your inputs, outputs and parameters,
then you can add your model code in run_model().

For model code examples, look at the [documented model examples for Python](../../examples/models/python).
We're running these models from CI, so they will work with a runtime built from
the same version of the code.

If you are using IntelliJ IDEA you can run the examples directly out of the IDE.
In the project root directory, run dev\ide\copy_settings.bat or dev/ide/copy_settings
to update your IDEA config with module definitions for the Python runtime. You'll need
to change the module configs to use your Python venv for TRAC as their runtime. Once
that is done you can use the pre-defined run configs, use the Codegen run config first
to generate the  Python metadata classes, then use the Examples run config to run the
Python example models as tests.
