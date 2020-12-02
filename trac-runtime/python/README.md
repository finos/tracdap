# TRAC Model Runtime for Python

*A next-generation data and analytics platform for use in highly regulated environments*

This package contains just the model runtime API, the execution engine will follow shortly!

## Requirements

The Python Runtime has these requirements:

* Python: 3.6 or later
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

Here is a hello-world example that receives two parameters and writes some output
to the model run log.

    import typing as tp
    import trac.rt.api as trac
    
    
    class SampleModel(trac.TracModel):
    
        def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
    
            # trac.P is an alias for define_parameter()
            # Every parameter must have a label and a type
            # More details such as default values, formatting etc. are optional
    
            return {
                'start_date': trac.P('A date to say hello', trac.BasicType.DATE),
                'end_date': trac.P('A date to say goodbye', trac.BasicType.DATE)
            }
    
        def define_inputs(self) -> tp.Dict[str, trac.TableDefinition]:
        
            # No inputs
            return {}
    
        def define_outputs(self) -> tp.Dict[str, trac.TableDefinition]:
        
            # No outputs
            return {}
    
        def run_model(self, ctx: trac.TracContext):
    
            # Parameters of type DATE are provided using Python's standard datetime.date class
            start_date = ctx.get_parameter('start_date')
            end_date = ctx.get_parameter('end_date')
    
            elapsed_days = (end_date - start_date).days
    
            # TRAC supplies a standard Python logger for use in models
            # Log output will be stored automatically when models run on the platform
            log = ctx.get_logger(SampleModel)
            log.info(f"The model will simulate {elapsed_days} day(s)")
