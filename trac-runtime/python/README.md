# TRAC Model Runtime for Python

*A next-generation data and analytics platform for use in highly regulated environments*

The TRAC model runtime provides all the APIs needed to write models for the TRAC platform.
It includes an implementation of the runtime library that can be used as a development
sandbox, so you can run and debug TRAC models right away from your favourite IDE or notebook.
A number of tools are included to make it easy to plug in development data and other settings.
When your models are ready they can be loaded into a real instance of TRAC for testing and
eventual deployment.

Documentation for the TRAC platform is available at
[trac-platform.readthedocs.io](https://trac-platform.readthedocs.io).

## Requirements

The TRAC runtime for Python has these requirements:

* Python: 3.7 or later
* Pandas: 1.0 or later
* PySpark 2.4.x, 3.0.x or 3.1.x

Not every combination of versions will work, e.g. PySpark 3 requires Python 3.8.


## Writing TRAC Models

The TRAC runtime package can be installed directly from PyPI:

    pip install trac-runtime

The TRAC runtime depends on Pandas and PySpark, so these libraries will be pulled in as 
dependencies. If you want to target particular versions, install them explicitly first.

Once the runtime is installed you can write your first TRAC model! Start by
inheriting the TracModel base class, your IDE should be able to generate stubs for you:

    import trac.rt.api as trac

    class SampleModel(trac.TracModel):

        def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
            pass

        def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
            pass

        def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
            pass

        def run_model(self, ctx: trac.TracContext):
            pass

You can fill in the three define_* methods to declare any parameters, inputs and outputs your
model is going to need, then start writing your model code in run_model.

To learn about modelling with TRAC and what is possible, check out the
[modelling tutorials](https://trac-platform.readthedocs.io/en/stable/modelling/tutorial)
available in our online documentation. The tutorials are based on
[example models](https://github.com/Accenture/trac/tree/main/examples/models/python)
in the TRAC GitHub repository. We run these examples as part of our CI, so they will always
be in sync with the corresponding version of the runtime library.
