<div align="center">

![TRAC the modern model platform](https://github.com/finos/tracdap/raw/main/doc/_images/tracmmp_horizontal_400.png)

  <br />

  <div>
    <a href="https://pypi.org/project/tracdap-runtime"><img alt="PyPI Version" src="https://img.shields.io/pypi/v/tracdap-runtime.svg?maxAge=3600" /></a>
    <a href="https://pypi.org/project/tracdap-runtime"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/tracdap-runtime.svg?maxAge=3600" /></a>
    <a href="https://github.com/finos/tracdap/actions/workflows/packaging.yaml?query=branch%3Amain"><img alt="Packaging status" src="https://github.com/finos/tracdap/actions/workflows/packaging.yaml/badge.svg?branch:main&workflow:CI" /></a>
    <a href="https://github.com/finos/tracdap/actions/workflows/compliance.yaml?query=branch%3Amain"><img alt="Compliance status" src="https://github.com/finos/tracdap/actions/workflows/compliance.yaml/badge.svg?branch:main&workflow:CI" /></a>
    <a href="https://github.com/pandas-dev/pandas/blob/main/LICENSE"><img alt="License - Apache-2.0" src="https://img.shields.io/pypi/l/tracdap-runtime.svg" /></a>
    <a href="https://community.finos.org/docs/governance/software-projects/stages/incubating/"><img alt="FINOS - Incubating" src="https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg" /></a>
  </div>

  <p>
    <a href="https://www.fintrac.co.uk/">Homepage</a>
    ◆ <a href="https://docs.fintrac.co.uk/versions/latest/modelling">Documentation</a>
    ◆ <a href="https://github.com/fintrac-hub/examples">Examples</a>
    ◆ <a href="https://github.com/finos/tracdap">Source Code</a>
    ◆ <a href="https://github.com/finos/tracdap/issues">Issue Tracker</a>
  </p>
</div>


# TRAC Model Runtime for Python

The TRAC Model Runtime is a lightweight package for building portable,
production-grade Python models. Models created with the runtime can be
executed anywhere, from local development environments to enterprise
production systems. The runtime can be used independently of the broader
TRAC platform and provides a simple framework in which to build, ship,
and share models.

Each model defines its parameters, inputs and outputs, and the runtime
provides the execution context, ensuring that models always receive valid
data and parameters. Taking responsibility for data access, marshalling
and formatting out of the model code allows developers to focus on model
logic, safe in the knowledge that the model will behave consistently
across environments.

For a complete guide to writing models in the TRAC framework, see the
[online documentation](https://docs.fintrac.co.uk/versions/latest/modelling).


## 📦 Requirements

* **Python :**  3.10 or later
* **Tools :**   Any popular IDE (PyCharm, VS Code, etc.)
* **Pandas :**  Version 1.2 and later are supported
* **Polars :**  Version 1.0 and later are supported

*Some 3rd party libraries may have additional version constraints,
for example, Pandas 1.5 is not available for Python 3.12 or later.*


## 🚀 Quick start

TRAC can be added to a new or existing Python project using [pip](https://pip.pypa.io):

```shell
$ pip install tracdap-runtime
```

You will also need to install a data framework (Pandas or Polars) and any other libraries that the model uses.

Here is a minimum working example of a TRAC model that performs aggregation on a dataset:

```python
import tracdap.rt.api as trac

class QuickStartModel(trac.TracModel):

    def define_parameters(self):

        # Define any parameters the model will use
        return trac.define_parameters(
            trac.P("exchange_rate", trac.FLOAT, "EUR / GBP exchange rate")
        )

    def define_inputs(self):

        # Define an input table with the columns and data types that the model needs
        customer_loans = trac.define_input_table(
            trac.F("id", trac.STRING, label="Customer account ID", business_key=True),
            trac.F("region", trac.STRING, label="Customer home region", categorical=True),
            trac.F("loan_amount", trac.FLOAT, label="Principal loan amount (EUR)"),
            label="Customer loans data")

        return {"customer_loans": customer_loans}

    def define_outputs(self):

        # Define an output table with the columns and data types that the model will produce
        loans_by_region = trac.define_output_table(
            trac.F("region", trac.STRING, label="Customer home region", categorical=True),
            trac.F("total_lending", trac.FLOAT, label="Total lending (GBP)"),
            label="Loans by region")

        return {"loans_by_region": loans_by_region}

    def run_model(self, ctx: trac.TracContext):

        # Parameters and inputs are loaded and validated by TRAC
        exchange_rate = ctx.get_parameter("exchange_rate")
        customer_loans = ctx.get_pandas_table("customer_loans")

        # Model code is regular Python
        customer_loans["loan_amount_gbp"] = customer_loans["loan_amount"] * exchange_rate

        loans_by_region = customer_loans \
            .groupby("region", observed=True, as_index=False) \
            .aggregate(total_lending=("loan_amount_gbp", "sum"))

        # Logs written to ctx.log are captured by the platform
        ctx.log().info("Aggregated loans for %d regions", len(loans_by_region))

        # Outputs are handed back to TRAC for validation and saving
        ctx.put_pandas_table("loans_by_region", loans_by_region)

# Use the desktop launcher to run, test and debug models locally
if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(QuickStartModel, "config/quick_start.yaml", "config/sys_config.yaml")
```

The model needs two config files in order to run: A job config file and a system config file.
By convention, for local development these are kept in a ``config`` folder.

The system config file defines the resources that are available for models to use.
As a minimum the default storage location must be specified.

*sys_config.yaml*
```yaml
properties:

  storage.default.location: example_data
  storage.default.format: CSV

resources:

  example_data:
    resourceType: INTERNAL_STORAGE
    protocol: LOCAL
    properties:
      rootPath: C:\path\to\your\data
```

The job config file supplies the model with the parameters, inputs and outputs that it needs to run.
The default data location from ``sys_config.yaml`` is used to load input data and save output data.
TRAC checks the types and schemas of every parameter and input dataset, types are cast automatically
where it is safe to do so, otherwise TRAC will raise an error if the inputs are not valid.

*quick_start.yaml*
```yaml
job:
  runModel:
    
    parameters:
      exchange_rate: 0.865
    
    inputs:
      customer_loans: "inputs/customer_loans_data.csv"

    outputs:
      loans_by_region: "outputs/example_model/loans_by_region.csv"
```

You can run the model from your IDE (right click the model file and choose "Run",
or look for the "Play" button). You will see the model logs, and an output file
will be created inside your data folder.


## 📖 Documentation

See the [online documentation](https://docs.fintrac.co.uk/versions/latest/modelling)
for a complete guide to writing models in the TRAC framework,
including tutorials and an API reference.


## ✋ Contributing

If you'd like to contribute a feature or a fix then we'd love to hear from you!
Please raise an issue on our [issue tracker](https://github.com/finos/tracdap/issues)
to discuss your suggestion before working on a PR.

Contributions are governed according to the [contribution guidelines](https://github.com/finos/tracdap/blob/main/CONTRIBUTING.md)
and the [FINOS code of conduct](https://www.finos.org/code-of-conduct).


## 📜 License

The TRAC model runtime is maintained by [finTRAC Ltd](https://fintrac.co.uk/) in association with
the [Fintech Open Source Foundation](https://www.finos.org/) (FINOS) and distributed under the terms of
the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)

For more information including copyright history,
see the [NOTICE](https://github.com/finos/tracdap/blob/main/NOTICE) file.


## 🏢 Enterprise

Professional support for TRAC is available from [finTRAC Ltd](https://fintrac.co.uk/),
for more information please [contact us](https://fintrac.co.uk/contact).
