# ![TRAC Data & Analytics Platform](doc/_images/tracdap_horizontal_400.png)

*A next-generation data and analytics platform for use in highly regulated environments*

[![FINOS - Incubating](https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg)](https://finosfoundation.atlassian.net/wiki/display/FINOS/Incubating)

TRAC D.A.P. brings a step change in performance, insight, flexibility and control 
compared to conventional analytics platforms. By redrawing the boundary
between business and technology, modellers and business users are given easy
access to modern, open source tools that can execute at scale, while technology
integrations and operational concerns are cleanly separated and consolidated
across use cases.

At the core of a platform, a flexible metadata model allows data and models to
be catalogued, plugged together and shared across the business. Using the
principal of immutability, TRAC allows new data structures and model pipelines
to be created, updated and executed at any time without change risk to production
workflows, guaranteeing total repeatability, audit and control (TRAC).


## Documentation and Packages

Documentation for the TRAC platform is available on our website at
[tracdap.finos.org](https://tracdap.finos.org).

The following packages are available:

| Package                                                                  | Description                                                                                           |
|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| [Model runtime for Python](https://pypi.org/project/tracdap-runtime/)    | Build models and test them in a sandbox, ready to deploy to the platform                              |
| [Web API package](https://www.npmjs.com/package/@finos/tracdap-web-api)  | Build client apps in JavaScript or TypeScript using the TRAC platform APIs                            |
| [Platform releases](https://github.com/finos/tracdap/releases)           | Packages for the platform services and a standalone sandbox are published with each release on GitHub |


## Development Status

[![Build and Test](https://github.com/finos/tracdap/actions/workflows/build.yml/badge.svg)](
https://github.com/finos/tracdap/actions/workflows/build.yml)
[![Integration](https://github.com/finos/tracdap/actions/workflows/integration.yml/badge.svg)](
https://github.com/finos/tracdap/actions/workflows/integration.yml)
[![Compliance](https://github.com/finos/tracdap/actions/workflows/compliance.yml/badge.svg)](
https://github.com/finos/tracdap/actions/workflows/compliance.yml)
[![Packaging](https://github.com/finos/tracdap/actions/workflows/packaging.yml/badge.svg)](
https://github.com/finos/tracdap/actions/workflows/packaging.yml)
[![Documentation Status](https://readthedocs.org/projects/tracdap/badge/?version=stable)](
https://tracdap.finos.org/en/stable/?badge=stable)


The current release series (0.4.x) is intended for model development and prototyping.
It provides an end-to-end workflow to build and run individual models in a local
environment. It also provides the platform APIs needed to build client applications
such as web tools or system client system integrations.

The TRAC metadata structures and API calls are mostly complete. Metadata compatibility
is ensured within a release series starting from version 0.4.0 - the 0.4.x series
will be compatible with 0.4.0 but changes may be introduced in 0.5.0. The metadata
model will continue to stabilise before eventually being frozen for version 1.0.0,
after which it may be added to but no fields will be removed or changed.

For more information see the
[development roadmap](https://github.com/finos/tracdap/wiki/Development-Roadmap).

## Building models

With TRAC D.A.P. you can build and run production-ready models right on your desktop!
All you need is an IDE, Python  and the tracdap-runtime Python package.
TRAC D.A.P. requires Python 3.7 or later.

The [modelling tutorial](https://tracdap.finos.org/en/stable/modelling/tutorial/chapter_1_hello_world.html)
shows you how to get set up and write your first models. You can write models locally using
an IDE or notebook, once the model is working t can be loaded to the platform without modification.
TRAC D.A.P. will validate the model and ensure it behaves the same on-platform as it does locally.
Of course, the production platform will allow for significantly greater data volumes and compute power!

A full listing of the modelling API is available in the
[model API reference](https://tracdap.finos.org/en/stable/autoapi/tracdap/rt/index.html).

## Running the platform

TRAC D.A.P. is designed for easy installation in complex and controlled enterprise environments.
The tracdap-platform package is available with each release on our
[release page](https://github.com/finos/tracdap/releases) and includes a pre-built distribution
of each  of the platform services and supporting tools, suitable for deploying in a container
or on physical or virtual infrastructure. All the packages are platform-agnostic. 

A sandbox version of the platform is also available for quick setup in development, testing and demo
scenarios. The tracdap-sandbox package is available with each release on our
[release page](https://github.com/finos/tracdap/releases) and instructions are available in the
[sandbox quick start guide](https://tracdap.finos.org/en/stable/deployment/sandbox.html)
in our documentation.

## Development

We have used the excellent tools from [JetBrains](https://www.jetbrains.com) to build TRAC D.A.P.
After you fork and clone the repository you can open the project in IntelliJ IDEA and use the script
dev/ide/copy_settings.sh (Linux/macOS) or dev\ide\copy_settings.bat (Windows) to set up some helpful IDE
config, including modules for the non-Java components, run configurations, license templates etc. 
If you prefer another IDE that is also fine, you may wish to set up a similar set of config in which case
we would welcome a PR.

If you need help getting set up to develop features for TRAC D.A.P., please
[get in touch](https://github.com/finos/tracdap/issues)!


## Contributing

1. Fork it (<https://github.com/finos/tracdap/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Read our [contribution guidelines](./CONTRIBUTING.md) and [Community Code of Conduct](https://www.finos.org/code-of-conduct)
4. Commit your changes (`git commit -am 'Add some fooBar'`)
5. Push to the branch (`git push origin feature/fooBar`)
6. Create a new Pull Request

_NOTE:_ Commits and pull requests to FINOS repositories will only be accepted from those contributors with an active, executed Individual Contributor License Agreement (ICLA) with FINOS OR who are covered under an existing and active Corporate Contribution License Agreement (CCLA) executed with FINOS. Commits from individuals not covered under an ICLA or CCLA will be flagged and blocked by the FINOS Clabot tool (or [EasyCLA](https://community.finos.org/docs/governance/Software-Projects/easycla)). Please note that some CCLAs require individuals/employees to be explicitly named on the CCLA.

*Need an ICLA? Unsure if you are covered under an existing CCLA? Email [help@finos.org](mailto:help@finos.org)*

## License

Copyright 2022 Accenture Global Solutions Limited

Distributed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)
