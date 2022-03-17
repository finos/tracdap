# TRAC Data & Analytics Platform

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

| Package                                                                  | Description                                                                |
|--------------------------------------------------------------------------|----------------------------------------------------------------------------|
| [Model runtime for Python](https://pypi.org/project/tracdap-runtime/)    | Build models and test them in a sandbox, ready to deploy to the platform   |
| [Web API package](https://www.npmjs.com/package/@finos/tracdap-web-api)  | Build client apps in JavaScript or TypeScript using the TRAC platform APIs |


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


The current release series (0.3.x) is intended for reference and experimentation.
It includes the metadata service, data service and core data engine, runtime engine
for Python and a partial implementation of the platform Gateway. This release can
be used to prototype client applications (e.g. web UIs) and to build and run models
in a development sandbox.

At the moment TRAC's API calls and metadata structures are subject to change between 
versions, including between point versions. As the platform evolves these APIs will
stabilise before eventually being frozen for TRAC version 1.0, after which they may 
be added to but not removed or changed.

For more information see the
[development roadmap](https://github.com/finos/tracdap/wiki/Development-Roadmap).

## Building TRAC

The core platform services require Java version 11 or higher, using LTS 11 or LTS 17
is recommended. Suitable JDKs are available from Azul and Adoptium:

* [Azul - Zulu](https://www.azul.com/downloads/?package=jdk)
* [Adoptium - Eclipse Temurin](https://adoptium.net/)

To build the TRAC platform, clone the repository and run this command
in the source directory:

    gradlew build     # Windows
    ./gradlew build   # Linux or macOS
    
If you are behind a corporate firewall, you may need to use a web proxy and/or
point Gradle at a Nexus server hosted inside your network to download 
dependencies. The Gradle documentation explains how to declare a local mirror
in an init script, so that it can be used for all Gradle builds.

* [Gradle web proxy](https://docs.gradle.org/current/userguide/build_environment.html#sec:accessing_the_web_via_a_proxy)
* [Gradle init scripts](https://docs.gradle.org/current/userguide/init_scripts.html)
* [Gradle repo declarations](https://docs.gradle.org/current/userguide/declaring_repositories.html)


## Running TRAC

In order to run TRAC you will need to supply some configuration. There is a
sample "devlocal" configuration included in the "etc" folder which is set up
to use a local backend with data stored under build/run.

Before starting the TRAC services you will need to create a metadata database.
There is a tool provided that can deploy the schema and create tenants. To use
the "devlocal" setup the supplied config can be used as-is, this will deploy
the TRAC metadata schema into an H2 database file.

    gradlew :deploy-metadb:run --args="--config etc/trac-devlocal.yaml --task deploy_schema"
    gradlew :deploy-metadb:run --args="--config etc/trac-devlocal.yaml --task add_tenant:ACME_CORP"

Once you have a database prepared you can start the TRAC services.

    gradlew :tracdap-svc-meta:run --args="--config etc/trac-devlocal.yaml"
    gradlew :tracdap-svc-data:run --args="--config etc/trac-devlocal.yaml"
    gradlew :tracdap-gateway:run --args="--config etc/trac-devlocal-gateway.yaml"

To confirm the platform is working you can use the [example API calls](./examples/rest_calls)
with a REST client such as [Postman](https://www.postman.com/). For more information on the
platform APIs and how to use them to build applications, check out the
[application development section](https://tracdap.readthedocs.io/en/stable/app_dev)
in the online documentation.

## Contributing

1. Fork it (<https://github.com/finos/tracdap/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Read our [contribution guidelines](./CONTRIBUTING.md) and [Community Code of Conduct](https://www.finos.org/code-of-conduct)
4. Commit your changes (`git commit -am 'Add some fooBar'`)
5. Push to the branch (`git push origin feature/fooBar`)
6. Create a new Pull Request

_NOTE:_ Commits and pull requests to FINOS repositories will only be accepted from those contributors with an active, executed Individual Contributor License Agreement (ICLA) with FINOS OR who are covered under an existing and active Corporate Contribution License Agreement (CCLA) executed with FINOS. Commits from individuals not covered under an ICLA or CCLA will be flagged and blocked by the FINOS Clabot tool (or [EasyCLA](https://github.com/finos/community/blob/master/governance/Software-Projects/EasyCLA.md)). Please note that some CCLAs require individuals/employees to be explicitly named on the CCLA.

*Need an ICLA? Unsure if you are covered under an existing CCLA? Email [help@finos.org](mailto:help@finos.org)*

## License

Copyright 2022 Accenture Global Solutions Limited

Distributed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)
