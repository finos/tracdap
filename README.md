# TRAC

*A next-generation data and analytics platform for use in highly regulated environments*


The TRAC platform offers dramatic improvements in performance, insight,
flexibility and control compared to conventional analytics platforms. By 
redefining the boundary between business and technology, modellers and
business users are given easy access to modern, open source tools that can
execute at scale, while technology integrations and operational concerns are
cleanly separated and consolidated across use cases.

TRAC is designed to work in complex environments with ever-changing regulation.
At the core of a platform, a flexible metadata model allows data and models to
be catalogued, plugged together and shared across the business. Using a simple
model of immutability it remembers every action that happens on the platform,
guaranteeing total repeatability, audit and control (TRAC).


## Development Status

The current release series (0.1.x) is intended for reference and experimentation.
It includes an implementation of the metadata API, which can be used to
prototype client applications (e.g. web UIs) against the TRAC platform.

For the time being, TRAC's API calls and metadata structures are subject to
change between versions, including between point versions. As the platform
evolves these APIs and metadata structures will stabilise eventually forming
a contract for the behaviour of the platform across versions. The expectation
is that the APIs will be frozen for TRAC version 1.0, after which they may
be added to but not removed or changed.


## Building TRAC

TRAC requires Java 11 or higher, you can download a suitable JDK from
[AdoptOpenJDK](https://adoptopenjdk.net/).

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

    gradlew :deploy-metadb:run --args="--config etc/trac-devlocal.properties --task deploy_schema"
    gradlew :deploy-metadb:run --args="--config etc/trac-devlocal.properties --task add_tenant:ACME_CORP"

Once you have a database prepared you can start the TRAC services.

    gradlew :trac-svc-meta:run --args="--config etc/trac-devlocal.properties"
    gradlew :trac-gateway:run --args="--config etc/trac-devlocal-gw.properties"


## Using the TRAC APIs

TRAC version 0.1 provides the TRAC web API, which can be used for developing
user-facing applications on the TRAC platform. This API is available to be
consumed in both gRPC / proto (via the metadata service) and REST / json (via
the platform gateway).

You can use a regular REST client such as [Postman](https://www.postman.com/)
to experiment with the REST API.


## Contributing

