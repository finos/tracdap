# Docker and docker-compose

This directory contains dockerfiles, which can be used to deploy TRAC D.A.P.

They can be used to setup [sandbox](https://tracdap.finos.org/en/stable/deployment/sandbox.html).

**Note:** All commands are run *from this directory*: 

```
cd dev/docker
```

## Distribution directory

Directory `tracdap-platform` must have install-ready distribution of TRAC D.A.P.
The directory should have `tracdap-svc-data`, `tracdap-svc-meta` and other services.

There are two ways to prepare the directory.

**Released version**

Download `tracdap-platform-X.Y.Z.tgz` from [releases on GitHub](https://github.com/finos/tracdap/releases).

Extract archive contents to the new directory `tracdap-platform`.

**Development version**

You can prepare your development version.

```
# these commands are run from the root of the project
cd ../..

# build install distribution
./gradlew installDist

# copy binaries to tracdap-platform
mkdir dev/docker/tracdap-platform
cp -r build/modules/*/install/* dev/docker/tracdap-platform

# go back to the docker directory
cd dev/docker
```

## Plugins

You can add plugins to `tracdap-platform/*/plugins` directories.

Example with H2 database plugin:

```
# download the plugin
curl -LO "https://search.maven.org/remotecontent?filepath=com/h2database/h2/2.1.214/h2-2.1.214.jar"

cp h2-2.1.214.jar tracdap-platform/tracdap-svc-meta/plugins
cp h2-2.1.214.jar tracdap-platform/deploy-metadb/plugins

rm h2-2.1.214.jar
```

## Config

By convention `etc` directory should be mounted:

```
docker \
    --mount "type=bind,source=$(pwd)/etc,target=/opt/trac/current/etc,readonly" \
    ...
```

`etc` should NOT be part of any docker image.

## Build

You can build docker images for each service, example:

```
docker build . -f tracdap-svc-meta.Dockerfile -t tracdap-svc-meta 
```

## Setup

These containers can be used to setup an environment
(database setup and JKS secret key creation), for instance:

```
# create database
docker run --rm \
    --user "$(id -u):$(id -g)" \
    --mount "type=bind,source=$(pwd)/etc,target=/opt/trac/current/etc,readonly" \
    --mount "type=bind,source=$(pwd)/state/metadata,target=/opt/trac/metadata" \
    --mount "type=tmpfs,target=/opt/trac/current/run,tmpfs-mode=1777" \
    deploy-metadb --task deploy_schema
```

## Run

The whole system can be run with [docker-compose](https://docs.docker.com/compose/):

```
UID=${UID} GID=${GID} docker compose up
```

Do not forget to setup the database and JKS secret key.

**Note:** This `docker-compose.yaml` is for development purposes, NOT for production.