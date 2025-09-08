<h1 align="center">

![tracdap](https://github.com/finos/tracdap/raw/main/doc/_images/tracmmp_horizontal_400.png)

</h1>

<p align="center">
  <a href="https://pypi.org/project/tracdap-ext-http"><img alt="PyPI Version" src="https://img.shields.io/pypi/v/tracdap-ext-http.svg?maxAge=3600" /></a>
  <a href="https://pypi.org/project/tracdap-ext-http"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/tracdap-ext-http.svg?maxAge=3600" /></a>
  <a href="https://github.com/finos/tracdap/actions/workflows/packaging.yaml?query=branch%3Amain"><img alt="Packaging status" src="https://github.com/finos/tracdap/actions/workflows/packaging.yaml/badge.svg?branch:main&workflow:CI" /></a>
  <a href="https://github.com/finos/tracdap/actions/workflows/compliance.yaml?query=branch%3Amain"><img alt="Compliance status" src="https://github.com/finos/tracdap/actions/workflows/compliance.yaml/badge.svg?branch:main&workflow:CI" /></a>
  <a href="https://community.finos.org/docs/governance/software-projects/stages/incubating/"><img alt="FINOS - Incubating" src="https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg" /></a>
</p>


# HTTP Extension for the TRAC Model Runtime

This extension allows TRAC models to make calls to external systems using HTTP and HTTPS.

- Make HTTP endpoints available to models, to use directly in model code
- Connection settings managed by TRAC for both local and deployed models
- Supports http.client and urllib3

Models that make external calls are not considered repeatable,
and will be flagged as not repeatable when they run on the TRAC platform.

This extension is a pre-release and will be finalized inTRAC 0.10.


## Installing

The HTTP extension can be installed with [pip](https://pip.pypa.io):

```shell
$ pip install tracdap-ext-http
```

The package has the following dependencies:

- tracdap-runtime (version 0.10.0-beta1 or later)
- urllib3 (optional, version 2.x)


## Using the http.client API

Here is a minimum working example of a TRAC model using the http.client API:

```python
import tracdap.rt.api as trac
import http.client as hc

class TestModel(trac.TracModel):

    # ... define parameters, inputs and outputs

    def define_resources(self):

        return {
            "github": trac.define_external_system("http", hc.HTTPSConnection)
        }

    def run_model(self, ctx: trac.TracContext):

        with ctx.get_external_system("github", hc.HTTPSConnection) as github:

            github.connect()
            github.request("GET", "finos/tracdap/refs/heads/main/README.md")

            response = github.getresponse()
            response_text = response.read().decode("utf-8")
            first_line = response_text.splitlines()[0]

            ctx.log.info(first_line)

if __name__ == '__main__':
    import tracdap.rt.launch as launch
    launch.launch_model(TestModel, "config/job_config.yaml", "config/sys_config.yaml")
```

To make this example work, you will need to add ``github`` as a resource in the system config file:

```yaml
resources:

  github:
    resourceType: EXTERNAL_SYSTEM
    protocol: http
    properties:
      host: raw.githubusercontent.com
      port: 443
      tls: true
```

The following configuration properties are supported:

- host, string, required
- port, int, optional
- tls, bool, default = true
- timeout, float, optional

Models using the client type ``HTTPSConnection`` will only work if tls = true is set in the configuration.
Models requesting ``HTTPConnection`` will work with tls = true or tls = false,
and will receive an ``HTTPSConnection`` if tls = true.


## Using the urllib3 API

Here is a minimum working example of a TRAC model using the urllib3 API.
In order to use this API, the urllib3 package must be installed.

```python
import tracdap.rt.api as trac
import urllib3

class TestModel(trac.TracModel):

    # ... define parameters, inputs and outputs

    def define_resources(self):

        return {
            "github": trac.define_external_system("http", urllib3.HTTPSConnectionPool)
        }

    def run_model(self, ctx: trac.TracContext):

        with ctx.get_external_system("github", urllib3.HTTPSConnectionPool, timeout=10.0) as github:

            response = github.request("GET", "/finos/tracdap/refs/heads/main/README.md")

            response_text = response.data.decode("utf-8")
            first_line = response_text.splitlines()[0]

            ctx.log.info(first_line)

if __name__ == '__main__':
    import tracdap.rt.launch as launch
    launch.launch_model(TestModel, "config/job_config.yaml", "config/sys_config.yaml")
```

The resource configuration for the urllib3 API is identical to the http.client API.
