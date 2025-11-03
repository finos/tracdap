<div align="center">

![TRAC the modern model platform](https://github.com/finos/tracdap/raw/main/doc/_images/tracmmp_horizontal_400.png)

  <br />

  <div>
    <a href="https://pypi.org/project/tracdap-ext-pypi"><img alt="PyPI Version" src="https://img.shields.io/pypi/v/tracdap-ext-pypi.svg?maxAge=3600" /></a>
    <a href="https://pypi.org/project/tracdap-ext-pypi"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/tracdap-ext-pypi.svg?maxAge=3600" /></a>
    <a href="https://github.com/finos/tracdap/actions/workflows/packaging.yaml?query=branch%3Amain"><img alt="Packaging status" src="https://github.com/finos/tracdap/actions/workflows/packaging.yaml/badge.svg?branch:main&workflow:CI" /></a>
    <a href="https://github.com/finos/tracdap/actions/workflows/compliance.yaml?query=branch%3Amain"><img alt="Compliance status" src="https://github.com/finos/tracdap/actions/workflows/compliance.yaml/badge.svg?branch:main&workflow:CI" /></a>
    <a href="https://github.com/finos/tracdap/blob/main/LICENSE"><img alt="License - Apache-2.0" src="https://img.shields.io/pypi/l/tracdap-ext-pypi.svg" /></a>
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


# PyPI Extension for the TRAC Model Runtime

This extension allows TRAC to load models dynamically from PyPI repositories,
or a compatible proxy such as Nexus or Artifactory.

- Support for JSON, Simple JSON and Simple HTML query formats
- Support for private repositories using tokens
- Handles corporate firewalls, proxies and SSL

This extension is a pre-release and will be finalized inTRAC 0.10.



## Installing

The HTTP extension can be installed with [pip](https://pip.pypa.io):

```shell
$ pip install tracdap-ext-pypi
```

The package has the following dependencies:

- tracdap-runtime (version 0.10.0-beta2 or later)
- urllib3 (optional, version 2.x)



## Using the Git model repository

With the PyPI extension installed, you can add PyPI repositories as resources in sys_config.yaml like this:

```yaml
resources:
  
  my_json_repository:
    resourceType: MODEL_REPOSITORY
    protocol: pypi
    properties:
      pipIndex: https://artifactory.mycompany.com/artifactory/api/pypi

  my_simple_repository:
    resourceType: MODEL_REPOSITORY
    protocol: pypi
    properties:
      pipIndexUrl: https://nexus.mycompany.com/repository/pypi/simple
      pipSimpleFormat: html
```

The examples show the use of both the JSON and Simple query formats. The particular query formats
available and the paths for the index and index-url properties will depend on how your package
server is configured. This information is often published along with other internal developer
documentation to help set up a development environment (Look for the "index" or "index-url"
settings when setting up other Python tools). Sometimes is is also visible in the web interface
of the package server. If you can't find the settings you will need to contact your dev-ops team.

For the simple query format (i.e. an index configured with the "index-url" setting), there are two possible
response formats: `pipSimpleFormat: html` or `pipSimpleFormat: json`. Use this setting to see what
is available on your package server. For the JSON query format (i.e. an index configured with the
"index" setting) there is only one response format available so this is not configurable.

To supply credentials, use either the `username` and `password` properties, or the `token` property.
In production deployments these properties should be supplied using the TRAC secrets mechanism.

This extension assumes that packages on the package server are available as Python wheels.
