<div align="center">

![TRAC the modern model platform](https://github.com/finos/tracdap/raw/main/doc/_images/tracmmp_horizontal_400.png)

  <br />

  <div>
    <a href="https://pypi.org/project/tracdap-runtime"><img alt="PyPI Version" src="https://img.shields.io/pypi/v/tracdap-ext-git.svg?maxAge=3600" /></a>
    <a href="https://pypi.org/project/tracdap-runtime"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/tracdap-ext-git.svg?maxAge=3600" /></a>
    <a href="https://github.com/finos/tracdap/actions/workflows/packaging.yaml?query=branch%3Amain"><img alt="Packaging status" src="https://github.com/finos/tracdap/actions/workflows/packaging.yaml/badge.svg?branch:main&workflow:CI" /></a>
    <a href="https://github.com/finos/tracdap/actions/workflows/compliance.yaml?query=branch%3Amain"><img alt="Compliance status" src="https://github.com/finos/tracdap/actions/workflows/compliance.yaml/badge.svg?branch:main&workflow:CI" /></a>
    <a href="https://github.com/finos/tracdap/blob/main/LICENSE"><img alt="License - Apache-2.0" src="https://img.shields.io/pypi/l/tracdap-ext-git.svg" /></a>
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


# Git Extension for the TRAC Model Runtime

This extension allows TRAC to load models dynamically from Git repositories.

- Includes a pure-Python implementation using the excellent [dulwich](https://pypi.org/project/dulwich/) library
- Includes a native Git implementation that wraps a Git binaries on the host system
- Support for private repositories using tokens
- Handles corporate firewalls, proxies and SSL

This extension is a pre-release and will be finalized inTRAC 0.10.


## Installing

The HTTP extension can be installed with [pip](https://pip.pypa.io):

```shell
$ pip install tracdap-ext-git
```

The package has the following dependencies:

- tracdap-runtime (version 0.10.0-beta2 or later)
- dulwich (0.24.x series)
- urllib3 (version 2.2 or later)


## Using the Git model repository

With the Git extension installed, you can add a Git model repository as a resource in sys_config.yaml like this:

```yaml
resources:

  my_repository:
    resourceType: MODEL_REPOSITORY
    protocol: git
    properties:
      repoUrl: https://github.com/a-user/my-repository
```

By default the extension will use the pure-python implementation of Git, to use native Git binaries set
`nativeGit: true` as a property on the repository. To supply credentials, use either the `username` and
`password` properties, or the `token` property. In production deployments these properties should be
supplied using the TRAC secrets mechanism.

Using the pure-Python implementation, all of TRAC's network properties are respected.
In particular, to supply an enterprise root trust certificate you can set
`network.ssl.caCertificates` to the path of a certificate in PEM format.
Setting `network.ssl.publicCertificates: true` will  include the global, public CAs
in addition to the enterprise root.

All properties starting with the prefix `git.` are treated as Git configuration and
written into the Git config file for the repository when it is checked out. For example,
setting `git.http.sslCAInfo` will set the `sslCAInfo` property in the `http` section of the
Git configuration. This is an alternate way of specifying SSL details that works with the
native Git implementation, but can also be used to set arbitrary Git configuration values
with either implementation. Some Git config values may not be respected by Dulwich, so it
is recommended to use the TRAC config settings with the pure-Python implementation and
native Git config settings with the native Git implementation.
