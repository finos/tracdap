

Sandbox Quick Start
===================


The TRAC sandbox is a self-contained distribution of TRAC D.A.P. that can run on a single machine.
It includes all the functionality of the platform in one ready-to-use bundle.
Of course, it doesn't have the power and scalability available with a cloud deployment or physical cluster.
Technology controls are not enforced, so developers with access to the server can delete or modify data,
or change the deployment entirely.

The sandbox is useful for development, testing, PoCs and small-scale demo scenarios. It can be used to
develop and test end-to-end processes, including models, model chains, data preparation, applications / UIs and
control / sign-off workflows. All these items should work without modification  when deployed to a real production
instance of TRAC *(subject to capacity constraints and assuming models are coded in line with TRAC conventions)*.


Downloading
-----------

The TRAC platform sandbox is available to download on the GitHub release page:

* https://github.com/finos/tracdap/releases

You want the tracdap-sandbox zip file, the package will work on Linux, macOS and Windows.
Unless you have a good reason, always use the latest stable release.


Deploying the sandbox
---------------------

**Java**

TRAC requires Java version 11 or higher, using LTS version 11, 17 or 21 is recommended.
Suitable JDKs are available from
`Azul (Zulu) <https://www.azul.com/downloads/?package=jdk>`_ and
`Adoptium (Eclipse Temurin) <https://adoptium.net/>`_.
If your company restricts software deployment, it is fine to use the Java package they provide
so long as it is version 11 or higher.

**Python**

TRAC requires Python 3.8 or higher, which can be downloaded and installed from
`python.org <https://www.python.org/downloads/>`_.
If your company restricts software deployment, it is fine to use the Python package they provide
so long as it is version 3.8 or higher.

**TRAC Services**

Start by creating a directory for your TRAC installation and unzipping the sandbox distribution.

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            mkdir /opt/trac
            cd /opt/trac
            unzip path/to/downloads/tracdap-sandbox-<version>

            # Having a "current" link makes it easy to upgrade and switch versions later
            ln -s tracdap-sandbox-<version> current

    .. tab-item:: Windows
        :sync: platform_windows

        Create a directory *C:\\trac* then unzip the TRAC sandbox zip in this folder.

You'll also need to create directories for storing both data and metadata. These should be outside of the
versioned deployment folder, because you want to keep them when you install newer versions of the platform.

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            mkdir /opt/trac/data
            mkdir /opt/trac/metadata

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            mkdir C:\trac\data
            mkdir C:\trac\metadata

**Database driver**

The metadata service requires a JDBC database driver, which must be copied as a JAR file into the
*plugins* folder of the TRAC installation. To use the H2 embedded database, you can download the H2
JDBC JAR from Maven Central:

* https://search.maven.org/artifact/com.h2database/h2

If you are behind a corporate firewall, you may need to use a web proxy and/or
point at a Nexus server hosted inside your network.

**VENV for model execution**

You will need to set up a Python VENV with the TRAC runtime library installed,
which will be used for executing models. If you have multiple versions of Python installed,
make sure to use the right one to create your venv.

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            cd /opt/trac/trac-platform-<version>
            python -m venv ./venv
            . venv/bin/activate

            pip install "tracdap-runtime == <version>"

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            cd C:\trac\trac-platform-<version>
            python -m venv .\venv
            venv\Scripts\activate

            pip install "tracdap-runtime == <version>"

The pip install command will download the TRAC runtime package for Python from PyPi.
If you are behind a corporate firewall, you may need to use a web proxy and/or
point at a Nexus server hosted inside your network.


Quick configuration
-------------------

The sandbox deployment comes with some example configuration to use as a starting point. Below is a
quick walk through of the major sections and what they all do, for a standalone sandbox most of these
settings can be left as they are, with just the locations of key resources to be filled in. Before editing
these configuration files, it is recommended to take a copy as a backup, in case you need to refer to it
later.

**Platform configuration**

The config section refers to other configuration files or sources. The example config includes a logging
config file and a local secret store using the PKCS12 format. We'll set up the secret store later using
the TRAC setup tools.

.. literalinclude:: ../../dist/template/etc/trac-platform.yaml
    :caption: etc/trac-platform.yaml
    :name: trac_platform_yaml_config
    :language: yaml
    :lines: 15 - 18

.. note::
    Relative file names or URLs in the config section will be taken relative to the main config file.

The *platformInfo* section allows you to specify some information about the TRAC environment.
This is made available to clients and can be shown in UIs and client apps, so users know which
environment they are connected to.

.. literalinclude:: ../../dist/template/etc/trac-platform.yaml
    :name: trac_platform_yaml_platform_info
    :language: yaml
    :lines: 21 - 23

The *authentication* section controls the platform's internal authentication mechanism, which uses JWT.
This is to allow the platform to validate user tokens and to let different platform services talk to each other.

The task of actually logging users in and obtaining their details is handled by the gateway component, so the
configuration for this is in the gateway config file. The JWT details supplied here need to match what is in
the gateway config file.

.. literalinclude:: ../../dist/template/etc/trac-platform.yaml
    :name: trac_platform_yaml_authentication
    :language: yaml
    :lines: 26 - 29

The *metadata* section describes how and where the TRAC metadata will be stored. The current implementation
uses SQL to store metadata and several common SQL dialects are supported. The default sandbox config uses the
H2 embedded database for the simplest possible setup, you just need to add the path for the metadata folder
created above. For examples of how to configure other SQL dialects, see the deployment docs for the
:doc:`metadata store </deployment/metadata_store>`.


.. literalinclude:: ../../dist/template/etc/trac-platform.yaml
    :name: trac_platform_yaml_metadata
    :language: yaml
    :lines: 32 - 44

.. note::
    H2 is mostly used in development scenarios where the password is not sensitive.
    If you want to use a secret for the H2 password, see :doc:`secrets`.

The *storage* section allows you to configure one or more storage buckets to hold primary data. In TRAC,
a "bucket" is any storage location that can hold files, which could be a cloud storage bucket on a cloud
platform but can also be a local folder. Other protocols such as network storage or HDFS can also be
supported with the appropriate storage plugins.

The *defaultBucket* and *defaultFormat* settings tell TRAC where to store new data by default. These defaults
can be changed later, data that is already written will be picked up using the correct location and format.

The sample configuration contains one storage bucket, you just need to specify a path.

.. literalinclude:: ../../dist/template/etc/trac-platform.yaml
    :name: trac_platform_yaml_storage
    :language: yaml
    :lines: 47 - 58

.. note::
    Pay particular attention to the bucket key, which is *STORAGE1* in this example.
    The bucket key is a unique identifier for a storage location, you may want to give it
    a meaningful name, for example relating to your project or business division.

The repositories section let's you configure model repositories, that TRAC will access to load
models into the platform. The sample config includes the TRAC repository as an example, you
should replace this with your own model repository and choose a meaningful repository key.
You can add multiple repositories if required, so long as each one has a unique key.

.. literalinclude:: ../../dist/template/etc/trac-platform.yaml
    :name: trac_platform_yaml_repositories
    :language: yaml
    :lines: 61 - 67

The last thing you need to add in the platform config is an executor. The example config is already set up
with a local executor, so you just need to add the path for the VENV you built in the deployment step.

.. literalinclude:: ../../dist/template/etc/trac-platform.yaml
    :name: trac_platform_yaml_executor
    :language: yaml
    :lines: 69 - 73

There are two more sections in the platform config, *services* and *instances*. The *services* section sets
up the service properties for each service, most importantly the ports they should run on. The *instances*
section lists the instances of each service. For a sandbox setup there is no need to alter these sections.


**Gateway configuration**

The gateway example config will work without alteration to serve the API endpoints for the TRAC services.
However, the gateway can also be used to route requests for client applications; this is particularly
useful for web applications in a dev / test scenario, because it provides a direct route to access the TRAC
API and avoids CORS issues. If you want to use this capability, look in the gateway config and you will find
an example of setting up an additional HTTP route. You can add as many HTTP routes as you need.

The example configuration uses *guest* authentication, which logs all users in as "guest" without requiring
a password. To set up real user logins, see the deployment docs for :doc:`authentication </deployment/authentication>`.

**Logging**

Logging is provided using log4j, the example configuration writes to the local log/ directory by default.


**Environment**

Environment variables can be specified in the shell before launching the TRAC services. Alternatively,
an environment file is available for both Linux / macOS (env.sh) and Windows (env.bat). These can be
useful for specifying system settings, such as JAVA_HOME to select a particular installation of Java,
or JAVA_OPTS to control the JVM memory parameters. You can also control some of the TRAC options here,
e.g. setting CONFIG_FILE will tell trac to load a different root config file.

For sandbox setups, the main variable to set in this file is *TRAC_SECRET_KEY*. This is the master key for
the TRAC secret store, that unlocks all the other secrets in the configuration. In production setups
this key should not be stored in a file, but passed in through the environment using a scheduling tool,
or as part of a containerized job setup.

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell
            :caption: etc/env.sh

            TRAC_SECRET_KEY=a_very_secret_password

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch
            :caption: etc\\env.bat

            set TRAC_SECRET_KEY=a_very_secret_password


Setup tools
-----------

TRAC D.A.P. comes with a few tools to simplify the deployment. There are two we need to use for a
sandbox setup, *secret-tool* and *deploy-metadb*.

**Secret Tool**

The *secret-tool* utility is used to manage secrets, certificates and other sensitive configuration.
It can also be used to manage users if you are using a local user database. The tool will write secrets
to the secret store configured in the platform configuration. If this is a local keystore file and it
does not exist then it will be created. Make sure you have set the *TRAC_SECRET_KEY* environment variable
before using *secret-tool*.

For the sandbox setup we need a minimum of one secret, the root authentication key.
This key is used by TRAC to sign and verify its internal JWT tokens.
The available key types for the root authentication key are elliptic curve (EC) or RSA.
Elliptic curve keys are considered to give better security with better performance at lower key sizes,
so for this reason we recommended EC 256 keys.

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            cd /opt/trac/current
            bin/secret-tool run --task init_secrets
            bin/secret-tool run --task create_root_auth_key EC 256

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            cd /d C:\trac\tracdap-sandbox-<version>
            bin\secret-tool.bat run --task init_secrets
            bin\secret-tool.bat run --task create_root_auth_key EC 256

.. note::
    Running the *create_root_auth_key* command a second time will replace the root authentication key,
    which will invalidate any existing JWT tokens.


**Deploy MetaDB**

TRAC D.A.P. comes with a tool to help deploy the metadata database. It runs off the same configuration as
the platform services, so make sure to finish updating your configuration before running the tool.

We need to perform two tasks to prepare the database: deploy the schema and create a tenant. Choose a
tenant code and description that is meaningful for your project or business division. The description
can be altered later but the tenant code cannot.

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            cd /opt/trac/current
            bin/deploy-metadb run --task deploy_schema
            bin/deploy-metadb run --task add_tenant ACME_CORP "ACME Supplies Inc."

            bin/deploy-metadb run --task alter_tenant ACME_CORP "ACME Mega Supplies Inc."

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            cd /d C:\trac\tracdap-sandbox-<version>
            bin\deploy-metadb.bat run --task deploy_schema
            bin\deploy-metadb.bat run --task add_tenant ACME_CORP "ACME Supplies Inc."

            bin\deploy-metadb.bat run --task alter_tenant ACME_CORP "ACME Mega Supplies Inc."


Start the services
------------------

Once the configuration is done and the setup tools have be run, all that remains is to start the services:

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            cd /opt/trac/current
            bin/tracdap-svc-meta start
            bin/tracdap-svc-data start
            bin/tracdap-svc-orch start
            bin/tracdap-gateway start

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            cd /d C:\trac\tracdap-sandbox-<version>
            bin\tracdap-svc-meta.bat start
            bin\tracdap-svc-data.bat start
            bin\tracdap-svc-orch.bat start
            bin\tracdap-gateway.bat start

By default, the gateway will be listening on port 8080 and logs will be written to the log/ directory
in the installation folder.

To test that the services are running, you can use `Postman <https://www.postman.com/>`_
to send REST requests to the TRAC APIs. There are some example REST requests
`available in the TRAC GitHub repo <https://github.com/finos/tracdap/tree/main/examples/rest_calls>`_.

The service control scripts provide several commands which may be helpful:

* start - Start the service
* stop - Stop the service
* restart - Stop then immediately start the service
* status - Indicate whether a service is up or down
* kill - Kill the service immediately (Send SIGKILL, do not process a clean shutdown)
* kill_all - Find and kill all running instances of the service
* run - Run the service in the foreground

.. note::
    The *run* option requires a separate console for each service and will terminate the service on Ctrl-C.
    For this configuration, it is recommended to enable logging to stdout in trac-logging.xml.
