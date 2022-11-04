

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

TRAC requires Java version 11 or higher, using LTS 11 or LTS 17 is recommended.
Suitable JDKs are available from
`Azul (Zulu) <https://www.azul.com/downloads/?package=jdk>`_ and
`Adoptium (Eclipse Temurin) <https://adoptium.net/>`_.
If your company restricts software deployment, it is fine to use the Java package they provide
so long as it is version 11 or higher.

**Python**

TRAC requires Python 3.7 or higher, which can be downloaded and installed from
`python.org <https://www.python.org/downloads/>`_.
If your company restricts software deployment, it is fine to use the Python package they provide
so long as it is version 3.7 or higher.

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

The sandbox deployment comes with some example configuration to use as a starting point. Before editing
these configuration files, it is recommended to take a copy as a backup, in case you need to refer to it
later.

**Platform configuration**

The sample platform configuration is already set up to use the H2 database, but you need to specify a path
where the database file will be stored:

.. code-block:: yaml

    dalType: JDBC
    dalProps:
      dialect: H2
      jdbcUrl: /path/to/trac/metadata/trac.meta
      ...

The configuration also contains an example for using local data storage. You need to specify a path.

.. code-block:: yaml

    storage:

      ACME_SALES_DATA:
        instances:
          - storageType: LOCAL
            storageProps:
              rootPath: /path/to/trac/data

Pay particular attention to the storage key, which is *ACME_SALES_DATA* in this example.
The storage key is a unique identifier for a storage location, you may want to give it
a meaningful name, for example relating to your project or business division.

You will also need to set a default storage location and format. If you only have one storage
location, that must be set as the default. If you want to store data in CSV format (not advised),
you can also change the default storage format to *CSV*.

.. code-block:: yaml

    data:

      defaultStorageKey: ACME_SALES_DATA
      defaultStorageFormat: ARROW_FILE

The example config contains the TRAC repository as an example, you should replace this with
your own model repository and choose a meaningful repository key. You can add multiple
repositories if required, so long as each one has a unique key.

.. code-block:: yaml

    repositories:

      sales_model_repo:
        repoType: git
        repoUrl: https://github.com/acme_corp/sales_model_repo

The last thing you need to add in the platform config is an executor. The example config is already set up
with a local executor, so you just need to add the path for the VENV you built in the deployment step.

.. code-block:: yaml

    executor:
      executorType: LOCAL
      executorProps:
        venvPath: /path/to/trac/tracdap-sandbox-<version>/venv

**Gateway configuration**

The gateway example config will work without alteration to serve the API endpoints for the TRAC services.
However, the gateway can also be used to route requests for client applications; this is particularly
useful for web applications in a dev / test scenario, because it provides a direct route to access the TRAC
API and avoids CORS issues. If you want to use this capability, look in the gateway config and you will find
an example of setting up an additional HTTP route. You can add as many HTTP routes as you need.

**Logging**

Logging is provided using log4j, the example configuration writes to the local log/ directory by default.

**Environment**

Environment variables can be specified in the shell before launching the TRAC services. Alternatively,
an environment file is available for both Linux / macOS (env.sh) and Windows (env.bat). These can be
useful for specifying system settings, such as JAVA_HOME to select a particular installation of Java,
or JAVA_OPTS to control the JVM memory parameters. You can also control some of the TRAC options here,
e.g. setting CONFIG_FILE will tell trac to load a different root config file.

Metadata setup
--------------

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

Once the configuration is done and the metadata database is prepared, all that remains is to start the services:

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

.. note:
    The *run* option requires a separate console for each service and will terminate the service on Ctrl-C.
    For this configuration, it is recommended to enable logging to stdout in trac-logging.xml.
