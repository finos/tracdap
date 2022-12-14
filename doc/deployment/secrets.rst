
Using Secrets
=============

The TRAC platform is designed to connect to a wide variety of technologies and services, which
often involves holding sensitive configuration details or "secrets" such as passwords
and key files. TRAC uses plugins to handle these connections and has a configuration mechanism
to keep the secrets safe. This mechanism can be used for any integration point with sensitive configuration.

.. note::
    Plugins can still use alternate methods to secure their sensitive configuration if required.


Plugin configuration blocks
---------------------------

Anywhere you are connecting to a technology service, you will see a plugin configuration block
in the TRAC configuration. These blocks all have the same pattern, here is an example for connecting
an S3 storage bucket:

.. code-block:: yaml

    protocol: S3
    properties:
      bucket: acme-sales-data
      prefix: rockets/
      credentials: static
      accessKeyId: <aws_access_key_id>
    secrets:
      secretAccessKey: acme_bucket_secret_key

In this configuration block, *protocol* is the protocol that will be used to talk to the storage service.
TRAC will look for a plugin that supports the protocol, if the plugin is missing TRAC will not start.

Once the plugin is found, all the *properties* are passed to that plugin as configuration. The
properties that are required vary depending on the protocol / plugin. For example for the
JDBC metadata plugin, properties are used to construct a JDBC datasource and vary depending
on the SQL dialect and JDBC driver.


Sensitive configuration
------------------------

Any property required by a plugin can also be supplied as a secret, by moving the property
into the *secrets* section of the configuration. In this case, the ID (or "alias") of the
secret is specified in the main configuration file and TRAC fetches the secret value from
the secret store, decrypts it and passes the decrypted value into the plugin as a property.

In the example above using static credentials for an AWS bucket, the key ID is presented as a
regular property and the secret key is stored as a secret. TRAC does not limit which properties
are stored as secrets, you could choose to key the key ID secret as well if necessary.


Configuring the secret store
----------------------------

The secret store itself is a technology touch point, and multiple implementations are available.
The core platform includes an implementation  using Java Keystores. All the major cloud providers
offer key manager services, so for cloud deployments those may be more appropriate.

The secret store is configured in the *config* section at the top of the main configuration files.
This example would set up a Java Keystore using PKCS12 format called *secrets.p12* in the main
config directory:

.. code-block:: yaml

    config:
      secret.type: PKCS12
      secret.url: secrets.p12

In many deployments you will need a master key to access the secret store, which is called the *secret key*.
A secret key is not normally needed for cloud deployments, where access to the secret store can be
controlled using service roles in the cloud infrastructure, but there are several deployment situations
where a secret key will be required.

The secret key can be passed to any of the TRAC services or tools using the *TRAC_SECRET_KEY*
environment variable. For production deployments this can be done through a service configuration
or a scheduling tool. For a developer running a sandbox on their laptop it is often OK to store the
secret key in the config folder, by adding it to env.sh (or env.bat on Windows).


Managing secrets
----------------

If you are using a Java Keystore you can use the *keytool* that comes with the JDK to create and manage secrets.
In practice we have found this to be fiddly, particularly for certificates and public/private key pairs.
We have created a utility called *secret-tool*, which can be used to manage secrets in a java keystore
using TRAC-style tasks. By default secret-tool will operate on the keystore configured in your TRAC platform
config file.

To complete the example above, suppose we wanted to add the secret *acme_bucket_secret_key* into the keystore:

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            cd /opt/trac/current
            bin/secret-tool run --task init_secrets
            bin/secret-tool run --task add_secret acme_bucket_secret_key

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            cd /d C:\trac\tracdap-sandbox-<version>
            bin\secret-tool.bat run --task init_secrets
            bin\secret-tool.bat run --task add_secret acme_bucket_secret_key

The tool will prompt you for the value of the secret, which will not be displayed on the console.
There is also a *delete_secret* task to remove a secret from the store.

Currently the *secret-tool* utility only works with Java Keystores. If you are using a secret manager
from your cloud provider, you will need to use their console or CLI tools to create and manage secrets.
