
Authentication
==============

Authentication in TRAC consists of two main elements:

* JWT tokens, which are created and validated by the platform and hold information about users
* Authentication providers, which provide the log-on mechanism and are the source of user details

When a user logs on they are authenticated by an authentication provider, once authentication is successful
their details are retrieved from the provider and coded into a JWT token. The JWT token gives them access to
the platform for a limited period of time, after which they must be re-authenticated with the provider.

TRAC supports multiple authentication providers and more can be added using the TRAC plugins API.
The JWT mechanism is owned by the platform, and cannot be extended.


Root Authentication Key
-----------------------

The authentication system requires a root authentication key, that is used by the platform to sign
and validate JWT tokens. In order to set up the key you must have secrets configured, in both
the platform and gateway config files.

.. note::
    If you have already set up your root authentication key as described in the
    :ref:`sandbox quick start guide <deployment/sandbox:Setup Tools>` then you can skip this step.

You will also need to add an authentication block in both config files, specifying the issuer
and expiry times for JWT tokens. If you know the DNS address that TRAC will be served from you
could use this as the JWT issuer, other options could be the user ID of a service account you
have set up to run TRAC, or a TRAC reserved identifier such as "trac_system".

.. code-block:: yaml

    config:
      secret.type: PKCS12
      secret.url: secrets.p12

    authentication:
      jwtIssuer: http://localhost:8080/
      jwtExpiry: 7200

The auth-tool utility can be used to generate the root signing key, it will be written into the
secret store. The available key types are elliptic curve (EC) or RSA. Elliptic curve keys are
considered to give better security with better performance at lower key sizes. For this reason
we recommended EC 256 keys.

Make sure you have set the *SECRET_KEY* environment variable before running *auth_tool*. For
sandbox deployments, this can be set in *etc/env.sh* (or *etc\\env.bat* on Windows).

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            cd /opt/trac/current
            bin/auth-tool run --task create_root_auth_key EC 256

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            cd /d C:\trac\tracdap-sandbox-<version>
            bin\auth-tool.bat run --task create_root_auth_key EC 256

.. note::
    Running the *create_root_auth_key* command a second time will replace the root authentication key,
    which will invalidate any existing JWT tokens.

Providers
---------

You need to configure one provider in the authentication section of the gateway config file.

Guest Provider
^^^^^^^^^^^^^^

The guest provider logs everyone in as guest, without prompting for credentials.
This is the default provider set up in the sandbox example configuration.
The user ID and name can be set as properties of the provider.

.. code-block:: yaml

    authentication:

      provider:
        protocol: guest
        properties:
          userId: guest
          userName: Guest User


Basic Provider
^^^^^^^^^^^^^^

The basic provider uses HTTP basic authentication, which typically causes the browser
authentication window to appear when users try to access pages in a browser. To use
the basic provider you will need to enable TRAC's built in user database, by adding
these settings into the *config* section of the gateway config file.

.. code-block:: yaml

    config:
      users.type: PKCS12
      users.url: local_users.p12
      users.key: local_users_key

You will need to initialize the user database and add at least one user. The *auth-tool* utility will let
you do this. The add_user command is interactive and will ask for details to create a user. You can remove
users later using the *delete_user* command.

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            cd /opt/trac/current
            bin/auth-tool run --task init_trac_users
            bin/auth-tool run --task add_user

            bin/auth-tool run --task delete_user <user_id>

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            cd /d C:\trac\tracdap-sandbox-<version>
            bin\auth-tool.bat run --task init_trac_users
            bin\auth-tool.bat run --task add_user

            bin\auth-tool.bat run --task delete_user <user_id>

Once the user database is created you can enable the basic authentication provider. To do this,
replace the provider section in the authentication block of the gateway config file and set the
protocol to basic. Currently the basic provider does not require any other properties.

.. code-block:: yaml

    authentication:

      provider:
        protocol: basic
