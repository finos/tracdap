
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

.. note::
    If you have already set up your root authentication key as described in the
    :ref:`sandbox quick start guide <deployment/sandbox:Setup Tools>` then you can skip this step.

The authentication system requires a root authentication key, that is used by the platform to sign
and validate JWT tokens. The root authentication key is stored in the secret store, so make sure
you have a secret store configured in both the platform and gateway config files.

.. code-block:: yaml

    config:
      secret.type: PKCS12
      secret.url: secrets.p12

The secret-tool utility can be used to generate the root signing key. The available key types are
elliptic curve (EC) or RSA. Elliptic curve keys are considered to give better security with better
performance at lower key sizes. For this reason we recommended EC 256 keys.

Make sure you have initialized the secret store and set the *TRAC_SECRET_KEY* environment variable
before running *secret-tool*. For more details on the *secret-tool*, see :doc:`secrets`.

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            cd /opt/trac/current
            bin/secret-tool run --task create_root_auth_key EC 256

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            cd /d C:\trac\tracdap-sandbox-<version>
            bin\secret-tool.bat run --task create_root_auth_key EC 256

.. note::
    Running the *create_root_auth_key* command a second time will replace the root authentication key,
    which will invalidate any existing JWT tokens.


JWT configuration
-----------------

The *authentication* block in *trac-platform.yaml* can be used to specify the issuer
and expiry times for JWT tokens. If you know the DNS address that TRAC will be served from you
could use this as the JWT issuer, other options could be the user ID of a service account you
have set up to run TRAC, or a TRAC reserved identifier such as "trac_platform".

As long as TRAC has a valid JWT token for a user (or connected system), it does not need to reauthenticate.
Expiry of the TRAC tokens is managed by the expiry, limit and refresh parameters:

* jwtExpiry: Token expiry time in seconds, if the user is inactive
* jwtLimit: Hard limit on the token expiry time, whether the user is active or not
* jwtRefresh: Time in seconds after which tokens will be refreshed

In this example the user will be given a token for one hour when they log in. TRAC will check the token
on every API call, if the token is older than the refresh time the user will be given a new token with
the expiry time extended back to one hour. The limit is set to 16 hours, the token cannot be extended
past that time even if the user remains active. When the token expires or the limit is reached the user
will have to log in again.

.. code-block:: yaml

    authentication:
      jwtIssuer: trac.platform@acme.corp
      jwtExpiry: 3600
      jwtLimit: 57600
      jwtRefresh: 300


Providers
---------

You need to configure a provider in the authentication section of the platform config file.
The authentication provider is a plugin, so the particular settings required will depend on
the protocol you choose.

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
these settings into the *config* section of the platform config file.

.. code-block:: yaml

    config:
      users.type: PKCS12
      users.url: local_users.p12
      users.key: local_users_key

You will need to initialize the user database and add at least one user. The *secret-tool* utility will let
you do this. The add_user command is interactive and will ask for details to create a user. You can remove
users later using the *delete_user* command.

.. tab-set::

    .. tab-item:: Linux / macOS
        :sync: platform_linux

        .. code-block:: shell

            cd /opt/trac/current
            bin/secret-tool run --task init_trac_users
            bin/secret-tool run --task add_user

            bin/secret-tool run --task delete_user <user_id>

    .. tab-item:: Windows
        :sync: platform_windows

        .. code-block:: batch

            cd /d C:\trac\tracdap-sandbox-<version>
            bin\secret-tool.bat run --task init_trac_users
            bin\secret-tool.bat run --task add_user

            bin\secret-tool.bat run --task delete_user <user_id>

Once the user database is created you can enable the basic authentication provider. To do this,
replace the provider section in the authentication block of the gateway config file and set the
protocol to basic. Currently the basic provider does not require any other properties.

.. code-block:: yaml

    authentication:

      provider:
        protocol: basic
