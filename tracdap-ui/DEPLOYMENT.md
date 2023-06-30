Deploying TRAC UI
-----------------

# Requirements & Compatibility

The TRAC UI has only two requirements in terms of hosting:

* Any web server capable of serving static content
* The ability to configure URL re-writing

TRAC UI is compatible with modern web browser standards. It does not support Internet Explorer.

# Basic install

The TRAC UI tarball for each release is published on the TRAC UI GitHub repository. Releases are accessible to Accenture employees at this URL:

https://github.com/Accenture/trac-ui/releases

Accenture can download and supply the packaged builds, which are TGZ tarballs.

The tarball includes the entire UI distribution in a versioned folder. To install the UI on a Linux server, it is recommended to create versioned folders and use symlinks, for easy
rollback if required. E.g. if you are using /opt/trac/ui as your content location:

mkdir /opt/trac/ui cd /opt/trac/ui tar -xzvf path/to/trac-ui-<version>.tgz . ln -s trac-ui-<version>/ current

In your web server config, serve content from /opt/trac/ui/current. This could either be the root content location, or a sub-location using an alias.

Here is an example of the Apache configuration to serve the TRAC content from the path /trac-ui:

    Alias /trac-ui "/opt/trac/ui/current"
    
    <Directory "/opt/trac/ui">

        Options SymlinksIfOwnerMatch
        AllowOverride None
        Require all granted

    </Directory>

It is not required to use the Apache web server, so long as the server can serve static content and supports URL rewriting.

Once you bring the web server up you should be able to see the TRAC UI at the configured port / path. The UI will report errors that it cannot find / connect to the TRAC API
endpoints - this is normal and will be resolved once routing is set up in the TRAC gateway.

# Configuration

In order to avoid having to build the entire TRAC UI package to change configuration of the application a config file called client-config.json can be added to the /public/static/
folder. This allows a range options to be configured including the GitHub model repositories to use to load models into TRAC. A sample model repository config is:

    {
        "name": "test_models",
        "owner": "Accenture",
        "ownerType": "organisation",
        "type": "gitHub",
        "modelFileExtensions": [
          "py"
        ],
        "modelMetadataExtension": "json",
        "modelMetadataName": "_metadata",
        "tracConfigName": "test_models_repo",
        "httpUrl": "https://github.com",
        "apiUrl": "https://api.github.com",
        "tenants": [
          "ACME_CORP"
        ]
    }

Here tenants is an array of TRAC tenants that should have access to the repository to load from while tracConfigName should match a repository configured in
/etc/trac-platform.yaml.

The client-config.json file also allows the application and client logos to be configured. The default config file includes TRAC d.a.p. and Accenture logos but these can be set in
the config, please make sure that the corresponding images are loaded to the path specified.

# Gateway configuration

This step configures the TRAC gateway to route requests for the UI, so that the UI and APIs are all served from a single address. The gateway also provides an integration point for
authentication, as well as performing protocol conversion and a number of other functions.

To configure the gateway, edit the trac-gateway.yaml file in the TRAC platform configuration and restart the gateway service. This example configuration will mount the UI into the
/trac-ui/ path, assuming the server is serving the UI content from the same path on port 9090. You can choose any deployment path that is meaningful to you, but the match path
cannot be "/" as this would re-route all requests, including requests for the TRAC APIs. It is not necessary for the source and target path to be the same, although having them the
same can make the routing configuration slightly more clear.

For hostnames, the "match" hostname should match what will be received in the "Host:" header of HTTP requests. For physical or VM deployments this will normally be the name of the
box the gateway is running on. For container deployments it will be the externally facing address that is mapped to the gateway container(s). The target hostname is where requests
for UI content will be sent, which may be the address of your web server, or a load balancer / vDNS that sits in front of it.

    routes:

      # This route can be used to run a local development environment against the TRAC APIs

      - routeName: TRAC UI
        routeType: HTTP

        match:
          host: trac-gw-1.trac.sit.example.org
          path: /trac-ui/

        target:
          scheme: http
          host: web-svr-1.trac.sit.example.org
          port: 9090
          path: /trac-ui/

# URL rewriting

TRAC UI uses virtual URLs to integrate the user experience with the browser, allow navigation using the browser's back/forward buttons and create permanent links to TRAC artifacts.
In order to serve these URLs the web server must support URL rewriting. If URL rewriting is not available the UI will work with a degraded user experience, users will have to go to
the index page and navigate through to what they want to see.

    <Directory "/opt/trac/ui">

        Options SymlinksIfOwnerMatch
        AllowOverride None
        Require all granted

        RewriteEngine on
        RewriteRule /app/(.+/)*static/(.*) "/trac-ui/static/$2"
        RewriteRule /app/(.+/)*bundle/(.*) "/trac-ui/bundle/$2"
        RewriteRule /app/(.*) "/trac-ui/"

    </Directory>

Equivalent rewriting rules can be configured in other popular web servers, such as NginX or Microsoft's IIS.
