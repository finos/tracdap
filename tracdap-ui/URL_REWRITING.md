# URL Rewriting

## Introduction

The TRAC UI can be run locally in development mode using the webpack devserver or in production mode using a web server
such as Apache http webserver. In both cases there are several rewriting rules required to serve the application that
all serve different purposes, this document describes the rules in place and what their role is in serving the
application.

## Development mode

* The devserver is started using the 'Start development' script. This creates a development bundle and opens up a
  browser at http://localhost:3000/app/. The settings for this are in the webpack.config.ts file. The 3000 port is the
  port the application is served on. The TRAC UI should show, but it won't be connected to TRAC


* In order to connect the web application to the TRAC services it must be accessed through the TRAC gateway service,
  this handles authentication as well as TRAC API and web server requests. For local deployments the TRAC gateway is
  configured in trac\etc\trac-devlocal-gateway.yaml in the TRAC project and listens on port 8080, so the URL
  becomes http://localhost:8080/app/. If you go to this URL however the browser will show an error, this is because we
  have not told the gateway which requests to redirect to the devserver on port 3000. Furthermore, the gateway needs to
  have a way to differentiate between requests that it should forward to the devserver and those that it should pass to
  the other TRAC services, such as data requests. Both are achieved by appending the URL with 'trac-ui'. This is
  configured in the TRAC project in trac\etc\trac-devlocal-gateway.yaml. Note that this path can be changed but 'trac'
  and 'app' are blacklisted.

  ~~~~
  routes:
  
    # This route can be used to run a local development environment against the TRAC APIs
  
    - routeName: Local App
      routeType: HTTP
  
      match:
        host: localhost
        path: /trac-ui/
  
      target:
        scheme: http
        host: localhost
        port: 3000
        path: /
  ~~~~

  This config says that any requests to the gateway service on port 8080 that includes '/trac-ui/' in the path will be
  redirected to '/' on port 3000. That is that it will forward the request to the devserver. This means that users
  should access the application at http://localhost:8080/trac-ui/.


* If you visit http://localhost:8080/trac-ui/ you will see that the URL is rewritten
  to http://localhost:8080/trac-ui/app/. This rewriting rules is defined in Javascript in trac-ui\src\index.tsx using
  the publicPath value from the general config.

  ~~~
  if (!window.location.pathname.includes(GeneralChartConfig.publicPath)) {
  
      window.history.pushState({}, "", "." + GeneralChartConfig.publicPath)
  }
  ~~~

  This code says that if the user hits the root URL, put app/ into the browser URL. This doesn't trigger an HTTP
  request, which means index.html will still work even if no rewrite rules are enabled.


* After this URL rewrite some additional Javascript updates the public path value. This looks at the URL and extracts
  the full public path, normally this is '/app/' so anything up to this will be set as the React Router basename e.g.
  for /trac-ui/app/page, /trac-ui/app/ will be detected as the React Router basename.

  ~~~
  const appPathIndex = window.location.pathname.indexOf(GeneralChartConfig.publicPath);
  GeneralChartConfig.publicPath = appPathIndex > 0
      ? window.location.pathname.substring(0, appPathIndex + GeneralChartConfig.publicPath.length)
      : GeneralChartConfig.publicPath
  ~~~

* The updated public path is passed to [React Router](https://reactrouter.com/en/main) as the basename for the
  virtualized paths for the various scenes in the application. The Router is imported in trac-ui\src\App.tsx. As the
  user navigates the application the URL will be updated by React Router to make it look like the single page
  application actually has a directory structure.


* If you navigate to http://localhost:8080/trac-ui/app/run-a-model you will notice that the page loads normally.
  Ordinarily the devserver would look for an index.html file in this folder despite the fact that the folder is
  virtualized by React Router and doesn't exist. To make the application load on any of the paths used by React Router
  the devserver has some URL rewriting rules defined using
  the [proxy](https://webpack.js.org/configuration/dev-server/#devserverproxy) property of the devserver config.

  ~~~
  proxy: {
              '/app/': {
                  target: 'http://localhost:3000',
                  //@ts-ignore
                  "pathRewrite": (path) => {
  
                      if (path.includes("/static/")) {
                          return path.substring(path.indexOf("/static/"));
                      }
  
                      if (path.includes("/bundle/")) {
                          return path.substring(path.indexOf("/bundle/"));
                      }
  
                      return "/"
                  },
              }
          }
  ~~~

  These rewrites redirect any request for a static file (e.g. images and css) file or a bundle (Javascript) from a
  virtualized folder will be redirected to the 'static' or 'bundle' root folder of the application, where the real files
  exist. The only remaining file that can be requested is index.html which exists at the root folder ('/). Note that the
  static and bundle folders are defined as part of the webpack config.