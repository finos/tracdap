# TRAC Web API

*TRAC is a next-generation data and analytics platform for use in highly regulated environments*

The TRAC web API provides a structured interface for developing web applications to run on the TRAC platform.
It is based on the popular [protobuf.js](https://www.npmjs.com/package/protobufjs) framework and uses the
highly optimised gRPC-Web protocol for transport. The package supports both JavaScript and TypeScript and 
provides everything needed to communicate with the TRAC platform directly from a web browser, no additional
development tooling, setup or middleware is required. Inline documentation is available for IDEs that support it.

Documentation for the TRAC platform is available at
[trac-platform.readthedocs.io](https://trac-platform.readthedocs.io).


## Installing the web API package

To install the web API package in your project:

    npm install --save trac-web-api


## Using the web API in an application

Each of the TRAC services has a single public API class, which can be instantiated with just two lines of code.


    import {trac} from 'trac-web-api';
    
    class Example1 {
    
        constructor() {
    
            // Use trac.setup to create an RPC instance, you need one for each API class
            // This instance is for use in the browser, it will direct calls to the page origin server

            const metaApiRpcImpl = trac.setup.rpcImplForBrowser(trac.api.TracMetadataApi);

            // Then create the API

            this.metaApi = new trac.api.TracMetadataApi(metaApiRpcImpl);
        }

        ...


The API methods can be called as JavaScript methods on the API classes, both futures and callbacks are supported.


        exampleSearchWithFutures(tenant, searchParams) {

            const searchRequest = {
                tenant: tenant, 
                searchParams: searchParams
            };

            // API call using JavaScript futures
            return this.metaApi.search(searchRequest)
                .then(response => {
                    // handle response
                    console.log(response);
                })
                .catch(err => {
                    // handle error
                    console.log(err.message)
                });
        }

        exampleSearcWithCallbacks(tenant, searchParams) {

            const searchRequest = {
                tenant: tenant, 
                searchParams: searchParams
            };

            // API call using Node-style callbcks
            this.metaApi.search(searchRequest, (err, response) => {

                if (err) {
                    // handle error
                    console.log(err.message);
                }
                else {
                    // handle response
                    console.log(response);
                }
            });
        }

        buildSearchParams() {

            // An example search that could be used for the above calls

            const exampleSearchParams = {

                objectType: trac.ObjectType.MODEL,
                search: { logical: {
        
                    operator: trac.LogicalOperator.AND,
                    expr: [
                        { term: {
                            attrName: "model_type",
                            attrType: trac.STRING,
                            operator: trac.SearchOperator.EQ,
                            searchValue: { stringValue: "acme_widget_model" }
                        }},
                        { term: {
                            attrName: "model_owner",
                            attrType: trac.STRING,
                            operator: trac.SearchOperator.EQ,
                            searchValue: { stringValue: "wile.e.cyote" }
                        }},
                    ]
                }}
            }

            const err = trac.metadata.SearchParameters.verify(exampleSearchParams);

            if (err)
                throw err;
    
            return trac.metadata.SearchParameters.create(exampleSearchParams);
        }

To learn how to build applications on TRAC, check out the
[application development section](https://trac-platform.readthedocs.io/en/stable/app_dev)
in our online documentation. It may also be helpful to look at the documentation of
[protobuf.js](https://www.npmjs.com/package/protobufjs),
which is used to generate the TRAC API classes for the web API package.


# Running a local instance of TRAC for development

Often it will be helpful to run a local instance of TRAC when developing against the web API, to test your API calls
against a real implementation without needing to run your full build pipeline and push to a dev/test server.

There are no pre-built packages (yet!) for the TRAC platform services. To build and run the platform from source,
follow the instructions in the main README file in the root of the
[TRAC source code repository](https://github.com/accenture/trac).
To avoid any compatibility issues, make sure you check out the version tag that matches your version of the web API.

For a local dev setup, you can use the TRAC gateway to route both your app content and API calls. Look in the
main code repo under etc/ for an example configuration file for the gateway, which includes an example of pointing
a route at your local dev server, whether that's the one in your IDE or WebPack or whatever your favourite dev server
tool is. Once your you dev server running and the gateway is configured, access your app through the gateway. You will
see API calls redirected to the appropriate TRAC services.


## Mocking the API services for rapid development and unit testing

The simplest way to mock the API classes is to extend the API class you want to mock and just override the methods
you are interested in to return your test data. Some simple scaffolding will allow for support of both futures and
callbacks (if you are sticking to one pattern this may not be necessary). It is also probably helpful to throw an
error for methods that are not available in the mock implementation.

Here is a quick example of one way this could be done.


    class LocalImpl extends trac.api.TracMetadataApi {

        // Set an RPC impl to throw an error for methods that have not been mocked

        constructor() {
            super(() => { throw new Error("Not implemented locally")});
        }

        // Helper function to handle both future and callback patterns

        callbackOrFuture(callback, err, response) {

            if (callback)
                callback(err, response);

            else if (err)
                return Promise.reject(err);

            else
                return Promise.resolve(response);
        }

        // Add mock implementations for whichever methods are needed...

        search(request, callback) {

            try {
                
                // Some logic here, optionally using the contents of request
                const dummyResponse = {searchResult: []};

                return this.callbackOrFuture(callback, null, dummyResponse);
            }
            catch (err) {

                return this.callbackOrFuture(callback, err, null);
            }
        }
    }

## Building the web API from source

This is not normally necessary for app development, but if you want to do it here are the commands.

    cd trac-api/packagegs/web
    npm install

    npm run tracVersion:windows  # For Windows platforms, requires PowerShell
    npm run tracVersion:posix    # For macOS or Linux
    npm run buildApi
