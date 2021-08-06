# TRAC Web API

*TRAC is a next-generation data and analytics platform for use in highly regulated environments*


The TRAC web API provides a structured interface to develop web applications for the TRAC platform, based on the
popular [protobuf.js](https://www.npmjs.com/package/protobufjs) framework and using the highly optimised gRPC-Web
protocol for transport. Type information, auto-completion and inline documentation are made available for IDEs that
support those features. The APIs for each service and the associated data model are described in the API definition
files (.proto files):

* [TRAC Services](https://github.com/accenture/trac/tree/main/trac-api/trac-services/src/main/proto/trac/api)
* [TRAC Metadata](https://github.com/accenture/trac/tree/main/trac-api/trac-metadata/src/main/proto/trac/metadata)

The web API is intended for building applications that will run entirely in the browser (i.e. served as statically
compiled content) and communicate with the TRAC platform directly from there. It comes "batteries included", which is
to say it should have everything you need to get going right away. We recommend using the web API package  for building
front-end applications unless you have a good reason not to, as it is the simplest, most well-structured and
best-documented option. 


## Using the web API in an application

To install the web API in your project:

    npm install --save trac-web-api

Each of the TRAC services has a single public API class, which can be instantiated with just two lines of code.


    import {trac} from 'trac-web-api';
    
    class Example1 {
    
        constructor() {
    
            // 1. Create an RPC implementation, you need one for each API class
            const metaApiRpcImpl = trac.setup.createWebRpcImpl(trac.api.TracMetadataApi);

            // 2. Create the API instance using the corresponding RPC implementation
            this.metaApi = new trac.api.TracMetadataApi(metaApiRpcImpl);
        }

        ...

The methods available in each API can be called as JavaScript methods on the API class. They can be used with futures 
or callbacks. Documentation for each method including type information for the request and response objects is included
in the API package and should appear in auto-complete if your IDE supports it. Alternatively you can refer to the API
definition files for the full documentation.


        exampleSearchWithFutures(tenant, searchParams) {

            const searchRequest = {tenant: tenant, searchParams: searchParams};

            // API call using JavaScript futures
            return this.metaApi.search(searchRequest)
                .then(response => {
                    // handle response
                    console.log(response);
                })
                .catch(err => {
                    // handle error
                    console.log(err)
                });
        }

        exampleSearcWithCallbacks(tenant, searchParams) {

            const searchRequest = {tenant: tenant, searchParams: searchParams};

            // API call using Node-style callbcks
            this.metaApi.search(searchRequest, (err, response) => {

                if (err) {
                    // handle error
                    console.log(err);
                }
                else {
                    // handle response
                    console.log(response);
                }
            });
        }

         buildSearchParams() {

            // An example of building a search object that could be used for the above calls

            const exampleSearchParams = {

                objectType: trac.metadata.ObjectType.MODEL,
                search: { logical: {
        
                    operator: trac.metadata.search.LogicalOperator.AND,
                    expr: [
                        { term: {
                            attrName: "model_type",
                            attrType: trac.metadata.BasicType.STRING,
                            operator: trac.metadata.search.SearchOperator.EQ,
                            searchValue: { 
                                type: { basicType: trac.metadata.BasicType.STRING }, 
                                stringValue: "acme_widget_model" 
                            }
                        }},
                        { term: {
                            attrName: "model_owner",
                            attrType: trac.metadata.BasicType.STRING,
                            operator: trac.metadata.search.SearchOperator.EQ,
                            searchValue: { 
                                type: { basicType: trac.metadata.BasicType.STRING }, 
                                stringValue: "wile.e.cyote" 
                            }
                        }},
                    ]
                }}
            }

            const err = trac.metadata.search.SearchParameters.verify(exampleSearchParams);

            if (err)
                throw err;
    
            return trac.metadata.search.SearchParameters.create(exampleSearchParams);
        }

It may also be helpful to look at the documentation of [protobuf.js](https://www.npmjs.com/package/protobufjs),
which is used to generate the TRAC API classes.


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
    npm run pbjs
    npm run pbts                 # Even if you are using plain JavaScript, this will supply type hints to the IDE
