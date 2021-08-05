# TRAC Web API

*A next-generation data and analytics platform for use in highly regulated environments*


The TRAC web API provides a structured interface to develop web applications for the TRAC platform, based on the
popular [protobuf.js](https://www.npmjs.com/package/protobufjs) framework and using the highly optimised gRPC-Web
protocol for transport. Type information, auto-completion and inline documentation are made available for IDEs that
support those features. The APIs for each service and the associated data model are described in the API definition
files (.proto files):

* [TRAC Services](../../trac-services/src/main/proto/trac/api)
* [TRAC Metadata](../../trac-metadata/src/main/proto/trac/metadata)

The web API is intended for building applications that will run entirely in the browser (i.e. served as statically
compiled content) and communicate with the TRAC platform directly from there. It comes "batteries included", which is
to say it should have everything you need to get going right away. There is no need to generate code using protoc or to
deploy any kind of proxy or middle tier application server to get communication working, as the TRAC gateway component
handles all necessary protocol translations. In production setups it is likely that some combination of routers and load
balancers will be deployed, but for development and testing the routing capabilities of the TRAC gateway should suffice.

The TRAC gateway serves the platform APIs in gRPC native, gRPC-Web and JSON/REST formats, so any pattern of front-end
components that ultimately speaks one of those three protocols is supported. We recommend using the web API package
for building front-end applications unless you have a good reason not to, as it is the simplest, most well-structured
and best-documented option. 


## Building the web API

At present the API must be built from the TRAC source code (we will publish to the global NPM package repo in the
near future). Here are the commands to build the API.

    cd trac-api/packagegs/web
    npm install

    npm run tracVersion:windows  # For Windows platforms, requires PowerShell
    npm run tracVersion:posix    # For macOS or Linux
    npm run pbjs
    npm run pbts                 # Even if you are using plain JavaScript, this will supply type hints to the IDE


## Using the web API in an application

To include the web API in your project, just install it from the API package directory in the TRAC code tree.

    npm install --save path/to/trac/trac-api/packages/web

There is no need to install any other libraries to communicate with TRAC.

To run a local instance of TRAC for development, build and run the TRAC services as described in the
[platform README file](../../../README.md). You can use the TRAC gateway to route content and API calls. The
[sample gateway config](../../../etc/trac-devlocal-gateway.yaml) includes an example for pointing a route at your
local dev server, whether that's the one in your IDE or WebPack or whatever your favourite dev server tool is.
With you dev server running and the gateway configured, access your app through the gateway and you'll see API calls
redirected to the appropriate TRAC services.

Here is an example of using the API package to set up an API instance and make a simple call. Full documentation of the
APIs and data structures is included in the TRAC API packages and will also appear in auto-complete if your IDE
supports it. It may also be helpful to look at the documentation of
[protobuf.js](https://www.npmjs.com/package/protobufjs), which is used to generate the TRAC API classes.

    import {trac} from 'trac-web-api';
    
    class Example1 {
    
        constructor(rpcImpl) {

            // Setting up the API requires only two steps:
    
            // 1. Create an RPC implementation, you need one for each API class
            const metaApiRpcImpl = trac.setup.createWebRpcImpl(trac.api.TracMetadataApi);

            // 2. Create the API instance using the corresponding RPC implementation
            this.metaApi = new trac.api.TracMetadataApi(metaApiRpcImpl);
        }
    
        exampleSearch(tenant, search) {
    
            // The request and response object for each call is documented in the service API file
            // The data structures to go inside them are documented in the metadata package
            const searchRequest = {tenant: tenant, searchParams: search};
    
            // This call uses the futures pattern
            // Node-style callbacks are also supported if preferred
            return this.metaApi.search(searchRequest)
                .then(response => {
                    console.log(response)
                })  // handle response
                .catch(err => {
                    console.log(err)
                });  // handle error
        }

        buildSearchParams() {

            // An example of building a search object that could be used for te above call

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
    }
