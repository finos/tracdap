# TRAC JavaScript API

*A next-generation data and analytics platform for use in highly regulated environments*

The JavaScript API provides a structured interface to develop webapps for the TRAC platform, based on the popular
[protobuf.js](https://www.npmjs.com/package/protobufjs) framework. Type information, auto-completion and inline
documentation are made available for IDEs that support those features.

## Building the JavaScript API

At present the API must be built from the TRAC source code. We plan to publish to the global NPM package repo in the
near future. Here are the commands to build the API.

    cd trac-api/packagegs/javascript
    npm install

    npm run tracVersion:windows  # For Windows platforms, requires PowerShell
    npm run tracVersion:posix    # For macOS or Linux
    npm run pbjs
    npm run pbts                 # Even if you are using plain JavaScript, this will supply type hints to the IDE


## Using the JavaScript API

To include the JavaScript API in your project, just install it from the JavaScript package directory in the TRAC
code tree.

    npm install --save path/to/trac/trac-api/packages/javascript

There are two ways to build metadata objects, either using the types from the TRAC API directly, or by creating
regular JavaScript objects and then converting them using the TRAC API to validate them.

    import {trac} from 'trac-js-api';

    class Examples {

        // Create a flow definition using structured objects from the TRAC API
        createFlow1(inputName, outputName) {
    
            const flow = new trac.metadata.FlowDefinition();
    
            flow.node[inputName] = trac.metadata.FlowNode.create({
                nodeType: trac.metadata.FlowNodeType.INPUT_NODE});
    
            flow.node[outputName] = trac.metadata.FlowNode.create({
                nodeType: trac.metadata.FlowNodeType.OUTPUT_NODE});
    
            flow.edge.push(trac.metadata.FlowEdge.create({
                head: inputName,
                tail: outputName
            }));
    
            return flow;
        }
    
        // Create a flow definition using regular JavaScript objects and then convert it
        createFlow2(inputName, outputName) {
            
            const flow = {node: {}, edge: []};
            flow.node[inputName] = {nodeType: "INPUT_NODE"};
            flow.node[outputName] = {nodeType: "OUTPUT_NODE"};
            flow.edge.push({head: inputName, tail: outputName});
    
            return trac.metadata.FlowDefinition.create(flow);
        }
    };

To use the API service interfaces, you will need to supply an rpcImpl function to perform network communication. An
example of how to do this for native gRPC calls is included in the documentation for protobuf.js: 
[Using services in protobuf.js](https://www.npmjs.com/package/protobufjs#using-services).

If you want to replace real network calls with a local implementation for testing, you can do that by providing an
alternate rpcImpl. Here is an example of how to do that.

    class LocalServiceImpl {
    
        constructor() {
            // Set up whatever local data you are planning to use
            this.localData = {};
        }
    
        // This method creates an rpcImpl that looks for available method implementations in the class
        createRpcImpl() {
    
            const _self = this;
    
            return (method, request, callback) => {
    
                if (!method.name in _self || typeof _self[method.name] !== 'function') {
                    callback(new Error("Unknown API call " + method.name));
                }
                else {
    
                    try {
                        const response = _self[method.name](request);
                        callback(null, response);
                    } catch (e) {
                        console.log(e);
                        callback(e);
                    }
                }
            }
        }
    
        // Now add whichever methods you need for testing as plain JS functions
        search(searchProto) {

            // Decode proto request
            const searchRequest = trac.api.MetadataSearchRequest.decode(searchRequest);

            const result = [...];  // Logic to build search results using this.localData

            // Using a structured object verifies the response
            const searchResponse = trac.api.MetadataSearchResponse.create({searchResult: result});
        
            // Encode proto result
            return trac.api.MetadataSearchResponse.encode(searchResponse).finish();
        }
    
    }

    class ExampleLogic {

        constructor(rpcImpl) {

            if (rpcImpl !== null)
                this.metaApi = new trac.api.TracMetadataApi(rpcImpl);

            else {
                const localImpl = new LocalServiceImpl();
                this.metaApi = new trac.api.TracMetadataApi(metaImpl.createRpcImpl());
            }
        }

        exampleSearch() {

            const search = {...};  // Create SearchParameters
            const searchRequest = { tenant: tenant, searchParams: search});

            this.metaApi.search(searchRequest)
                .then(response => ...)  // handle response
                .catch(err => ...);  // handle error
        }
    }
