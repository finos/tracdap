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
    
        // Create a flow definition using regular JavaScript objects, then verify and convert
        createFlow2(inputName, outputName) {
            
            const flow = {node: {}, edge: []};
            flow.node[inputName] = {nodeType: "INPUT_NODE"};
            flow.node[outputName] = {nodeType: "OUTPUT_NODE"};
            flow.edge.push({head: inputName, tail: outputName});

            const err = trac.api.FlowDefinition.verify(flow);

            if (err)
                throw err;
    
            return trac.metadata.FlowDefinition.create(flow);
        }
    };

To use the API service interfaces, you will need to supply an rpcImpl function to perform network communication. An
example of how to do this for native gRPC calls is included in the documentation for protobuf.js: 
[Using services in protobuf.js](https://www.npmjs.com/package/protobufjs#using-services).

If you want to replace real network calls with a dummy local implementation for testing, you can do that by providing an
alternate rpcImpl. Here is an example of how to do that.
 
App.js:

    import React, {useState, useEffect} from "react";
    import {ExampleLogic} from "./apis/protobuf/exampleLogic";
    
    // When called using null the search call will be a dummy one with a spoofed response
    const controllerRpc = new ExampleLogic(null)
    
    function App() {

        const [fakeSearchResults, setFakeSearchResults] = useState("Not run")
    
        useEffect(() => {
    
           controllerRpc.exampleSearch("ACME_CORP", {}).then((response) => setFakeSearchResults(response))
    
        }, [])
    
        return (
    
            <React.Fragment>
                <div>
                    Welcome to TRAC rpc calls in React App using protobuf.js
                </div>
    
                <div>
                    {JSON.stringify(fakeSearchResults)}
                </div>
            </React.Fragment>
        );
    }

export default App;

exampleLogic.js

    import {trac} from 'trac-js-api';
    import {LocalServiceImpl} from "./localServiceImpl"
    
    export class ExampleLogic {
    
        constructor(rpcImpl) {
    
            this.rpcImpl = rpcImpl
    
            if (rpcImpl !== null)
                this.metaApi = new trac.api.TracMetadataApi(rpcImpl);
    
            else {
                const localImpl = new LocalServiceImpl();
                this.metaApi = new trac.api.TracMetadataApi(localImpl.createRpcImpl());
            }
        }
    
        exampleSearch(tenant, search) {
    
            const searchRequest = {tenant: tenant, searchParams: search};
    
            if (this.rpcImpl) {
    
                return this.metaApi.search(searchRequest)
                    .then(response => {
                        console.log(response)
                    })  // handle response
                    .catch(err => {
                        console.log(err)
                    });  // handle error
    
            } else {
    
                return this.metaApi.search(searchRequest)
            }
        }
    }

localServiceIpl.js

    import {trac} from 'trac-js-api';

    export class LocalServiceImpl {

    constructor() {

        // This needs to be a valid response, otherwise you get an empty
        // object when creating the message
        // TODO "FLOW" caused error see notes below
        this.fakeResponseObject = {
            searchResult: [{
                header: {
                    "objectType": "FLOW",
                    "objectId": "97f1a213-dc88-4cc8-9643-1e9f21018e27",
                    "objectVersion": 1,
                    "objectTimestamp": {
                        "isoDatetime": "2021-03-24T15:50:09.766544Z"
                    },
                    "tagVersion": 1,
                    "tagTimestamp": {
                        "isoDatetime": "2021-03-24T15:50:09.766544Z"
                    }
                },
                "attr": {}
            }]
        };
    }

    // This method creates a real rpcImpl that looks for available method implementations in the class
    createRpcImpl() {

        const _self = this;

        return (method, request, callback) => {

            if (!method.name in _self || typeof _self[method.name] !== 'function') {
                callback(new Error("Unknown API call " + method.name));
            } else {

                try {
                    const response = _self[method.name](request);
                    console.log("name: " + method.name);
                    callback(null, response);
                } catch (e) {
                    console.log(e);
                    callback(e);
                }
            }
        }
    }

    // Now add whichever methods you need for testing as plain JS functions. The
    // names of these must match the implemented API so that they
    // are correctly added to the local implementation
    search(requestObject) {

        // Example of spoofing a response with a local variable rather than
        // using a network call
        const errRequest = trac.api.MetadataSearchRequest.verify(requestObject);

        if (errRequest)
            throw errRequest;

        const requestMessage = trac.api.MetadataSearchRequest.create(requestObject)
        console.log(`requestMessage = ${JSON.stringify(requestMessage)}`)

        const requestBuffer = trac.api.MetadataSearchRequest.encode(requestMessage).finish();
        console.log(`requestBuffer = ${Array.prototype.toString.call(requestBuffer)}`)

        const requestDecode = trac.api.MetadataSearchRequest.decode(requestBuffer);
        console.log(`requestDecode = ${JSON.stringify(requestDecode)}`)

        // Verification step can not be done on the JavaScript object because of the definition
        // of objectType in the proto. When this is defined as enum it fails verification
        // see https://github.com/protobufjs/protobuf.js/issues/1261
        // The solution appears to not validate the object and use fromObject instead of encode.
        // If you skip the verify check then use create then objectType returns OBJECT_TYPE_NOT_SET
        // const errResponse = trac.api.MetadataSearchResponse.verify(this.fakeResponseObject);
        //
        // if (errResponse)
        //     throw errResponse;

        const searchResponseMessage = trac.api.MetadataSearchResponse.fromObject(this.fakeResponseObject)
        console.log(`searchResponseMessage = ${JSON.stringify(searchResponseMessage)}`)

        const searchResponseBuffer = trac.api.MetadataSearchResponse.encode(searchResponseMessage).finish()
        console.log(`searchResponseBuffer = ${Array.prototype.toString.call(searchResponseBuffer)}`)

        const searchResponseDecoded = trac.api.MetadataSearchResponse.decode(searchResponseBuffer)
        console.log(`searchResponseDecoded = ${JSON.stringify(searchResponseDecoded)}`)

        return searchResponseDecoded
    }

}
