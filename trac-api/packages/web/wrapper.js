/*
 * Copyright 2021 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

(function(global, factory) { /* global define, require, module */

    if (typeof define === 'function' && define.amd) {

        /* AMD */

        define([$DEPENDENCY, 'grpc-web'], factory);
    }

    else if (typeof require === 'function' && typeof module === 'object' && module && module.exports) {

        /* CommonJS */

        $root = factory(require($DEPENDENCY), require('grpc-web'));

        // Allow recent NPM versions to auto-detect ES6-style exports
        module.exports = $root;
        module.exports.trac = $root.trac;
        module.exports.trac.api = $root.trac.api;
        module.exports.trac.metadata = $root.trac.metadata;
        module.exports.google = $root.google;
    }

})(this, function($protobuf, grpc) {

    "use strict";

    $OUTPUT;

    // RPC impl and setup functions should move to a separate source file

    const WebRpcImpl = (function() {

        const METHOD_TYPE_MAP = $METHOD_TYPE_MAPPING;

        function WebRpcImpl(service, namespace, protocol, host, port) {

            const trac = $root.trac;

            // If the namespace is not specified, the service must be part of trac.api
            if (!namespace && !(service.name in trac.api))
                throw new Error('Service ' + service.name + ' is not part of trac.api, you must specify a namespace');

            // If a namespace is supplied, always use it even if the service exists in trac.api
            this.serviceName = namespace
                ? namespace + '.' + service.name
                : 'trac.api.' + service.name;

            this.hostAddress = protocol
                ? protocol + "://" + host + ":" + port
                : "";

            // Empty RPC metadata for now
            this.rpcMetadata = {}

            this.grpcWeb = new grpc.GrpcWebClientBase({format: 'binary'});
        }

        WebRpcImpl.prototype.rpcImpl = function(method, request, callback) {

            try {

                const methodUrl = `${this.hostAddress}/${this.serviceName}/${method.name}`;

                const methodType = (method.name in METHOD_TYPE_MAP)
                    ? METHOD_TYPE_MAP[method.name]
                    : grpc.MethodType.UNARY;

                const methodDescriptor = new grpc.MethodDescriptor(
                    methodUrl, methodType,
                    Uint8Array, Uint8Array,
                    request => request,
                    response => response);

                if (methodType === grpc.MethodType.SERVER_STREAMING)
                    this.serverStreaming(method, methodDescriptor, request, callback);

                else
                    this.unaryCall(method, methodDescriptor, request, callback);
            }
            catch (error) {
                console.log(JSON.stringify(error));
                callback(error, null);
            }
        }

        WebRpcImpl.prototype.unaryCall = function(method, descriptor, request, callback) {

            this.grpcWeb.rpcCall(method.name, request, this.rpcMetadata, descriptor, callback);
        }

        WebRpcImpl.prototype.serverStreaming = function(method, descriptor, request, callback) {

            const stream = this.grpcWeb.serverStreaming(method.name, request, this.rpcMetadata, descriptor);

            stream.on("data", msg => callback(null, msg));
            stream.on("end", () => callback(null, null));
            stream.on("error", err => callback(err, null));

            // TODO: Do we need to do anything with these two messages?

            stream.on("metadata", metadata => console.log("gRPC Metadata: " + JSON.stringify(metadata)));
            stream.on("status", status => console.log("gRPC Status: " + JSON.stringify(status)));
        }

        return WebRpcImpl;

    })();

    $root.trac.setup = (function() {

        /**
         * Namespace setup.
         * @memberof trac
         * @namespace
         */
        const setup = {};

        /**
         * Create an rpcImpl for use in a web browser, requests will be sent to the page origin server
         * @function rpcImplForBrowser
         * @memberof trac.setup
         * @param {$protobuf.rpc.Service} serviceClass The service class to create an rpcImpl for
         * @returns {$protobuf.RPCImpl} An rpcImpl function that can be used with the specified service class
         */
        setup.rpcImplForBrowser = function(serviceClass) {

            const rpcImpl = new WebRpcImpl(serviceClass, '');
            return rpcImpl.rpcImpl.bind(rpcImpl);
        }

        /**
         * Create an rpcImpl that connects to a specific target
         * @function rpcImplForTarget
         * @memberof trac.setup
         * @param {$protobuf.rpc.Service} serviceClass The service class to create an rpcImpl for
         * @param {string} protocol The protocol to use for connection (either "http" or "https")
         * @param {string} host The host to connect to
         * @param {number} port The port to connect to
         * @returns {$protobuf.RPCImpl} An rpcImpl function that can be used with the specified service class
         */
        setup.rpcImplForTarget = function(serviceClass, protocol, host, port) {

            const rpcImpl = new WebRpcImpl(serviceClass, '', protocol, host, port);
            return rpcImpl.rpcImpl.bind(rpcImpl);
        }

        return setup;

    })();

    const api_mapping = $API_MAPPING;

    $root.trac = {
        ...$root.trac,
        ...api_mapping
    };

    return $root;
});
