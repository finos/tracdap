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

    /* AMD */ if (typeof define === 'function' && define.amd)
        define([$DEPENDENCY, 'grpc-web'], factory);


    /* CommonJS */ else if (typeof require === 'function' && typeof module === 'object' && module && module.exports)
        module.exports = factory(require($DEPENDENCY), require('grpc-web'));

})(this, function($protobuf, grpc) {

    "use strict";

    $OUTPUT;

    // RPC impl and setup functions should move to a separate source file

    const WebRpcImpl = (function() {

        function WebRpcImpl(service, namespace) {

            const trac = $root.trac;

            // If the namespace is not specified, the service must be part of trac.api
            if (!namespace && !(service.name in trac.api))
                throw new Error('Service ' + service.name + ' is not part of trac.api, you must specify a namespace');

            // If a namespace is supplied, always use it even if the service exists in trac.api
            this.serviceName = namespace
                ? namespace + '.' + service.name
                : 'trac.api.' + service.name;

            // Empty RPC metadata for now
            this.rpcMetadata = {}

            this.grpcWeb = new grpc.GrpcWebClientBase({format: 'binary'});
        }

        WebRpcImpl.prototype.rpcImpl = function(method, request, callback) {

            try {

                const methodName = `/${this.serviceName}/${method.name}`;

                const methodDescriptor = new grpc.MethodDescriptor(
                    methodName, grpc.MethodType.UNARY,
                    Uint8Array, Uint8Array,
                    request => request,
                    response => response);

                return this.grpcWeb.rpcCall(method.name, request, this.rpcMetadata, methodDescriptor, callback);
            }
            catch (error) {
                callback(error, null);
            }
        }

        return WebRpcImpl;

    })();

    $root.trac.setup = (function() {

        const setup = {};

        setup.createWebRpcImpl = function(service) {

            const rpcImpl = new WebRpcImpl(service, '');
            return rpcImpl.rpcImpl.bind(rpcImpl);
        }

        return setup;

    })();

    return $root;
});
