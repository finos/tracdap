/*
 * Copyright 2022 Accenture Global Solutions Limited
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
        module.exports.tracdap = $root.tracdap;
        module.exports.tracdap.api = $root.tracdap.api;
        module.exports.tracdap.metadata = $root.tracdap.metadata;
        module.exports.google = $root.google;
    }

})(this, function($protobuf, grpc) {

    "use strict";

    $OUTPUT;

    $root.tracdap.api.TracMetadataApi._serviceName = "tracdap.api.TracMetadataApi";
    $root.tracdap.api.TracDataApi._serviceName = "tracdap.api.TracDataApi";
    $root.tracdap.api.TracOrchestratorApi._serviceName = "tracdap.api.TracOrchestratorApi";

    grpc.MethodType.CLIENT_STREAMING = "CLIENT_STREAMING";
    grpc.MethodType.BIDI_STREAMING = "BIDI_STREAMING";

    const METHOD_TYPE_MAP = $METHOD_TYPE_MAPPING;

    const DEFAULT_TRANSPORT = "google"


    const GoogleTransport = (function() {

        function GoogleTransport(serviceName, protocol, host, port, options) {

            this.serviceName = serviceName;
            this.options = options;

            this.hostAddress = protocol
                ? protocol + "://" + host + ":" + port
                : ""

            this.rpcMetadata = {}

            this.grpcWeb = new grpc.GrpcWebClientBase({format: 'binary'});

            this.options.debug && console.log(`GoogleTransport created, host address = [${this.hostAddress}]`);
        }

        GoogleTransport.prototype.rpcImpl = function(method, request, callback) {

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
                    this._grpcWebServerStreaming(method, methodDescriptor, request, callback);

                else
                    this._grpcWebUnary(method, methodDescriptor, request, callback);
            }
            catch (error) {
                console.log(JSON.stringify(error));
                callback(error, null);
            }
        }

        GoogleTransport.prototype._grpcWebUnary = function(method, descriptor, request, callback) {

            this.grpcWeb.rpcCall(method.name, request, this.rpcMetadata, descriptor, callback);
        }

        GoogleTransport.prototype._grpcWebServerStreaming = function(method, descriptor, request, callback) {

            const stream = this.grpcWeb.serverStreaming(method.name, request, this.rpcMetadata, descriptor);

            stream.on("data", msg => callback(null, msg));
            stream.on("end", () => callback(null, null));
            stream.on("error", err => callback(err, null));

            // TODO: Do we need to do anything with these two messages?

            stream.on("metadata", metadata => console.log("gRPC Metadata: " + JSON.stringify(metadata)));
            stream.on("status", status => console.log("gRPC Status: " + JSON.stringify(status)));
        }


        return GoogleTransport;

    })();


    const TracTransport = (function() {

        const GRPC_STATUS_HEADER = "grpc-status";
        const GRPC_MESSAGE_HEADER = "grpc-message";

        const LPM_PREFIX_LENGTH = 5;

        const WS_HEADERS = {
            "content-type": "application/grpc-web+proto",
            "accept": "application/grpc-web+proto",
            "x-grpc-web": 1,
        }

        function TracTransport(serviceName, protocol, host, port, options) {

            this.serviceName = serviceName;
            this.protocol = protocol;
            this.wsProtocol = protocol.replace("http", "ws");
            this.host = host;
            this.port = port;
            this.options = options;

            this.hostAddress = `${protocol}://${host}:${port}`;

            this.options.debug && console.log(`TracTransport created, host address = [${this.hostAddress}]`);

            // Empty RPC metadata for now
            this.requestMetadata = {};
            this.responseMetadata = {};
            this.response = null;

            this.ws = null;
            this.sendQueue = [];
            this.sendDone = false;
            this.rcvQueue = [];
            this.rcvFlag = 0;
            this.rcvLength = -1;
            this.rcvDone = false;
            this.finished = false;
        }

        TracTransport.prototype.setRpcInfo = function(method, callback) {

            this.options.debug && console.log("TracTransport setRpcInfo")

            this.methodName = method.name;

            this.methodType = (this.methodName in METHOD_TYPE_MAP)
                ? METHOD_TYPE_MAP[this.methodName]
                : grpc.MethodType.UNARY;

            this.serverStreaming =
                this.methodType === grpc.MethodType.SERVER_STREAMING ||
                this.methodType === grpc.MethodType.BIDI_STREAMING;

            this.clientStreaming =
                this.methodType === grpc.MethodType.CLIENT_STREAMING ||
                this.methodType === grpc.MethodType.BIDI_STREAMING;

            const methodProtocol = this.methodType === grpc.MethodType.UNARY
                ? this.protocol
                : this.wsProtocol;

            this.methodUrl = `${methodProtocol}://${this.host}:${this.port}/${this.serviceName}/${this.methodName}`;

            this.callback = callback;
        }


        // -------------------------------------------------------------------------------------------------------------
        // PROTOBUF.JS API
        // -------------------------------------------------------------------------------------------------------------


        TracTransport.prototype.rpcImpl = function(method, request, callback) {

            this.options.debug && console.log("TracTransport rpcImpl")

            if (this.ws == null) {
                this.setRpcInfo(method, callback)
                this._wsConnect();
            }

            if (method !== null && method.name !== this.methodName) {  // or request is complete / failed
                // todo error: streaming transport can only be used for a single request
            }

            if (request === null)
                this._wsSendEos();
            else if (this.clientStreaming)
                this._wsSendMessage(request);
            else {
                this._wsSendMessage(request);
                this._wsSendEos();
            }
        }


        // -------------------------------------------------------------------------------------------------------------
        // WEB SOCKETS API
        // -------------------------------------------------------------------------------------------------------------


        TracTransport.prototype._wsConnect = function() {

            this.options.debug && console.log("TracTransport _wsConnect")

            const ws = new WebSocket(this.methodUrl, "grpc-websockets");
            ws.binaryType = "arraybuffer";

            ws.onopen = this._wsHandleOpen.bind(this);
            ws.onclose = this._wsHandleClose.bind(this);
            ws.onmessage = this._wsHandleMessage.bind(this);
            ws.onerror = this._wsHandlerError.bind(this);

            this.ws = ws;
        }

        TracTransport.prototype._wsHandleOpen = function () {

            this.options.debug && console.log("TracTransport _wsHandleOpen")

            // This is to match the protocol used by improbable eng
            // In their implementation, header frame is not wrapped as an LPM

            const headerMsg = this._encodeHeaders(WS_HEADERS);
            this.ws.send(headerMsg);

            this._wsFlushSendQueue();
        }

        TracTransport.prototype._wsHandleClose = function(event) {

            this.options.debug && console.log("TracTransport _wsHandleClose")

            if (this.finished) {

                // This request is already finished, don't call a handler that would call back to client code
                // If there is a problem, put it in the console instead

                if (!event.wasClean)
                    console.log(`Connection did not close cleanly: ${event.reason} (websockets code ${event.code})`)
            }

            else if (event.wasClean)
                this._handleComplete();

            else {

                const status = grpc.StatusCode.UNKNOWN;
                const message = `Connection did not close cleanly: ${event.reason} (websockets code ${event.code})`
                this._handlerError(status, message);
            }
        }

        TracTransport.prototype._wsHandleMessage = function(event) {

            this.options.debug && console.log("TracTransport _wsHandleMessage")

            this._receiveFrame(event.data)
        }

        TracTransport.prototype._wsHandlerError = function(event) {

            this.options.debug && console.log("TracTransport _wsHandlerError")

            this.ws.close();

            const status = grpc.StatusCode.UNKNOWN;
            const message = event.reason;
            this._handlerError(status, message);
        }


        // -------------------------------------------------------------------------------------------------------------
        // MESSAGE SENDING
        // -------------------------------------------------------------------------------------------------------------


        TracTransport.prototype._wsSendMessage = function(msg) {

            this.options.debug && console.log("TracTransport _wsSendMessage")

            const frame = this._wrapMessage(msg, /* wsProtocol = */ true)

            if (this.ws != null && this.ws.readyState === WebSocket.OPEN)
                this.ws.send(frame);
            else
                this.sendQueue.push(frame);
        }

        TracTransport.prototype._wsSendEos = function() {

            this.options.debug && console.log("TracTransport _wsSendEos")

            const frame = new Uint8Array([1]);

            if (this.ws != null && this.ws.readyState === WebSocket.OPEN)
                this.ws.send(frame);
            else
                this.sendQueue.push(frame);

            this.sendDone = true;
        }

        TracTransport.prototype._wsFlushSendQueue = function () {

            this.options.debug && console.log("TracTransport _wsFlushSendQueue")

            while (this.sendQueue.length > 0 && this.ws.readyState === WebSocket.OPEN) {

                const frame = this.sendQueue.shift();

                this.options.debug && console.log("TracTransport _wsFlushSendQueue: frame size " + frame.byteLength)

                this.ws.send(frame);
            }
        }

        TracTransport.prototype._encodeHeaders = function(headers) {

            let headerText = "";

            Object.keys(headers).forEach(key => {
                const value = headers[key];
                headerText += `${key}: ${value}\r\n`;
            });

            headerText += '\r\n';

            const encoder = new TextEncoder()
            return encoder.encode(headerText);
        }

        TracTransport.prototype._wrapMessage = function(msg, wsProtocol = false) {

            const wsEos = 0;
            const wsPrefix = wsProtocol ? 1 : 0;
            const flag = 0;
            const length = msg.byteLength;

            this.options.debug && console.log(`TracTransport _wrapMessage: ws = ${wsProtocol}, eos = ${wsEos}, compress = ${flag}, length = ${length}`)

            const lpm = new Uint8Array(msg.byteLength + LPM_PREFIX_LENGTH + wsPrefix)
            const lpmView = new DataView(lpm.buffer, 0, LPM_PREFIX_LENGTH + wsPrefix);

            if (wsProtocol)
                lpmView.setUint8(0, wsEos);

            lpmView.setUint8(0 + wsPrefix, flag);
            lpmView.setUint32(1 + wsPrefix, length, false);
            lpm.set(msg, LPM_PREFIX_LENGTH + wsPrefix)

            return lpm
        }


        // -------------------------------------------------------------------------------------------------------------
        // MESSAGE RECEIVING
        // -------------------------------------------------------------------------------------------------------------


        TracTransport.prototype._receiveFrame = function(frame) {

            this.options.debug && console.log(`TracTransport _wsReceiveFrame, bytes = [${frame.byteLength}]`)

            this.rcvQueue.push(new Uint8Array(frame));

            while (this.rcvQueue.length > 0) {

                const queueSize = this.rcvQueue
                    .map(buf => buf.byteLength)
                    .reduce((x, y) => x + y, 0);

                if (this.rcvLength < 0) {

                    if (queueSize < LPM_PREFIX_LENGTH)
                        return;

                    const prefix = this._pollReceiveQueue(LPM_PREFIX_LENGTH);
                    const prefixView = new DataView(prefix.buffer);
                    this.rcvFlag = prefixView.getUint8(0);
                    this.rcvLength = prefixView.getUint32(1, false);
                }

                if (queueSize < LPM_PREFIX_LENGTH + this.rcvLength)
                    return;

                const msg = this._pollReceiveQueue(this.rcvLength);

                const compress = this.rcvFlag & 1;
                const trailers = this.rcvFlag & (1 << 7);

                // todo: handle decompression if required
                if (compress)
                    throw new Error("Incoming messages is compressed, but compression is not supported yet");

                this.rcvFlag = 0;
                this.rcvLength = -1;

                if (trailers)
                    this._receiveHeaders(msg)
                else
                    this._receiveMessage(msg)
            }
        }

        TracTransport.prototype._pollReceiveQueue = function(nBytes) {

            let frame0 = this._pollReceiveQueueUpto(nBytes);

            if (frame0.byteLength === nBytes) {
                return frame0;
            }

            const buffer = new Uint8Array(nBytes);
            let offset = frame0.byteLength;
            buffer.set(frame0);

            while (offset < nBytes) {

                let frame0 = this._pollReceiveQueueUpto(nBytes);
                buffer.set(frame0, offset);

                offset += frame0.byteLength;
            }

            return buffer;
        }

        TracTransport.prototype._pollReceiveQueueUpto = function(nBytes) {

            let frame = this.rcvQueue.shift();

            if (frame.byteLength <= nBytes) {
                return frame;
            }

            if (frame.byteLength > nBytes) {

                // May need to use slice if bytesOffset doesn't stick
                // new Uint8Array(frame.buffer.slice(nBytes));

                const remaining = new Uint8Array(frame.buffer, frame.byteOffset + nBytes);
                this.rcvQueue.unshift(remaining);

                return new Uint8Array(frame.buffer, frame.byteOffset, nBytes);
            }
        }

        TracTransport.prototype._receiveMessage = function(msg) {

            this.options.debug && console.log(`TracTransport _receiveMessage, bytes = [${msg.byteLength}]`)

            if (this.serverStreaming)
                this.callback(null, msg);
            else {
                this.response = msg;
            }
        }

        TracTransport.prototype._receiveHeaders = function (msg) {

            this.options.debug && console.log("TracTransport _receiveHeaders")

            const decoder = new TextDecoder();
            const headerText = decoder.decode(msg);
            const headerLines = headerText.split("\r\n")

            headerLines.forEach(line => {

                if (line !== "") {

                    // TODO: Should we filter out special HTTP values like :status: ?

                    const sep = line.indexOf(":", 1);
                    const key = line.substring(0, sep);
                    const value = line.substring(sep + 1).trim();

                    this.options.debug && console.log(`Response header [${key}] = [${value}]`);

                    this.responseMetadata[key] = value;
                }
            })

            if (GRPC_STATUS_HEADER in this.responseMetadata) {
                this.rcvDone = true;
                this._handleComplete();
            }
        }


        // -------------------------------------------------------------------------------------------------------------
        // RESULT HANDLING
        // -------------------------------------------------------------------------------------------------------------


        TracTransport.prototype._handleComplete = function() {

            this.options.debug && console.log("TracTransport _handleComplete")

            if (this.finished) {
                this.options.debug && console.log("_handleComplete called after the method already finished");
                return;
            }

            if (!this.sendDone || !this.rcvDone) {

                const code = grpc.StatusCode.UNKNOWN;
                const message = "The connection was closed before communication finished";
                const error = {status: code, message: message, metadata: this.responseMetadata};
                this.callback(error, null);
            }
            else if (!(GRPC_STATUS_HEADER in this.responseMetadata)) {

                const code = grpc.StatusCode.UNKNOWN;
                const message = "The connection was closed before a response was received";
                const error = {status: code, message: message, metadata: this.responseMetadata};
                this.callback(error, null);
            }
            else {

                const grpcStatus = Number.parseInt(this.responseMetadata[GRPC_STATUS_HEADER]);
                const grpcMessage = this.responseMetadata[GRPC_MESSAGE_HEADER];

                if (grpcStatus !== grpc.StatusCode.OK) {
                    const error = {status: grpcStatus, message: grpcMessage, metadata: this.responseMetadata}
                    this.callback(error, null);
                }

                else if (this.serverStreaming)
                    this.callback(null, null);

                else if (this.response != null)
                    this.callback(null, this.response);

                else {
                    const status = grpc.StatusCode.UNKNOWN;
                    const message = "The server replied but did not send a valid response";  // todo message
                    const error = {status: status, message: message, metadata: this.responseMetadata}
                    this.callback(error, null);
                }
            }

            this.ws.close();
            this.finished = true;
        }

        TracTransport.prototype._handlerError = function (status, message) {

            this.options.debug && console.log("TracTransport _handlerError", status, message)

            if (this.finished) {
                this.options.debug && console.log("_handlerError called after the method already finished");
                return;
            }

            // In an error condition make sure not to keep a reference to any data

            this.finished = true;
            this.sendQueue = []
            this.rcvQueue = []

            const error = { status: status, message: message }

            this.callback(error, null);
        }

        return TracTransport;

    })();


    function createRpcImpl(serviceName, protocol, host, port, options) {

        const transport = options.transport || DEFAULT_TRANSPORT;
        let transportImpl;

        if (transport === "google") transportImpl = new GoogleTransport(serviceName, protocol, host, port, options);
        else if (transport === "trac") transportImpl = new TracTransport(serviceName, protocol, host, port, options);
        else throw new Error(`Unsupported option for transport: [${transport}]`);

        const rpcImpl = transportImpl.rpcImpl.bind(transportImpl);
        rpcImpl._transport = transportImpl;

        return rpcImpl;
    }


    $root.tracdap.setup = (function() {

        /**
         * Namespace setup.
         * @memberof tracdap
         * @namespace
         */
        const setup = {};


        // We need the following typedef for rpcImpl methods:
        // @typedef {typeof $protobuf.rpc.Service} tracdap.setup.ServiceType

        // The JSDoc generator for .d.ts files cannot handle type names with spaces
        // So, use a placeholder instead, the real type is substituted in by api_builder.js

        /**
         * Type declaration for gRPC service classes
         * @typedef {$SERVICE_TYPE} tracdap.setup.ServiceType
         */

        /**
         * Create an rpcImpl that connects to a specific target
         *
         * <p>Deprecated since version 0.5.6, use transportForTarget() instead</p>
         *
         * @function rpcImplForTarget
         * @memberof tracdap.setup
         *
         * @deprecated Since version 0.5.6, use transportForTarget() instead
         *
         * @param serviceClass {ServiceType} The service class to create an rpcImpl for
         * @param {string} protocol The protocol to use for connection (either "http" or "https")
         * @param {string} host The host to connect to
         * @param {number} port The port to connect to
         *
         * @param {object=} options Options to control the behaviour of the transport
         * @param {("google"|"trac")} options.transport Controls which transport implementation to use for gRPC-Web
         * @param {boolean} options.debug Turn on debug logging
         *
         * @returns {$protobuf.RPCImpl} An rpcImpl function that can be used with the specified service class
         */
        setup.rpcImplForTarget = function(serviceClass, protocol, host, port, options = {}) {

            if (!serviceClass.hasOwnProperty("_serviceName"))
                throw new Error("Service class must specify gRPC service in _serviceName (this is a bug)")

            return createRpcImpl(serviceClass._serviceName, protocol, host, port, options);
        }

        /**
         * Create an rpcImpl for use in a web browser, requests will be sent to the page origin server
         *
         * <p>Deprecated since version 0.5.6, use transportForBrowser() instead</p>
         *
         * @function rpcImplForBrowser
         * @memberof tracdap.setup
         *
         * @deprecated Since version 0.5.6, use transportForBrowser() instead<
         *
         * @param serviceClass {ServiceType} The service class to create an rpcImpl for
         *
         * @param {object=} options Options to control the behaviour of the transport
         * @param {("google"|"trac")} options.transport Controls which transport implementation to use for gRPC-Web
         * @param {boolean} options.debug Turn on debug logging
         *
         * @returns {$protobuf.RPCImpl} An rpcImpl function that can be used with the specified service class
         */
        setup.rpcImplForBrowser = function(serviceClass, options = {}) {

            if (!serviceClass.hasOwnProperty("_serviceName"))
                throw new Error("Service class must specify gRPC service in _serviceName (this is a bug)")

            const protocol = window.location.protocol;
            const host = window.location.host;
            const port = window.location.port;

            return createRpcImpl(serviceClass._serviceName, protocol, host, port, options);
        }

        /**
         * Create an rpcImpl that connects to a specific target
         *
         * @function transportForTarget
         * @memberof tracdap.setup
         *
         * @param serviceClass {ServiceType} The service class to create an rpcImpl for
         * @param {string} protocol The protocol to use for connection (either "http" or "https")
         * @param {string} host The host to connect to
         * @param {number} port The port to connect to
         *
         * @param {object=} options Options to control the behaviour of the transport
         * @param {("google"|"trac")} options.transport Controls which transport implementation to use for gRPC-Web
         * @param {boolean} options.debug Turn on debug logging
         *
         * @returns {$protobuf.RPCImpl} An rpcImpl function that can be used with the specified service class
         */
        setup.transportForTarget = function(serviceClass, protocol, host, port, options = {}) {

            if (!serviceClass.hasOwnProperty("_serviceName"))
                throw new Error("Service class must specify gRPC service in _serviceName (this is a bug)")

            return createRpcImpl(serviceClass._serviceName, protocol, host, port, options);
        }

        /**
         * Create an rpcImpl for use in a web browser, requests will be sent to the page origin server
         *
         * @function transportForBrowser
         * @memberof tracdap.setup
         *
         * @param serviceClass {ServiceType} The service class to create an rpcImpl for
         *
         * @param {object=} options Options to control the behaviour of the transport
         * @param {("google"|"trac")} options.transport Controls which transport implementation to use for gRPC-Web
         * @param {boolean} options.debug Turn on debug logging
         *
         * @returns {$protobuf.RPCImpl} An rpcImpl function that can be used with the specified service class
         */
        setup.transportForBrowser = function(serviceClass, options = {}) {

            if (!serviceClass.hasOwnProperty("_serviceName"))
                throw new Error("Service class must specify gRPC service in _serviceName (this is a bug)")

            const protocol = window.location.protocol;
            const host = window.location.host;
            const port = window.location.port;

            return createRpcImpl(serviceClass._serviceName, protocol, host, port, options);
        }

        /**
         * Construct a streaming client suitable for transferring large datasets or files
         *
         * For ordinary RPC calls you only need one client per service and you can call it as
         * many times as you like. The same is not true for streaming -to use a stream you
         * must create a unique client instance for each call. This is to stop stream events
         * from different calls getting mixed up on the same stream.
         *
         * You can use this method to get a stream instance from an existing client.
         *
         * @function newStream
         * @memberOf tracdap.setup
         *
         * @template TService extends $protobuf.rpc.Service
         * @param service {TService} An existing client that will be used to construct the stream
         *
         * @return {TService} A streaming client instance, ready to call
         */
        setup.newStream = function(service) {

            const serviceClass = service.constructor;

            const originalTransport = service.rpcImpl._transport;
            const protocol = originalTransport.protocol;
            const host = originalTransport.host;
            const port = originalTransport.port;
            const options = originalTransport.options;

            const rpcImpl = createRpcImpl(serviceClass._serviceName, protocol, host, port, options);

            return new serviceClass(rpcImpl);
        }

        return setup;

    })();

    const api_mapping = $API_MAPPING;

    $root.tracdap = {
        ...$root.tracdap,
        ...api_mapping
    };

    return $root;
});