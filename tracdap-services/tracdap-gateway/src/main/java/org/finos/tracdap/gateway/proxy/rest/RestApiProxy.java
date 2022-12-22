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

package org.finos.tracdap.gateway.proxy.rest;

import org.finos.tracdap.common.auth.external.AuthHelpers;
import org.finos.tracdap.common.auth.internal.GrpcClientAuth;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.exception.EUnexpected;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;

import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.*;
import io.netty.util.concurrent.EventExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RestApiProxy extends Http2ChannelDuplexHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String grpcHost;
    private final short grpcPort;
    private final List<RestApiMethod<?, ?, ?>> methods;

    private final EventExecutor executor;
    private ManagedChannel serviceChannel;

    private final Map<Http2FrameStream, RestApiCallState> callStateMap;


    public RestApiProxy(String grpcHost, short grpcPort, List<RestApiMethod<?, ?, ?>> methods, EventExecutor executor) {

        this.grpcHost = grpcHost;
        this.grpcPort = grpcPort;
        this.methods = methods;

        this.executor = executor;

        this.callStateMap = new HashMap<>();
    }

    @Override
    protected void handlerAdded0(ChannelHandlerContext ctx) {

        serviceChannel = ManagedChannelBuilder.forAddress(grpcHost, grpcPort)
                .userAgent("TRAC/Gateway")
                .usePlaintext()
                .disableRetry()
                .executor(executor)
                .build();
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) {

        serviceChannel.shutdown();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        try {

            // REST proxy layer expects all messages to be HTTP/2 frames
            if (!(msg instanceof Http2Frame))
                throw new EUnexpected();

            var frame = (Http2Frame) msg;

            if (frame instanceof Http2HeadersFrame) {

                var headersFrame = (Http2HeadersFrame) frame;
                var stream = headersFrame.stream();

                if (!callStateMap.containsKey(stream)) {

                    var method = lookupMethod(headersFrame);
                    var headers = headersFrame.headers();

                    if (method == null) {

                        log.warn("PROXY REST CALL: {} {} ! NOT MAPPED",
                                headersFrame.headers().method(),
                                headersFrame.headers().path());

                        var httpCode = HttpResponseStatus.NOT_FOUND;
                        var httpContent = "REST API NOT MAPPED";

                        sendErrorResponse(stream, ctx, httpCode, httpContent);
                        return;
                    }
                    else {

                        log.info("PROXY REST CALL: {} {} -> {}",
                                headers.method(),
                                headers.path(),
                                method.grpcMethod.getFullMethodName());

                        var callState = new RestApiCallState();
                        callState.method = method;
                        callState.requestHeaders = new DefaultHttp2Headers();
                        callState.requestContent = ctx.alloc().compositeBuffer();
                        callState.options = CallOptions.DEFAULT;
                        callState.stream = stream;

                        var authToken = AuthHelpers.getAuthToken(headers);
                        callState.options = GrpcClientAuth.applyIfAvailable(callState.options, authToken);

                        callStateMap.put(stream, callState);
                    }
                }

                var callState = callStateMap.get(stream);
                callState.requestHeaders.add(headersFrame.headers());

                if (headersFrame.isEndStream())
                    dispatchUnaryRequest(callState.method, callState, ctx);
            }
            else if (frame instanceof Http2DataFrame) {

                var dataFrame = (Http2DataFrame) frame;
                var stream = dataFrame.stream();

                var callState = callStateMap.get(stream);

                // TODO: Check call state and stream are good
                if (dataFrame.content() != null && dataFrame.content().readableBytes() > 0)
                    callState.requestContent.addComponent(true, dataFrame.content());

                if (dataFrame.isEndStream())
                    dispatchUnaryRequest(callState.method, callState, ctx);
            }
            else {

                log.warn("Unexpected frame type {} will be dropped", frame.name());
            }
        }
        finally {

            // RestApiProxy uses the HTTP/2 codec and runs on an embedded channel
            // We are intercepting messages and then making calls to gRPC using the gRPC client
            // However, in order for the HTTP/2 codec to work, messages need to propagate down to the codec handler
            // So, instead of releasing the message, pass it on to the next handler
            // And the let the HTTP/2 codec at the end of the pipeline release it

            ctx.write(msg);
        }
    }

    private RestApiMethod<?, ?, ?> lookupMethod(Http2HeadersFrame headers) {

        for (var method: this.methods) {

            var uri = URI.create(headers.headers().path().toString());
            var httpMethod = HttpMethod.valueOf(headers.headers().method().toString());
            var httpHeaders = new DefaultHttpHeaders();  // TODO: Switch matcher to HTTP/2 headers?

            if (method.matcher.matches(uri, httpMethod, httpHeaders))
                return method;
        }

        return null;
    }

    private <TRequest extends Message, TRequestBody extends Message, TResponse extends Message>
    void dispatchUnaryRequest(
            RestApiMethod<TRequest, TRequestBody, TResponse> method,
            RestApiCallState callState,
            ChannelHandlerContext ctx) {

        try {
            var restUrlPath = callState.requestHeaders.path().toString();
            TRequest proxyRequest;

            if (method.hasBody) {
                var requestBody = method.translator.translateRequestBody(callState.requestContent);
                proxyRequest = method.translator.translateRequest(restUrlPath, requestBody);
            }
            else {
                proxyRequest = method.translator.translateRequest(restUrlPath);
            }

            var serviceCall = serviceChannel.newCall(method.grpcMethod, callState.options);
            var proxyCall = ClientCalls.futureUnaryCall(serviceCall, proxyRequest);

            var callback = new UnaryCallback<>(method, callState, ctx);
            Futures.addCallback(proxyCall, callback, executor);
        }
        catch (EInputValidation error) {

            log.warn("Bad request in REST API: " + error.getMessage(), error);

            // Validation errors can occur in the request builder
            // These are from extracting fields from the URL, or translating JSON -> protobuf
            // In this case, send some helpful information back about what cause the failure

            var errorCode = HttpResponseStatus.BAD_REQUEST;
            var errorMessage = error.getLocalizedMessage();

            sendErrorResponse(callState.stream, ctx, errorCode, errorMessage);
        }
        finally {

            if (callState.requestContent != null) {
                callState.requestContent.release();
                callState.requestContent = null;
            }
        }
    }

    private class UnaryCallback <
            TRequest extends Message,
            TRequestBody extends Message,
            TResponse extends Message>
            implements FutureCallback<TResponse> {

        private final RestApiMethod<TRequest, TRequestBody, TResponse> method;
        private final RestApiCallState callState;
        private final ChannelHandlerContext ctx;

        UnaryCallback(
                RestApiMethod<TRequest, TRequestBody, TResponse> method,
                RestApiCallState callState,
                ChannelHandlerContext ctx) {

            this.method = method;
            this.callState = callState;
            this.ctx = ctx;
        }

        @Override
        public void onSuccess(TResponse result) {

            try {

                log.info("PROXY REST CALL SUCCEEDED: {}", method.grpcMethod.getFullMethodName());

                var json = JsonFormat.printer().print(result);
                var content = ctx.alloc().buffer();
                content.writeBytes(json.getBytes(StandardCharsets.UTF_8));

                var headers = new DefaultHttp2Headers();
                headers.status(HttpResponseStatus.OK.toString());
                headers.set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
                headers.setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

                var headersFrame = new DefaultHttp2HeadersFrame(headers);
                var dataFrame = new DefaultHttp2DataFrame(content, true);

                ctx.fireChannelRead(headersFrame);
                ctx.fireChannelRead(dataFrame);
            }
            catch (InvalidProtocolBufferException e) {

                onFailure(e);
            }
        }

        @Override
        public void onFailure(Throwable error) {

            log.error("PROXY REST CALL FAILED: {} {}", method.grpcMethod.getFullMethodName(), error.getMessage());

            if (error instanceof StatusRuntimeException) {

                var grpcError = (StatusRuntimeException) error;
                var httpCode = method.translator.translateGrpcErrorCode(grpcError);
                var httpContent = method.translator.translateGrpcErrorMessage(grpcError);

                sendErrorResponse(callState.stream, ctx, httpCode, httpContent);
            }
            else {

                var httpCode = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                var httpContent = "Unexpected error in REST proxy communicating with gRPC service";

                sendErrorResponse(callState.stream, ctx, httpCode, httpContent);
            }

        }
    }

    private void sendErrorResponse(
            Http2FrameStream stream, ChannelHandlerContext ctx,
            HttpResponseStatus errorStatus, String errorMessage) {

        var headers = new DefaultHttp2Headers();
        headers.status(errorStatus.toString());
        headers.set(":version", "HTTP/2.0");

        if (errorMessage == null || errorMessage.isEmpty()) {

            var headersFrame = new DefaultHttp2HeadersFrame(headers, true).stream(stream);
            ctx.fireChannelRead(headersFrame);
        }
        else {

            // Putting error message in response body for now
            // It may be more appropriate in a header

            var content = ctx.alloc().buffer();
            content.writeCharSequence(errorMessage, StandardCharsets.UTF_8);

            headers.set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            headers.setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

            var headersFrame = new DefaultHttp2HeadersFrame(headers, false).stream(stream);
            var dataFrame = new DefaultHttp2DataFrame(content, true).stream(stream);

            ctx.fireChannelRead(headersFrame);
            ctx.fireChannelRead(dataFrame);
        }
    }

    private static class RestApiCallState {

        RestApiMethod<?, ?, ?> method;

        Http2Headers requestHeaders;
        CompositeByteBuf requestContent;
        CallOptions options;

        Http2FrameStream stream;
        boolean receiving = false;
    }
}
