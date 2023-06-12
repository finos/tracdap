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

import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.exception.ENetworkHttp;
import org.finos.tracdap.common.exception.EUnexpected;

import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.*;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class RestApiProxy extends Http2ChannelDuplexHandler {

    private static final Set<String> FILTER_REQUEST_HEADERS = Set.of(
            Http2Headers.PseudoHeaderName.METHOD.value().toString(),
            Http2Headers.PseudoHeaderName.PATH.value().toString(),
            HttpHeaderNames.CONTENT_TYPE.toString(),
            HttpHeaderNames.CONTENT_LENGTH.toString(),
            HttpHeaderNames.CONTENT_ENCODING.toString(),
            HttpHeaderNames.ACCEPT.toString());

    private static final Set<String> FILTER_RESPONSE_HEADERS = Set.of(
            Http2Headers.PseudoHeaderName.STATUS.value().toString(),
            HttpHeaderNames.CONTENT_TYPE.toString(),
            HttpHeaderNames.CONTENT_LENGTH.toString(),
            HttpHeaderNames.CONTENT_ENCODING.toString());

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final List<RestApiMethod<?, ?>> methods;
    private final Map<Http2FrameStream, RestApiCallState> callStateMap;


    public RestApiProxy(List<RestApiMethod<?, ?>> methods) {
        this.methods = methods;
        this.callStateMap = new HashMap<>();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        try {

            // REST proxy layer expects all messages to be HTTP/2 frames
            if (!(msg instanceof Http2Frame))
                throw new EUnexpected();

            // Allow control messages to pass through the REST proxy
            if (!(msg instanceof Http2StreamFrame)) {
                ReferenceCountUtil.retain(msg);
                ctx.write(msg, promise);
                return;
            }

            var frame = (Http2StreamFrame) msg;
            var stream = frame.stream();

            if (!callStateMap.containsKey(stream)) {
                var newState = new RestApiCallState(ctx, stream);
                callStateMap.put(stream, newState);
            }

            var state = callStateMap.get(stream);

            if (frame instanceof Http2HeadersFrame) {

                var headersFrame = (Http2HeadersFrame) frame;
                state.requestHeaders.add(headersFrame.headers());

                if (headersFrame.isEndStream())
                    dispatchRequest(state, ctx, promise);
                else
                    promise.setSuccess();
            }
            else if (frame instanceof Http2DataFrame) {

                var dataFrame = (Http2DataFrame) frame;

                if (dataFrame.content() != null && dataFrame.content().readableBytes() > 0) {
                    ReferenceCountUtil.retain(dataFrame.content());
                    state.requestContent.addComponent(true, dataFrame.content());
                }

                if (dataFrame.isEndStream())
                    dispatchRequest(state, ctx, promise);
                else
                    promise.setSuccess();
            }
            else {

                log.warn("Unexpected request frame type {} will be dropped", frame.name());
                promise.setSuccess();
            }
        }
        finally {

            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

        try {

            // REST proxy layer expects all messages to be HTTP/2 frames
            if (!(msg instanceof Http2Frame))
                throw new EUnexpected();

            // Allow control messages to pass through the REST proxy
            if (!(msg instanceof Http2StreamFrame)) {
                ReferenceCountUtil.retain(msg);
                ctx.fireChannelRead(msg);
                return;
            }

            var frame = (Http2StreamFrame) msg;
            var stream = frame.stream();
            var state = callStateMap.get(stream);
            var unaryResponse = ! state.method.grpcMethod.isServerStreaming();

            if (frame instanceof Http2HeadersFrame) {

                var grpcFrame = (Http2HeadersFrame) frame;
                var grpcHeaders = grpcFrame.headers();

                state.responseHeaders.add(grpcHeaders);

                if (unaryResponse) {
                    if (grpcFrame.isEndStream())
                        dispatchUnaryResponse(state, ctx);
                }
                else {
                    dispatchStreamHeaders(state, ctx, grpcFrame.isEndStream());
                }
            }
            else if (frame instanceof Http2DataFrame) {

                var grpcFrame = (Http2DataFrame) frame;

                if (grpcFrame.content() != null && grpcFrame.content().readableBytes() > 0) {
                    ReferenceCountUtil.retain(grpcFrame.content());
                    state.responseContent.addComponent(true, grpcFrame.content());
                }

                if (unaryResponse) {
                    if (grpcFrame.isEndStream())
                        dispatchUnaryResponse(state, ctx);
                }
                else {
                    dispatchStreamContent(state, ctx);
                    if (grpcFrame.isEndStream())
                        dispatchStreamComplete(state, ctx);
                }
            }
            else {

                log.warn("Unexpected response frame type {} will be dropped", frame.name());
            }
        }
        finally {

            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        log.error("There was an error", cause);

        super.exceptionCaught(ctx, cause);
    }

    private void dispatchRequest(RestApiCallState state, ChannelHandlerContext ctx, ChannelPromise promise) {

        try {

            var url = state.requestHeaders.path().toString();

            state.method = lookupMethod(state.requestHeaders);
            state.translator = state.method != null ? state.method.translator : null;

            if (state.method == null) {
                sendErrorResponse(state.stream, ctx, HttpResponseStatus.NOT_FOUND, "REST API METHOD NOT FOUND");
                promise.setFailure(new ENetworkHttp(HttpResponseStatus.NOT_FOUND.code(), "REST API METHOD NOT FOUND"));
                return;
            }

            // TODO: This content type checking should happen in the router for grpc and rest routes

            try {
                checkRequestHeaders(state);
            }
            catch (EInputValidation e) {
                sendErrorResponse(state.stream, ctx, HttpResponseStatus.NOT_ACCEPTABLE, e.getMessage());
                return;
            }

            var restHeaders = state.requestHeaders;
            var grpcHeaders = translateRequestHeaders(restHeaders, state);
            var grpcMessage = state.method.hasBody
                    ? state.translator.decodeRestRequest(url, state.requestContent)
                    : state.translator.decodeRestRequest(url);

            var lpm = state.translator.encodeGrpcRequest(grpcMessage, ctx.alloc());

            var headersFrame = new DefaultHttp2HeadersFrame(grpcHeaders).stream(state.stream);
            var dataFrame = new DefaultHttp2DataFrame(lpm, true).stream(state.stream);

            ctx.write(headersFrame);
            ctx.write(dataFrame, promise);
        }
        catch (EInputValidation error) {

            log.warn("Bad request in REST API: " + error.getMessage(), error);

            // Validation errors can occur in the request builder
            // These are from extracting fields from the URL, or translating JSON -> protobuf
            // In this case, send some helpful information back about what cause the failure

            var errorCode = HttpResponseStatus.BAD_REQUEST;
            var errorMessage = error.getLocalizedMessage();

            sendErrorResponse(state.stream, ctx, errorCode, errorMessage);
            promise.setFailure(new ENetworkHttp(errorCode.code(), errorMessage));
        }
        finally {

            if (state.requestContent != null) {
                state.requestContent.release();
                state.requestContent = null;
            }
        }
    }

    private void dispatchUnaryResponse(RestApiCallState state, ChannelHandlerContext ctx) {

        try {

            var grpcHeaders = state.responseHeaders;
            var grpcContentLength = grpcHeaders.getInt(HttpHeaderNames.CONTENT_LENGTH);

            if (grpcContentLength != null && state.responseContent.readableBytes() != grpcContentLength)
                throw new EUnexpected();  // todo

            ByteBuf restResponse;

            if (state.responseContent.readableBytes() == 0) {
                restResponse = Unpooled.EMPTY_BUFFER;
            }
            else {
                var grpcMessage = state.translator.decodeGrpcResponse(state.responseContent);
                restResponse = state.translator.encodeRestResponse(grpcMessage);
            }

            var restHeaders = translateResponseHeaders(grpcHeaders, state, /* streaming = */ false);
            restHeaders.add(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(restResponse.readableBytes()));

            var headersFrame = new DefaultHttp2HeadersFrame(restHeaders).stream(state.stream);
            var dataFrame = new DefaultHttp2DataFrame(restResponse, true).stream(state.stream);

            ctx.fireChannelRead(headersFrame);
            ctx.fireChannelRead(dataFrame);
        }
        catch (Exception error) {

            log.warn("Bad request in REST API: " + error.getMessage(), error);

            // Validation errors can occur in the request builder
            // These are from extracting fields from the URL, or translating JSON -> protobuf
            // In this case, send some helpful information back about what cause the failure

            var errorCode = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            var errorMessage = error.getLocalizedMessage();

            sendErrorResponse(state.stream, ctx, errorCode, errorMessage);
        }
        finally {

            if (state.responseContent != null) {
                state.responseContent.release();
                state.responseContent = null;
            }
        }
    }

    private void dispatchStreamHeaders(RestApiCallState state, ChannelHandlerContext ctx, boolean eos) {

        // todo: check headers not already sent
        // if already sent and grpc-status != 0, break the stream to signal error

        var restHeaders = translateResponseHeaders(state.responseHeaders, state, true);
        var restFrame = new DefaultHttp2HeadersFrame(restHeaders, eos);
        ctx.fireChannelRead(restFrame);
    }

    private void dispatchStreamContent(RestApiCallState state, ChannelHandlerContext ctx) {

//        var restData = translateResponseData(state);
//        restData.forEach(ctx::fireChannelRead);

        // todo
    }

    private void dispatchStreamComplete(RestApiCallState state, ChannelHandlerContext ctx) {

        // todo: check grpc-status, if not ok break the stream

        ctx.fireChannelRead(new DefaultHttp2DataFrame(true));
    }

    private RestApiMethod<?, ?> lookupMethod(Http2Headers headers) {

        for (var method: this.methods) {

            var uri = URI.create(headers.path().toString());
            var httpMethod = HttpMethod.valueOf(headers.method().toString());
            var httpHeaders = new DefaultHttpHeaders();  // TODO: Switch matcher to HTTP/2 headers?

            if (method.matcher.matches(uri, httpMethod, httpHeaders))
                return method;
        }

        return null;
    }

    private Http2Headers translateRequestHeaders(Http2Headers restHeaders, RestApiCallState state) {

        var grpcHeaders = new DefaultHttp2Headers();

        // Bring across all response headers that do not have special handling

        for (var header : restHeaders) {
            if (!FILTER_REQUEST_HEADERS.contains(header.getKey().toString()))
                grpcHeaders.add(header.getKey(), header.getValue());
        }

        // gRPC method

        var grpcMethod = state.method.grpcMethod;
        var httpPath = String.format("/%s/%s", grpcMethod.getService().getFullName(), grpcMethod.getName());

        grpcHeaders.method(HttpMethod.POST.asciiName());
        grpcHeaders.path(httpPath);
        grpcHeaders.add(HttpHeaderNames.TE, "trailers");

        // Content headers

        grpcHeaders.add(HttpHeaderNames.CONTENT_TYPE, "application/grpc+proto");
        grpcHeaders.add(HttpHeaderNames.ACCEPT, "application/grpc+proto");

        return grpcHeaders;
    }

    private Http2Headers translateResponseHeaders(Http2Headers grpcHeaders, RestApiCallState state, boolean streaming) {

        var restHeaders = new DefaultHttp2Headers();

        // Bring across all response headers that do not have special handling

        for (var header : grpcHeaders) {
            if (!FILTER_RESPONSE_HEADERS.contains(header.getKey().toString()))
                restHeaders.add(header.getKey(), header.getValue());
        }

        // Figure out the right HTTP response code

        var httpStatus = HttpResponseStatus.parseLine(grpcHeaders.status());
        var grpcStatus = grpcHeaders.getInt("grpc-status");
        var grpcMessage = grpcHeaders.get("grpc-message");

        // If the request fails at the HTTP level, the HTTP error is the response code
        if (httpStatus.code() != HttpResponseStatus.OK.code()) {
            restHeaders.status(httpStatus.toString());
        }
        // If gRPC status code is available, translate that
        else if (grpcStatus != null) {
            var grpcStatusCode = Status.fromCodeValue(grpcStatus).getCode();
            var restStatusCode = state.method.translator.translateGrpcErrorCode(grpcStatusCode);
            var restMessage = grpcMessage != null ? grpcMessage.toString() : restStatusCode.reasonPhrase();
            var restStatus = new HttpResponseStatus(restStatusCode.code(), restMessage);
            restHeaders.status(restStatus.toString());
        }
        // For streaming responses gRPC status is not known until the stream completes
        // Unless there is an early error we have to send OK to start sending content
        else if (streaming) {
            restHeaders.status(HttpResponseStatus.OK.toString());
        }
        // Otherwise if the status is not known that is an error
        else {
            var restStatusCode = HttpResponseStatus.BAD_REQUEST;
            var restStatus = new HttpResponseStatus(restStatusCode.code(), "RESPONSE STATUS UNKNOWN");
            restHeaders.status(restStatus.toString());
        }

        // TODO: Content headers

        restHeaders.add(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");

        return restHeaders;
    }


    private void checkRequestHeaders(RestApiCallState state) {

        var restHeaders = state.requestHeaders;

        // POST methods have a JSON payload for the encoded gRPC request type
        // GET methods do not have a body, the request is entirely built from the URL

        if (state.method.hasBody) {

            if (!restHeaders.contains(HttpHeaderNames.CONTENT_TYPE))
                throw new EInputValidation("Missing required HTTP header [" + HttpHeaderNames.CONTENT_TYPE + "]");

            var contentTypeHeader = restHeaders.get(HttpHeaderNames.CONTENT_TYPE).toString().split(";")[0];

            if (!contentTypeHeader.equals("application/json"))
                throw new EInputValidation("Invalid [content-type] header (expected application/json for REST calls)");
        }
        else {

            if (restHeaders.contains(HttpHeaderNames.CONTENT_TYPE))
                throw new EInputValidation("Unexpected HTTP header [" + HttpHeaderNames.CONTENT_TYPE + "]");
        }

        // Unary methods return a JSON encoding of the gRPC response type
        // Server streaming methods are for downloads and must accept whatever type the server sends
        // E.g. downloading a file, the response content type will depend on the type of the file

        if (!state.method.grpcMethod.isServerStreaming()) {

            if (!restHeaders.contains(HttpHeaderNames.ACCEPT))
                throw new EInputValidation("Missing required HTTP header [" + HttpHeaderNames.ACCEPT + "]");

            var acceptHeader = restHeaders.get(HttpHeaderNames.ACCEPT).toString().split(";")[0];

            if (!acceptHeader.equals("application/json"))
                throw new EInputValidation("Invalid [accept] header (expected application/json for REST calls)");
        }
        else {

            if (restHeaders.contains(HttpHeaderNames.ACCEPT))
                throw new EInputValidation("Unexpected HTTP header [" + HttpHeaderNames.ACCEPT + "]");
        }
    }

    private void sendErrorResponse(
            Http2FrameStream stream, ChannelHandlerContext ctx,
            HttpResponseStatus errorStatus, String errorMessage) {

        var headers = new DefaultHttp2Headers();
        headers.status(errorStatus.toString());

        if (errorMessage == null || errorMessage.isEmpty()) {

            var headersFrame = new DefaultHttp2HeadersFrame(headers, true).stream(stream);
            ctx.fireChannelRead(headersFrame);
            ctx.fireChannelReadComplete();
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
            ctx.fireChannelReadComplete();
        }
    }

    private static class RestApiCallState {

        Http2FrameStream stream;

        RestApiMethod<?, ?> method;
        RestApiTranslator translator;

        Http2Headers requestHeaders;
        CompositeByteBuf requestContent;
        Http2Headers responseHeaders;
        CompositeByteBuf responseContent;

        RestApiCallState(ChannelHandlerContext ctx, Http2FrameStream stream) {

            this.stream = stream;

            this.requestHeaders = new DefaultHttp2Headers();
            this.requestContent = ctx.alloc().compositeBuffer();
            this.responseHeaders = new DefaultHttp2Headers();
            this.responseContent = ctx.alloc().compositeBuffer();
        }
    }
}
