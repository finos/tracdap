/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.gateway.proxy;

import com.accenture.trac.common.exception.EInputValidation;
import com.accenture.trac.common.exception.EUnexpected;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.gson.stream.MalformedJsonException;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;

import io.grpc.*;
import io.grpc.stub.ClientCalls;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;


public class RestApiUnaryHandler<
        TRequest extends Message,
        TRequestBody extends Message,
        TResponse extends Message>
        extends ChannelInboundHandlerAdapter {

    private final Logger log;

    private final String serviceHost;
    private final int servicePort;

    private final MethodDescriptor<TRequest, TResponse> grpcMethod;
    private final RestApiRequestBuilder<TRequest> requestBuilder;
    private final TRequestBody blankRequestBody;
    private final boolean hasBody;

    private boolean gotHeader;
    private HttpRequest clientRequest;
    private CompositeByteBuf clientRequestContent;
    private ManagedChannel serviceChannel;


    public RestApiUnaryHandler(
            String serviceHost, int servicePort,
            MethodDescriptor<TRequest, TResponse> grpcMethod,
            RestApiRequestBuilder<TRequest> requestBuilder,
            TRequestBody blankRequestBody) {

        this.log = LoggerFactory.getLogger(getClass());

        this.serviceHost = serviceHost;
        this.servicePort = servicePort;

        this.grpcMethod = grpcMethod;
        this.requestBuilder = requestBuilder;
        this.blankRequestBody = blankRequestBody;
        this.hasBody = true;
    }

    public RestApiUnaryHandler(
            String serviceHost, int servicePort,
            MethodDescriptor<TRequest, TResponse> grpcMethod,
            RestApiRequestBuilder<TRequest> requestBuilder) {

        this.log = LoggerFactory.getLogger(getClass());

        this.serviceHost = serviceHost;
        this.servicePort = servicePort;

        this.grpcMethod = grpcMethod;
        this.requestBuilder = requestBuilder;
        this.blankRequestBody = null;
        this.hasBody = false;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        try {

            if (!gotHeader) {

                if (!(msg instanceof HttpRequest))
                    throw new EUnexpected();

                clientRequest = (HttpRequest) msg;
                gotHeader = true;

                log.info("PROXY API CALL START: {}", grpcMethod.getFullMethodName());
            }

            if (hasBody && msg instanceof HttpContent) {

                var contentBuffer = ((HttpContent) msg).content();

                if (clientRequestContent == null) {
                    var allocator = contentBuffer.alloc();
                    clientRequestContent = allocator.compositeBuffer();
                }

                contentBuffer.retain();
                clientRequestContent.addComponent(true, contentBuffer);
            }

            if (msg instanceof LastHttpContent)
                dispatchRequest(ctx);
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void dispatchRequest(ChannelHandlerContext ctx) {

        try {
            TRequest proxyRequest;

            if (hasBody && clientRequestContent != null) {
                var requestBody = translateRequestBody(clientRequestContent);
                proxyRequest = requestBuilder.build(clientRequest.uri(), requestBody);
            }
            else
                proxyRequest = requestBuilder.build(clientRequest.uri());

            serviceChannel = ManagedChannelBuilder.forAddress(serviceHost, servicePort)
                    .userAgent("TRAC/Gateway")
                    .usePlaintext()
                    .disableRetry()
                    .executor(ctx.executor())
                    .build();

            var options = CallOptions.DEFAULT;
            var serviceCall = serviceChannel.newCall(grpcMethod, options);
            var proxyCall = ClientCalls.futureUnaryCall(serviceCall, proxyRequest);

            Futures.addCallback(proxyCall, new Callback(ctx), ctx.executor());
        }
        catch (EInputValidation error) {

            log.warn("Bad request in REST API: " + error.getMessage(), error);

            // Validation errors can occur in the request builder
            // These are from extracting fields from the URL, or translating JSON -> protobuf
            // In this case, send some helpful information back about what cause the failure

            var message = error.getLocalizedMessage();
            var content = ctx.alloc().buffer();
            content.writeCharSequence(message, StandardCharsets.UTF_8);

            var protocolVersion = clientRequest.protocolVersion();
            var response = new DefaultFullHttpResponse(protocolVersion, HttpResponseStatus.BAD_REQUEST, content);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

            ctx.writeAndFlush(response);
            ctx.close();
        }
        finally {

            if (clientRequestContent != null)
                clientRequestContent.release();
        }
    }

    private Message translateRequestBody(ByteBuf bodyBuffer) {

        try (var jsonStream = new ByteBufInputStream(bodyBuffer);
             var jsonReader = new InputStreamReader(jsonStream)) {

            var bodyBuilder = blankRequestBody.newBuilderForType();
            var jsonParser = JsonFormat.parser();
            jsonParser.merge(jsonReader, bodyBuilder);

            return bodyBuilder.build();
        }
        catch (InvalidProtocolBufferException e) {

            // Validation failures will go back to users (API users, i.e. application developers)
            // Strip out GSON class name from the error message for readability
            var detailMessage = e.getLocalizedMessage();
            var classNamePrefix = MalformedJsonException.class.getName() + ": ";

            if (detailMessage.startsWith(classNamePrefix))
                detailMessage = detailMessage.substring(classNamePrefix.length());

            var message = String.format(
                    "Invalid JSON input for type [%s]: %s",
                    blankRequestBody.getDescriptorForType().getName(),
                    detailMessage);

            log.warn(message);
            throw new EInputValidation(message, e);
        }
        catch (IOException e) {

            // Shouldn't happen, reader source is a buffer already held in memory
            log.error("Unexpected IO error reading from internal buffer", e);
            throw new EUnexpected();
        }
    }

    private class Callback implements FutureCallback<TResponse> {

        private final ChannelHandlerContext ctx;

        Callback(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void onSuccess(TResponse result) {

            try {

                log.info("PROXY API CALL SUCCEEDED: {}", grpcMethod.getFullMethodName());

                var json = JsonFormat.printer().print(result);
                var content = ctx.alloc().buffer();

                content.writeBytes(json.getBytes(StandardCharsets.UTF_8));

                var protocolVersion = clientRequest.protocolVersion();
                var response = new DefaultFullHttpResponse(protocolVersion, HttpResponseStatus.OK, content);
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

                ctx.writeAndFlush(response);
                ctx.close();

                serviceChannel.shutdown();
            }
            catch (InvalidProtocolBufferException e) {

                onFailure(e);
            }
        }

        @Override
        public void onFailure(Throwable error) {

            log.error("PROXY API CALL FAILED: {} {}", grpcMethod.getFullMethodName(), error.getMessage());

            HttpResponse response;

            if (error instanceof StatusRuntimeException)
                response = translateGrpcError((StatusRuntimeException) error);

            else {

                var protocolVersion = clientRequest.protocolVersion();
                response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            }

            ctx.writeAndFlush(response);
            ctx.close();

            serviceChannel.shutdown();
        }

        private HttpResponse translateGrpcError(StatusRuntimeException error) {

            var grpcCode = error.getStatus().getCode();
            HttpResponseStatus httpCode;

            switch (grpcCode) {

                case INVALID_ARGUMENT:
                    httpCode = HttpResponseStatus.BAD_REQUEST;
                    break;

                case NOT_FOUND:
                    httpCode = HttpResponseStatus.NOT_FOUND;
                    break;

                case ALREADY_EXISTS:
                    httpCode = HttpResponseStatus.CONFLICT;
                    break;

                case FAILED_PRECONDITION:
                    httpCode = HttpResponseStatus.PRECONDITION_FAILED;
                    break;

                default:

                    // For unrecognised errors, send error code 500 with no message
                    httpCode = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                    var protocolVersion = clientRequest.protocolVersion();
                    return new DefaultHttpResponse(protocolVersion, httpCode);

            }

            // For recognised gRPC errors, assume the error message says something helpful
            // Putting message in response body for now
            // It may be more appropriate in a header
            var message = error.getStatus().getDescription();
            var content = ctx.alloc().buffer();
            content.writeCharSequence(message, StandardCharsets.UTF_8);

            var protocolVersion = clientRequest.protocolVersion();
            var response = new DefaultFullHttpResponse(protocolVersion, httpCode, content);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

            return response;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        log.error("Unhandled error in REST API: " + cause.getMessage(), cause);

        var protocolVersion = clientRequest.protocolVersion();
        var response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);

        ctx.writeAndFlush(response);
        ctx.close();

        if (serviceChannel != null && !serviceChannel.isShutdown())
            serviceChannel.shutdownNow();
    }
}
