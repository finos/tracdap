/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.webserver;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.OrderedEventExecutor;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.exception.ENetworkHttp;
import org.finos.tracdap.common.exception.EUnexpected;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Flow;


public class Http1Server extends ChannelInboundHandlerAdapter {

    private static final List<HttpMethod> SUPPORTED_METHODS = List.of(HttpMethod.HEAD, HttpMethod.GET);

    private static final Logger log = LoggerFactory.getLogger(Http1Server.class);

    private final ContentServer contentServer;

    private HttpRequest currentRequest;

    public Http1Server(ContentServer contentServer) {
        this.contentServer = contentServer;
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        try {

            if (!(msg instanceof HttpObject))
                throw new EUnexpected();

            var processed = false;

            if (msg instanceof HttpRequest) {

                if (currentRequest != null) {
                    throw new EUnexpected();  // TODO: Err pipelining
                }

                currentRequest = (HttpRequest) msg;
                processed = true;
            }

            if (msg instanceof LastHttpContent) {
                if (currentRequest != null) {
                    serveRequest(ctx, currentRequest);
                    currentRequest = null;
                    processed = true;
                }
            }

            if (!processed) {

                if (currentRequest != null && !SUPPORTED_METHODS.contains(currentRequest.method())) {

                    var responseMessage = String.format(
                            "HTTP method not supported: %s [%s]",
                            currentRequest.method().name(),
                            currentRequest.uri());

                    var responseContent = Unpooled.copiedBuffer(responseMessage,StandardCharsets.UTF_8);

                    var response = new DefaultFullHttpResponse(
                            currentRequest.protocolVersion(),
                            HttpResponseStatus.METHOD_NOT_ALLOWED,
                            responseContent);

                    ctx.write(response);
                    ctx.flush();
                }
                else {

                    var responseMessage = "Request could not be understood";
                    var responseContent = Unpooled.copiedBuffer(responseMessage,StandardCharsets.UTF_8);
                    var response = new DefaultFullHttpResponse(
                            currentRequest.protocolVersion(),
                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            responseContent);

                    ctx.write(response);
                    ctx.flush();

                }

            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }

    private void serveRequest(ChannelHandlerContext ctx, HttpRequest request) {

        log.info("Request {} [{}]", request.method(), request.uri());

        if (request.method().equals(HttpMethod.HEAD)) {
            serveHeadRequest(ctx, request);
        }

        else if (request.method().equals(HttpMethod.GET)) {
            serveGetRequest(ctx, request);
        }

        else {
            var msg = String.format("Unsupported HTTP method in request: %s [%s] ", request.method(), request.uri());
            log.error(msg);
            throw new ENetworkHttp(HttpResponseStatus.METHOD_NOT_ALLOWED.code(), msg);
        }
    }

    private void serveHeadRequest(ChannelHandlerContext ctx, HttpRequest request) {

        var executor = (OrderedEventExecutor) ctx.executor();
        var dataCtx = new DataContext(executor, null);

        contentServer.headRequest(request.uri(), dataCtx)
                .thenAccept(response -> serverHeadResponse(ctx, request, response))
                .exceptionally(err -> unexpectedError(ctx, err));
    }

    private void serverHeadResponse(ChannelHandlerContext ctx, HttpRequest request, ContentResponse serverResponse) {

        var httpResponse = new DefaultFullHttpResponse(
                request.protocolVersion(),
                serverResponse.statusCode);

        httpResponse.headers().setAll(serverResponse.headers);

        ctx.write(httpResponse);
        ctx.flush();
    }

    private void serveGetRequest(ChannelHandlerContext ctx, HttpRequest request) {

        var executor = (OrderedEventExecutor) ctx.executor();
        var dataCtx = new DataContext(executor, null);

        contentServer.getRequest(request.uri(), dataCtx)
                .thenAccept(response -> serveGetResponse(ctx, request, response))
                .exceptionally(err -> unexpectedError(ctx, err));
    }

    private void serveGetResponse(ChannelHandlerContext ctx, HttpRequest request, ContentResponse serverResponse) {

        var httpResponse = new DefaultHttpResponse(
                request.protocolVersion(),
                serverResponse.statusCode);

        httpResponse.headers().setAll(serverResponse.headers);

        ctx.write(httpResponse);

        if (serverResponse.statusCode == HttpResponseStatus.OK) {

            serverResponse.reader.subscribe(new ResponseSender(ctx));
        }
        else {

            ctx.write(new DefaultLastHttpContent());
            ctx.flush();
        }
    }

    private Void unexpectedError(ChannelHandlerContext ctx, Throwable error) {

        ctx.fireExceptionCaught(error);
        return null;
    }

    private static class ResponseSender implements Flow.Subscriber<ByteBuf> {

        private static final long REQUEST_BUFFER = 32;

        private final ChannelHandlerContext ctx;

        private Flow.Subscription subscription;
        private long nPending;

        ResponseSender(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {

            this.subscription = subscription;

            nPending += REQUEST_BUFFER;
            subscription.request(REQUEST_BUFFER);
        }

        @Override
        public void onNext(ByteBuf chunk) {

            ctx.write(new DefaultHttpContent(chunk));
            nPending -= 1;

            if (nPending < REQUEST_BUFFER / 2) {
                var nRequest = REQUEST_BUFFER - nPending;
                nPending += nRequest;
                subscription.request(nRequest);
            }
        }

        @Override
        public void onError(Throwable throwable) {

            ctx.close();
        }

        @Override
        public void onComplete() {

            ctx.write(new DefaultLastHttpContent());
            ctx.flush();
        }
    }
}
