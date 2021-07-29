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

package com.accenture.trac.gateway;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.net.InetAddress;


public class Http1Client {

    private final HttpScheme scheme;
    private final InetAddress host;
    private final short port;

    private final SslContext sslCtx;
    private final EventLoopGroup clientGroup;
    private final Bootstrap clientBootstrap;

    public Http1Client(HttpScheme scheme, InetAddress host, short port) {

        this.scheme = scheme;
        this.host = host;
        this.port = port;

        // No SSL yet
        this.sslCtx = null;

        this.clientGroup = new NioEventLoopGroup(1);
        this.clientBootstrap = new Bootstrap()
                .group(clientGroup)
                .channel(NioSocketChannel.class);
    }

    public Future<? extends HttpResponse> headRequest(String path) {

        try {

            // Make the connection attempt.
            var clientInit = new ClientInitializer();
            var channel = this.clientBootstrap
                    .handler(clientInit)
                    .connect(host, port)
                    .sync()
                    .channel();

            var responseFuture = clientInit.getResponseFuture();

            // Prepare the HTTP request.
            var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, path, Unpooled.EMPTY_BUFFER);
            request.headers().set(HttpHeaderNames.HOST, host);
            request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);

            channel.writeAndFlush(request);

            return responseFuture;
        }
        catch (InterruptedException e) {

            return clientGroup.next().newFailedFuture(e);
        }
    }

    public Future<? extends FullHttpResponse> getRequest(String path) {

        try {

            // Make the connection attempt.
            var clientInit = new ClientInitializer();
            var channel = this.clientBootstrap
                    .handler(clientInit)
                    .connect(host, port)
                    .sync()
                    .channel();

            var responseFuture = clientInit.getResponseFuture();

            // Prepare the HTTP request.
            var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path, Unpooled.EMPTY_BUFFER);
            request.headers().set(HttpHeaderNames.HOST, host);
            request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);

            channel.writeAndFlush(request);

            return responseFuture;
        }
        catch (InterruptedException e) {

            return clientGroup.next().newFailedFuture(e);
        }
    }

    private class ClientInitializer extends ChannelInitializer<SocketChannel> {

        private Promise<FullHttpResponse> response;

        @Override
        public void initChannel(SocketChannel ch) {

            var p = ch.pipeline();

            // Enable HTTPS if necessary.
            if (sslCtx != null)
                p.addLast(sslCtx.newHandler(ch.alloc()));

            p.addLast(new HttpClientCodec());
            p.addLast(new HttpContentDecompressor());  // Use automatic content decompression
            p.addLast(new HttpObjectAggregator(1048576));  // Automatic response aggregation up to a fixed limit

            response = ch.eventLoop().newPromise();
            p.addLast(new ClientHandler(response));
        }

        public Promise<FullHttpResponse> getResponseFuture() {
            return response;
        }
    }

    private static class ClientHandler extends SimpleChannelInboundHandler<HttpObject> {

        private final Promise<FullHttpResponse> response;

        public ClientHandler(Promise<FullHttpResponse> response) {
            this.response = response;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {

            if (msg instanceof FullHttpResponse) {

                var response = (FullHttpResponse) msg;

                if (response.content() != null && response.content().readableBytes() > 0)
                    response.retain();

                this.response.setSuccess(response);

                ctx.close();
            }

            else
                throw new RuntimeException("Unexpected object in client handler");
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

            ctx.close();
            response.setFailure(cause);
        }
    }
}
