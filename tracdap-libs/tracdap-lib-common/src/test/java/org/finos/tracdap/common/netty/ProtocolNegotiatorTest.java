/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.codec.http2.*;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;


public class ProtocolNegotiatorTest {

    private static final int IDLE_TIMEOUT = 60;

    static class SimpleNegotiator extends BaseProtocolNegotiator {

        private final Supplier<ChannelHandler> http1;
        private final Supplier<ChannelHandler> http2;
        private final Supplier<ChannelHandler> ws;

        public SimpleNegotiator(
                Supplier<ChannelHandler> http1,
                Supplier<ChannelHandler> http2,
                Supplier<ChannelHandler> ws) {

            super(http2 != null, http2 != null, ws != null, IDLE_TIMEOUT);

            this.http1 = http1;
            this.http2 = http2;
            this.ws = ws;
        }

        @Override
        protected ChannelInboundHandler http1AuthHandler() {
            return new ChannelInboundHandlerAdapter();
        }

        @Override
        protected ChannelInboundHandler http2AuthHandler() {
            return new ChannelInboundHandlerAdapter();
        }

        @Override
        protected ChannelHandler http1PrimaryHandler() {
            return http1.get();
        }

        @Override
        protected ChannelHandler http2PrimaryHandler() {
            return http2.get();
        }

        @Override
        protected WebSocketServerProtocolConfig wsProtocolConfig(HttpRequest upgradeRequest) {
            return WebSocketServerProtocolConfig.newBuilder().build();
        }

        @Override
        protected ChannelHandler wsPrimaryHandler() {
            return ws.get();
        }
    }




    private static final Supplier<ChannelHandler> UNUSED_PROTOCOL = ChannelDuplexHandler::new;

    private static final Logger log = LoggerFactory.getLogger(ProtocolNegotiatorTest.class);

    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;

    private static final AtomicInteger serverPort = new AtomicInteger(60000);

    void startServer(
            int gatewayPort,
            Supplier<ChannelHandler> http1,
            Supplier<ChannelHandler>http2,
            Supplier<ChannelHandler> webSockets)
            throws Exception {

        // The protocol negotiator is the top level initializer for new inbound connections
        var protocolNegotiator = new SimpleNegotiator(http1, http2, webSockets);

        bossGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("boss"));
        workerGroup = new NioEventLoopGroup(6, new DefaultThreadFactory("worker"));

        var bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(protocolNegotiator);

        // Bind and start to accept incoming connections.
        var startupFuture = bootstrap.bind(gatewayPort);

        // Block until the server channel is ready - it's just easier this way!
        // The sync call will rethrow any errors, so they can be handled before leaving the start() method

        startupFuture.await();
    }

    @AfterEach
    void stopServer() {

        log.info("Doing server shutdown");

        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 3, TimeUnit.SECONDS);
            workerGroup = null;
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 3, TimeUnit.SECONDS);
            bossGroup = null;
        }
    }

    @Test
    void negotiateClearHttp1() throws Exception {

        var port = serverPort.getAndIncrement();

        startServer(port, () -> new Http1Server("test_response"), UNUSED_PROTOCOL, UNUSED_PROTOCOL);

        var request = java.net.http.HttpRequest.newBuilder().GET()
                .uri(new URI("http://localhost:" + port + "/test/path"))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(3))
                .build();

        var client = HttpClient.newHttpClient();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
        Assertions.assertEquals("test_response", response.body());
    }

    @Test
    void negotiateClearHttp2() throws Exception {

        var port = serverPort.getAndIncrement();

        startServer(port, UNUSED_PROTOCOL, () -> new Http2Server("test_response"), UNUSED_PROTOCOL);

        var request = java.net.http.HttpRequest.newBuilder().GET()
                .uri(new URI("http://localhost:" + port + "/test/path"))
                .version(HttpClient.Version.HTTP_2)
                .timeout(Duration.ofSeconds(3))
                .build();

        var client = HttpClient.newHttpClient();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(HttpClient.Version.HTTP_2, response.version());
        Assertions.assertEquals("test_response", response.body());
    }

    @Test
    void negotiateClearWs() throws Exception {

        var port = serverPort.getAndIncrement();

        startServer(port, UNUSED_PROTOCOL, UNUSED_PROTOCOL, () -> new WebSocketServer("test_response_ws"));

        var latch = new CountDownLatch(1);
        var response = new String[1];

        var client = HttpClient
                .newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + port + "/"), new WebSocket.Listener() {
                    @Override
                    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                        response[0] = data.toString();
                        latch.countDown();
                        return CompletableFuture.completedFuture(0);
                    }
                }).join();

        client.sendText("test_message", true).join();

        var done = latch.await(5, TimeUnit.SECONDS);

        Assertions.assertTrue(done);
        Assertions.assertEquals("test_response_ws", response[0]);
    }

    private static class Http1Server extends ChannelInboundHandlerAdapter {

        private final String response;

        Http1Server(String response) {
            this.response = response;
        }

        @Override
        public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

            if (msg instanceof HttpRequest) {

                log.info("HTTP/1 server is responding");

                var content = Unpooled.wrappedBuffer(response.getBytes(StandardCharsets.UTF_8));
                var headers = new DefaultHttpHeaders();
                headers.set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

                var response = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                        content, headers, new DefaultHttpHeaders());

                ctx.writeAndFlush(response);
                ctx.close();
            }
            else {
                super.channelRead(ctx, msg);
            }
        }
    }

    private static class Http2Server extends ChannelInboundHandlerAdapter {

        private final String response;

        Http2Server(String response) {
            this.response = response;
        }

        @Override
        public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

            if (msg instanceof Http2HeadersFrame) {

                log.info("HTTP/2 server is responding");

                var req = (Http2HeadersFrame) msg;

                var content = Unpooled.copiedBuffer(response, StandardCharsets.UTF_8);

                var headers = new DefaultHttp2Headers();
                headers.status(HttpResponseStatus.OK.codeAsText());
                headers.set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                headers.setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

                ctx.write(new DefaultHttp2HeadersFrame(headers).stream(req.stream()));
                ctx.write(new DefaultHttp2DataFrame(content, true).stream(req.stream()));
                ctx.flush();
            }
            else {
                super.channelRead(ctx, msg);
            }
        }
    }

    private static class WebSocketServer extends ChannelDuplexHandler {

        private final String response;

        WebSocketServer(String response) {
            this.response = response;
        }

        @Override
        public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

            if (msg instanceof TextWebSocketFrame) {

                log.info("WebSocket server is responding");

                var req = (TextWebSocketFrame) msg;
                var resp = new TextWebSocketFrame(true, req.rsv(), response);

                ctx.write(resp);
                ctx.flush();
            }
            else {
                super.channelRead(ctx, msg);
            }
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

            if (msg instanceof HttpRequest) {

                var req = (HttpRequest) msg;
                System.out.println(req.headers().toString());
            }

            super.write(ctx, msg, promise);
        }
    }
}