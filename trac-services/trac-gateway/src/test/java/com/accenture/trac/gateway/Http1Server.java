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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class Http1Server {

    // Very quick test server, serves static content to test proxying at HTTP level
    // Based on the Netty example here:
    // https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/http/file

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int port;
    private final Path contentRoot;

    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    private ChannelFuture serverChannel;

    public Http1Server(int port, Path contentRoot) {
        this.port = port;
        this.contentRoot = contentRoot;
    }

    public void run() throws Exception {

        bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("http1_boss"));
        workerGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("http1_worker"));

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new Http1ServerInitializer());

        this.serverChannel = bootstrap.bind(port);

        // Wait for the server to come up
        serverChannel.sync();

        log.info("Test server is up on port {}", port);
    }

    public void shutdown() {

        try {

            if (serverChannel == null)
                return;

            var closePromise = serverChannel.channel().newPromise();
            serverChannel.channel().close(closePromise);

            closePromise.addListener(ch -> {
                workerGroup.shutdownGracefully(1, 1, TimeUnit.SECONDS);
                bossGroup.shutdownGracefully(1, 1, TimeUnit.SECONDS);
            });

            closePromise.sync();
            log.info("Test server has gone done");
        }
        catch (InterruptedException e) {
            log.warn("Interrupt during test server shutdown");
            Thread.currentThread().interrupt();
        }
    }

    private class Http1ServerInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) {

            log.info("New connection from {}", ch.remoteAddress());

            ChannelPipeline p = ch.pipeline();
            p.addLast(new HttpRequestDecoder());
            p.addLast(new HttpResponseEncoder());
            p.addLast(new Http1Handler());
        }
    }

    private class Http1Handler extends SimpleChannelInboundHandler<HttpObject> {

        private HttpRequest request;
        //private HttpContent content;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {

            if (msg instanceof HttpRequest) {
                request = (HttpRequest) msg;
                log.info("New HTTP request: {} {}", request.method(), request.uri());
            }

//            if (msg instanceof HttpContent)
//                content = (HttpContent) msg;

            if (request == null)
                throw new RuntimeException("Bad request");

            if (request.method() == HttpMethod.GET || request.method() == HttpMethod.HEAD) {
                var uri = URI.create(request.uri());
                serveStaticContent(ctx, uri, request.method());
            }
            else
                sendError(ctx, BAD_REQUEST);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }

        private void serveStaticContent(ChannelHandlerContext ctx, URI uri, HttpMethod method) throws IOException {

            var uriPath = uri.getPath().startsWith("/")
                    ? uri.getPath().substring(1)
                    : uri.getPath();

            var fullPath = contentRoot.resolve(uriPath);

            if (!Files.exists(fullPath)) {
                sendError(ctx, NOT_FOUND);
                return;
            }

            if (!Files.isRegularFile(fullPath) || !Files.isReadable(fullPath)) {
                sendError(ctx, FORBIDDEN);
                return;
            }

            var keepAlive = HttpUtil.isKeepAlive(request);

            // This is not a reliable way of determining the MIME type, but good enough for our test server!
            var fileType = "text/plain"; // Files.probeContentType(fullPath);
            var fileLength = Files.size(fullPath);

            var response = new DefaultHttpResponse(HTTP_1_1, OK);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, fileType);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, fileLength);
            setDateAndCacheHeaders(response, fullPath);

            if (!keepAlive)
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            else if (request.protocolVersion().equals(HTTP_1_0))
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

            ctx.write(response);

            // For GET requests, set up transfer of the file content
            if (method == HttpMethod.GET) {

                var fileChannel = FileChannel.open(fullPath, StandardOpenOption.READ);
                var fileRegion = new DefaultFileRegion(fileChannel, 0, fileLength);

                // No SSL atm in the test server, so no need to chunk up the file

                var sendFuture = ctx.write(fileRegion, ctx.newProgressivePromise());
                var lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

                // Option to monitor on sendFuture progress

                if (!keepAlive) {
                    // Close the connection when the whole content is written out.
                    lastContentFuture.addListener(ChannelFutureListener.CLOSE);
                }
            }

            // For HEAD requests, do not send any content
            else {

                ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

                if (!keepAlive)
                    ctx.close();
            }
        }

        private void setDateAndCacheHeaders(HttpResponse response, Path fileToCache) throws IOException {

            var HTTP_CACHE_SECONDS = 3600;

            var clock = Clock.systemUTC();
            var currentTime = clock.instant();
            var expireTime = currentTime.plus(HTTP_CACHE_SECONDS, ChronoUnit.SECONDS);
            var lastModifiedTime = Files.getLastModifiedTime(fileToCache).toInstant();

            // HTTP date format
            var formatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                    .withZone(ZoneOffset.UTC)
                    .withLocale(Locale.ENGLISH);

            response.headers().set(HttpHeaderNames.DATE, formatter.format(currentTime));
            response.headers().set(HttpHeaderNames.EXPIRES, formatter.format(expireTime));
            response.headers().set(HttpHeaderNames.CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
            response.headers().set(HttpHeaderNames.LAST_MODIFIED, formatter.format(lastModifiedTime));
        }

        private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {

            var response = new DefaultFullHttpResponse(
                    HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

            sendAndCleanupConnection(ctx, response);
        }

        private void sendAndCleanupConnection(ChannelHandlerContext ctx, FullHttpResponse response) {

            HttpUtil.setContentLength(response, response.content().readableBytes());

            var keepAlive = HttpUtil.isKeepAlive(request);

            if (!keepAlive) {
                // We're going to close the connection as soon as the response is sent,
                // so we should also make it clear for the client.
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            }
            else if (request.protocolVersion().equals(HTTP_1_0)) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            ChannelFuture flushPromise = ctx.writeAndFlush(response);

            if (!keepAlive) {
                // Close the connection as soon as the response is sent.
                flushPromise.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
