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

package com.accenture.trac.gateway.routing;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.gateway.config.RouteConfig;
import com.accenture.trac.gateway.config.RouteType;
import com.accenture.trac.gateway.exec.Route;
import com.accenture.trac.gateway.proxy.http.Http1ProxyBuilder;
import com.accenture.trac.gateway.proxy.grpc.GrpcProxyBuilder;

import com.accenture.trac.gateway.proxy.rest.RestApiProxyBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Http1Router extends ChannelDuplexHandler {

    private static final int SOURCE_IS_HTTP_1 = 1;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final AtomicInteger nextRouterId = new AtomicInteger();
    private final int routerId = nextRouterId.getAndIncrement();

    private final List<Route> routes;
    private final Map<Integer, TargetChannelState> targets;
    private final Map<Long, RequestState> requests;

    private long currentInboundRequest;
    private long currentOutboundRequest;

    private Bootstrap bootstrap;

    public Http1Router(List<Route> routes) {

        this.routes = routes;
        this.targets = new HashMap<>();
        this.requests = new HashMap<>();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // HANDLER LIFECYCLE for this router
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

        log.info("HTTP 1.1 router added for ID {}", routerId);

        // Bootstrap is used to create proxy channels for this router channel
        // In an HTTP 1 world, browser clients will normally make several connections,
        // so there will be multiple router instances per client

        // Proxy channels will run on the same event loop as the router channel they belong to
        // Proxy channels will use the same ByteBuf allocator as the router they belong to

        var eventLoop = ctx.channel().eventLoop();
        var allocator = ctx.alloc();

        this.bootstrap = new Bootstrap()
                .group(eventLoop)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, allocator);

        super.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {

        log.info("HTTP 1.1 router removed for ID {}", routerId);

        super.handlerRemoved(ctx);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {

        log.info("HTTP 1.1 channel registered");

        super.channelRegistered(ctx);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // MESSAGES AND EVENTS
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (! (msg instanceof HttpObject))
            throw new EUnexpected();

        if (msg instanceof HttpRequest) {
            processNewRequest(ctx, (HttpRequest) msg);
        }
        else if (msg instanceof HttpContent) {
            processRequestContent(ctx, (HttpContent) msg);
        }
        else {
            throw new EUnexpected();
        }

        if (msg instanceof LastHttpContent) {
            processEndOfRequest(ctx, (LastHttpContent) msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if (! (msg instanceof HttpObject))
            throw new EUnexpected();

        ctx.write(msg, promise);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

       super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        super.exceptionCaught(ctx, cause);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // REQUEST PROCESSING
    // -----------------------------------------------------------------------------------------------------------------

    private void processNewRequest(ChannelHandlerContext ctx, HttpRequest msg) throws Exception {

        // Increment counter for inbound requests
        currentInboundRequest++;

        // Set up a new request state with initial values
        var request = new RequestState();
        request.requestId = currentInboundRequest;
        request.status = RequestStatus.RECEIVING;
        requests.put(request.requestId, request);

        // Look for a matching route for this request
        var uri = URI.create(msg.uri());
        var method = msg.method();
        var headers = msg.headers();

        Route selectedRoute = null;

        for (var route : this.routes) {
            if (route.getMatcher().matches(uri, method, headers)) {

                log.info("{} {} -> {} ({})", method, uri, route.getConfig().getRouteName(), routerId);
                selectedRoute = route;
                break;
            }
        }

        // If there is no matching route, fail the request and respond with 404
        if (selectedRoute == null) {

            // No route available, send a 404 back to the client
            log.warn("No route available: " + method + " " + uri);

            var protocolVersion = msg.protocolVersion();
            var response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.NOT_FOUND);
            ctx.writeAndFlush(response);

            request.status = RequestStatus.FAILED;

            // No need to release, base class does it automatically
            // ReferenceCountUtil.release(msg);

            return;
        }

        request.routeIndex = selectedRoute.getIndex();

        var existingChannel = targets.getOrDefault(request.routeIndex, null);
        var target = existingChannel != null ? existingChannel : new TargetChannelState();

        if (existingChannel == null)
            targets.put(request.routeIndex, target);

        if (target.channel == null)
            openProxyChannel(target, selectedRoute, ctx);

        if (target.channel.isActive())
            target.channel.write(msg);
        else
            target.outboundQueue.add(msg);
    }

    private void processRequestContent(ChannelHandlerContext ctx, HttpContent msg) {

        var request = requests.getOrDefault(currentInboundRequest, null);

        if (request == null)
            throw new EUnexpected();

        var target = targets.getOrDefault(request.routeIndex, null);

        if (target == null)
            throw new EUnexpected();

        msg.retain();

        if (target.channel.isActive())
            target.channel.write(msg);
        else
            target.outboundQueue.add(msg);
    }

    private void processEndOfRequest(ChannelHandlerContext ctx, LastHttpContent msg) {

        var request = requests.getOrDefault(currentInboundRequest, null);

        if (request == null)
            throw new EUnexpected();

        var target = targets.getOrDefault(request.routeIndex, null);

        if (target == null)
            throw new EUnexpected();

        if (target.channel.isActive())
            target.channel.flush();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // PROXY CHANNEL HANDLING
    // -----------------------------------------------------------------------------------------------------------------

    private void openProxyChannel(TargetChannelState target, Route route, ChannelHandlerContext ctx) {

        var channelActiveFuture = ctx.newPromise();
        var channelInactiveFuture = ctx.newPromise();

        target.channelActiveFuture = channelActiveFuture;
        target.channelInactiveFuture = channelInactiveFuture;

        var channelInit = proxyInitializerForRoute(
                route.getConfig(),
                ctx, channelActiveFuture);

        if (route.getConfig().getRouteType() == RouteType.REST) {

            target.channel = new EmbeddedChannel(channelInit);
            target.channelOpenFuture = target.channel.newSucceededFuture();
            target.channelCloseFuture = target.channel.closeFuture();
        }

        else {

            var targetConfig = route.getConfig().getTarget();

            target.channelOpenFuture = bootstrap
                    .handler(channelInit)
                    .connect(targetConfig.getHost(), targetConfig.getPort());

            target.channel = target.channelOpenFuture.channel();
            target.channelCloseFuture = target.channel.closeFuture();
        }

        target.channelOpenFuture.addListener(future -> proxyChannelOpen(ctx, target, future));
        target.channelCloseFuture.addListener(future -> proxyChannelClosed(ctx, target, future));
        target.channelActiveFuture.addListener(future -> proxyChannelActive(ctx, target, future));
        target.channelInactiveFuture.addListener(future -> proxyChannelInactive(ctx, target, future));
    }

    private ChannelInitializer<Channel> proxyInitializerForRoute(
            RouteConfig routeConfig,
            ChannelHandlerContext routerCtx,
            ChannelPromise routeActivePromise) {

        switch (routeConfig.getRouteType()) {

            case HTTP:
                return new Http1ProxyBuilder(routeConfig, routerCtx, routeActivePromise);

            case GRPC:
                return new GrpcProxyBuilder(routeConfig, SOURCE_IS_HTTP_1, routerCtx, routeActivePromise);

            case REST:
                return new RestApiProxyBuilder(
                        routeConfig, SOURCE_IS_HTTP_1,
                        routerCtx, routeActivePromise,
                        routerCtx.executor());

            default:
                throw new EUnexpected();
        }
    }

    private void proxyChannelOpen(ChannelHandlerContext ctx, TargetChannelState target, Future<?> future) {

        if (!future.isSuccess()) {

            var response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.
                    SERVICE_UNAVAILABLE);

            ctx.writeAndFlush(response);
        }
    }

    private void proxyChannelActive(ChannelHandlerContext ctx, TargetChannelState target, Future<?> future) {

        if (future.isSuccess()) {
            log.info("Connection to server ok");

            var outboundHead = target.outboundQueue.poll();

            while (outboundHead != null) {
                target.channel.write(outboundHead);
                outboundHead = target.outboundQueue.poll();
            }

            target.channel.flush();
        }
        else
            log.error("Connection to server failed", future.cause());
    }

    private void proxyChannelInactive(ChannelHandlerContext ctx, TargetChannelState target, Future<?> future) {

    }

    private void proxyChannelClosed(ChannelHandlerContext ctx, TargetChannelState target, Future<?> future) {

    }




    private static class TargetChannelState {

        Channel channel;
        ChannelFuture channelOpenFuture;
        ChannelFuture channelCloseFuture;
        ChannelFuture channelActiveFuture;
        ChannelFuture channelInactiveFuture;

        Queue<Object> outboundQueue = new LinkedList<>();
    }

    private static class RequestState {

        long requestId;
        int routeIndex;

        RequestStatus status;
    }

    private enum RequestStatus {
        RECEIVING,
        RECEIVING_BIDI,
        WAITING_FOR_RESPONSE,
        RESPONDING,
        SUCCEEDED,
        FAILED
    }
}
