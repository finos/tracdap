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

package org.finos.tracdap.gateway.routing;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.GwProtocol;
import org.finos.tracdap.gateway.exec.Route;
import org.finos.tracdap.gateway.proxy.http.Http1ProxyBuilder;
import org.finos.tracdap.gateway.proxy.grpc.GrpcProxyBuilder;

import org.finos.tracdap.gateway.proxy.rest.RestApiProxyBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;


public class Http1Router extends ChannelDuplexHandler {

    private static final int SOURCE_IS_HTTP_1 = 1;

    private static final List<RequestStatus> REQUEST_STATUS_FINISHED = List.of(
            RequestStatus.COMPLETED,
            RequestStatus.FAILED);

    private static final List<RequestStatus> REQUEST_STATUS_CAN_RECEIVE = List.of(
            RequestStatus.RECEIVING,
            RequestStatus.RECEIVING_BIDI);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int connId;
    private final List<Route> routes;

    private final Map<Integer, TargetChannelState> targets;
    private final Map<Long, RequestState> requests;
    private final Map<Object, Integer> routeAssociation;

    private long currentInboundRequest;
    private long currentOutboundRequest;

    private Bootstrap bootstrap;

    public Http1Router(List<Route> routes, int connId) {

        this.routes = routes;
        this.connId = connId;

        this.targets = new HashMap<>();
        this.requests = new HashMap<>();
        this.routeAssociation = new HashMap<>();

        this.currentInboundRequest = -1;
        this.currentOutboundRequest = -1;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // HANDLER LIFECYCLE for this router
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

        log.info("conn = {}, HTTP 1.1 router added", connId);

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

        log.info("conn = {}, HTTP 1.1 router removed", connId);

        super.handlerRemoved(ctx);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {

        log.info("conn = {}, HTTP 1.1 channel registered", connId);

        super.channelRegistered(ctx);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // MESSAGES AND EVENTS
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

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

        try {
            if (!(msg instanceof HttpObject))
                throw new EUnexpected();

            ctx.write(msg, promise);
        }
        finally {

            routeAssociation.remove(msg);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt instanceof IdleStateEvent) {
            log.info("conn = {}, Idle time expired", connId);
            ctx.close();
        }
        else
            super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {

        log.error("conn = {}, Unhandled error in HTTP/1 routing handler", connId, error);

        // Only send an error response if there is an active request that has not been responded to yet
        // If there is no active request, or a response has already been sent,
        // then an error response would not be recognised by the client

        var currentRequest = requests.get(currentOutboundRequest);
        var responseNotSent = Set.of(RequestStatus.RECEIVING, RequestStatus.WAITING_FOR_RESPONSE);

        if (currentRequest != null && responseNotSent.contains(currentRequest.status)) {

            log.error("conn = {}, Sending 500 error", connId);

            var errorResponse = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);

            ctx.writeAndFlush(errorResponse);
        }

        // If an error reaches this handler, then the channel is in an inconsistent
        // Regardless of whether an error message could be sent or not, we're going to close the connection

        // todo: full clean up

        log.error("conn = {}, This client connection will now be closed", connId);

        ctx.close();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // REQUEST PROCESSING
    // -----------------------------------------------------------------------------------------------------------------


    // TODO: Check Releasing!!!
    // TODO: Some basic error handling


    private void processNewRequest(ChannelHandlerContext ctx, HttpRequest msg) {

        // Set up a new request state and record it in the requests map

        var request = new RequestState();
        request.requestId = ++currentInboundRequest;
        request.status = RequestStatus.RECEIVING;
        requests.put(request.requestId, request);


        // Look up the route for this request
        // If there is no matching route then fail the request immediately with 404
        // This is a normal error, i.e. the client channel can remain open for more requests

        var route = lookupRoute(msg, request.requestId);

        if (route == null) {

            var protocolVersion = msg.protocolVersion();
            var response = new DefaultFullHttpResponse(protocolVersion, HttpResponseStatus.NOT_FOUND);
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
            ctx.writeAndFlush(response);

            request.status = RequestStatus.FAILED;
            ++currentOutboundRequest;

            return;
        }

        request.routeIndex = route.getIndex();


        // Look up the proxy target for the selected route
        // If there is no state for the required target, create a new channel and target state record

        var existingChannel = targets.getOrDefault(request.routeIndex, null);
        var target = existingChannel != null ? existingChannel : new TargetChannelState();

        if (existingChannel == null)
            targets.put(request.routeIndex, target);

        if (target.channel == null)
            openProxyChannel(target, route, ctx);

        if (target.channel.isActive())
            target.channel.write(msg);
        else
            target.outboundQueue.add(msg);
    }

    private void processRequestContent(ChannelHandlerContext ctx, HttpContent msg) {

        try {
            var request = requests.getOrDefault(currentInboundRequest, null);

            if (request == null)
                throw new EUnexpected();

            // Inbound content may be received for requests that have already finished
            // E.g. After a 404 error or an early response from a source server
            // In this case the content can be silently discarded without raising a (new) error

            if (REQUEST_STATUS_FINISHED.contains(request.status))
                return;

            if (!REQUEST_STATUS_CAN_RECEIVE.contains(request.status))
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
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void processEndOfRequest(ChannelHandlerContext ctx, LastHttpContent msg) {

        var request = requests.getOrDefault(currentInboundRequest, null);

        if (request == null)
            throw new EUnexpected();

        switch (request.status) {

            case RECEIVING:
                request.status = RequestStatus.WAITING_FOR_RESPONSE;
                break;

            case RECEIVING_BIDI:
                request.status = RequestStatus.RESPONDING;
                break;

            case COMPLETED:
            case FAILED:
                requests.remove(currentInboundRequest);
                return;

            default:
                throw new EUnexpected();
        }

        var target = targets.getOrDefault(request.routeIndex, null);

        if (target == null)
            throw new EUnexpected();

        if (target.channel.isActive())
            target.channel.flush();
    }

    private Route lookupRoute(HttpRequest request, long requestId) {

        var uri = URI.create(request.uri());
        var method = request.method();
        var headers = request.headers();

        for (var route : this.routes) {
            if (route.getMatcher().matches(uri, method, headers)) {

                log.info("conn = {}, req = {}, ROUTE MATCHED {} {} -> {} ({})",
                        connId, requestId,
                        method, uri,
                        route.getConfig().getRouteName(),
                        route.getConfig().getRouteType());

                return route;
            }
        }

        // No route available, send a 404 back to the client
        log.warn("conn = {}, req = {}, ROUTE NOT MATCHED {} {} ",
                connId, requestId,  method, uri);

        return null;
    }

    void associateRoute(Object msg, int routeIndex) {
        this.routeAssociation.put(msg, routeIndex);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // PROXY CHANNEL HANDLING
    // -----------------------------------------------------------------------------------------------------------------

    private void openProxyChannel(TargetChannelState target, Route route, ChannelHandlerContext ctx) {

        var channelActiveFuture = ctx.newPromise();

        target.routeIndex = route.getIndex();
        target.channelActiveFuture = channelActiveFuture;

        var channelInit = proxyInitializerForRoute(
                route, route.getIndex(),
                ctx, channelActiveFuture);

        if (route.getConfig().getRouteType() == GwProtocol.REST) {

            target.channel = new EmbeddedChannel(channelInit);
            target.channelOpenFuture = target.channel.newSucceededFuture();
        }

        else {

            var targetConfig = route.getConfig().getTarget();

            target.channelOpenFuture = bootstrap
                    .handler(channelInit)
                    .connect(targetConfig.getHost(), targetConfig.getPort());

            target.channel = target.channelOpenFuture.channel();
        }

        target.channelCloseFuture = target.channel.closeFuture();

        target.channelOpenFuture.addListener(future -> proxyChannelOpen(ctx, target, future));
        target.channelCloseFuture.addListener(future -> proxyChannelClosed(ctx, target, future));
        target.channelActiveFuture.addListener(future -> proxyChannelActive(ctx, target, future));
    }

    private ChannelInitializer<Channel> proxyInitializerForRoute(
            Route routeConfig, int routeIndex,
            ChannelHandlerContext routerCtx,
            ChannelPromise routeActivePromise) {

        var routerLink = new Http1RouterLink(routerCtx, routeActivePromise, this, routeIndex);

        switch (routeConfig.getConfig().getRouteType()) {

            case HTTP:
                return new Http1ProxyBuilder(routeConfig.getConfig(), routerLink);

            case GRPC:
                return new GrpcProxyBuilder(routeConfig.getConfig(), SOURCE_IS_HTTP_1, routerLink);

            case REST:
                return new RestApiProxyBuilder(routeConfig, SOURCE_IS_HTTP_1, routerLink, routerCtx.executor());

            default:
                throw new EUnexpected();
        }
    }

    private void proxyChannelOpen(ChannelHandlerContext ctx, TargetChannelState target, Future<?> future) {

        // TODO: Check reason for connect failure
        // TODO: Handle pipelining - there could be multiple queued requests, and/or requests to other targets

        var targetConfig = routes.get(target.routeIndex).getConfig().getTarget();

        if (future.isSuccess()) {

            log.info("conn = {}, PROXY CONNECT -> {} {}", connId, targetConfig.getHost(), targetConfig.getPort());
        }
        else {

            log.error("conn = {}, PROXY CONNECT FAILED -> {} {}", connId, targetConfig.getHost(), targetConfig.getPort(), future.cause());

            var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
            ctx.writeAndFlush(response);

            while (!target.outboundQueue.isEmpty()) {
                var queuedMsg = target.outboundQueue.poll();
                ReferenceCountUtil.release(queuedMsg);
            }

            targets.remove(target.routeIndex);
        }
    }

    private void proxyChannelClosed(ChannelHandlerContext ctx, TargetChannelState target, Future<?> future) {

        // TODO: Check reason for connect failure
        // TODO: Handle pipelining
        // TODO: Is it possible to retain messages and reconnect in any cases?

        var targetConfig = routes.get(target.routeIndex).getConfig().getTarget();
        var lostMsg = !target.outboundQueue.isEmpty();

        while (!target.outboundQueue.isEmpty()) {
            var queuedMsg = target.outboundQueue.poll();
            ReferenceCountUtil.release(queuedMsg);
        }

        if (future.isSuccess())
            log.info("conn = {}, PROXY DISCONNECT -> {} {}", connId, targetConfig.getHost(), targetConfig.getPort());
        else
            log.error("conn = {}, PROXY DISCONNECT FAILED -> {} {}", connId, targetConfig.getHost(), targetConfig.getPort(), future.cause());

        if (lostMsg)
            log.error("conn = {}, Pending messages have been lost", connId);

        targets.remove(target.routeIndex);

        // Errors here are unexpected, this means there could be inconsistent state
        // Take the nuclear option and kill the client connection
        if (!future.isSuccess() || lostMsg) {
            ctx.close();
        }
    }

    private void proxyChannelActive(ChannelHandlerContext ctx, TargetChannelState target, Future<?> future) {

        // Errors here are unexpected, this means there could be inconsistent state
        // Take the nuclear option and kill the client connection
        if (!future.isSuccess()) {

            log.error("conn = {}, Unexpected error in proxy channel activation", connId, future.cause());

            while (!target.outboundQueue.isEmpty()) {
                var queuedMsg = target.outboundQueue.poll();
                ReferenceCountUtil.release(queuedMsg);
            }

            ctx.close();
        }

        var outboundHead = target.outboundQueue.poll();

        while (outboundHead != null) {
            target.channel.write(outboundHead);
            outboundHead = target.outboundQueue.poll();
        }

        target.channel.flush();

    }

    private static class TargetChannelState {

        int routeIndex;

        Channel channel;
        ChannelFuture channelOpenFuture;
        ChannelFuture channelCloseFuture;
        ChannelFuture channelActiveFuture;

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
        COMPLETED,
        FAILED
    }
}
