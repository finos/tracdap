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

import org.finos.tracdap.gateway.exec.Route;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;


abstract class CoreRouter extends ChannelDuplexHandler {

    // TODO: This could definitely use a work-through to get it more simple / solid

    // This base functionality has been taken from Http1Router to share with WebSocketsRouter.
    // The code has not been causing problems, but there is a lot of state here.
    // Getting it to the same level as e.g. the Grpc translators would probably be worthwhile
    // As well as stability, the router is the heart of the gateway and is likely to change semi-often
    // Clarity and convention make for  happy coders...


    private final Logger log = LoggerFactory.getLogger(getClass());

    protected final List<Route> routes;
    protected final int connId;
    protected final String protocol;

    private final Map<Integer, TargetChannelState> targets;
    private final Map<Object, Integer> routeAssociation;

    protected Bootstrap bootstrap;

    public CoreRouter(List<Route> routes, int connId, String protocol) {

        this.routes = routes;
        this.connId = connId;
        this.protocol = protocol;

        this.targets = new HashMap<>();
        this.routeAssociation = new HashMap<>();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

        log.info("conn = {}, router added for protocol [{}]", connId, protocol);

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

        log.info("conn = {}, router removed", connId);
        super.handlerRemoved(ctx);

        // Make sure any target connections that are still open are shut down cleanly
        var targetKeys = new ArrayList<>(this.targets.keySet());
        targetKeys.forEach(this::closeAndRemoveTarget);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {

        log.info("conn = {}, channel registered", connId);
        super.channelRegistered(ctx);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {

        // Do not try to close the channel twice
        if (!ctx.channel().isOpen()) {
            promise.setSuccess();
            return;
        }

        log.info("conn = {}, connection closed", connId);
        super.close(ctx, promise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {

        log.info("conn = {}, channel disconnected", connId);
        super.disconnect(ctx, promise);
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

    protected abstract ChannelInitializer<Channel> initializeProxyRoute(
            ChannelHandlerContext ctx, CoreRouterLink link, Route routeConfig);

    protected abstract void reportProxyRouteError(
            ChannelHandlerContext ctx, Throwable error, boolean direction);


    protected final Route lookupRoute(URI uri, HttpMethod method, long requestId) {

        for (var route : this.routes) {
            if (route.getMatcher().matches(method, uri)) {

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


    protected final void openProxyChannel(TargetChannelState target, Route route, ChannelHandlerContext ctx) {

        var targetConfig = route.getConfig().getTarget();

        // Router link signals the router link future once the link is active
        var routerLinkFuture = ctx.newPromise();
        var routerLink = new CoreRouterLink(this, ctx, routerLinkFuture, route.getIndex(), connId);

        // Channel initializer is built for the route
        // It will insert the router link as the last handler in the chain
        var channelInit = initializeProxyRoute(ctx, routerLink, route);

        target.channelOpenFuture = bootstrap
                .handler(channelInit)
                .connect(targetConfig.getHost(), targetConfig.getPort());

        target.channel = target.channelOpenFuture.channel();
        target.channelActiveFuture = routerLinkFuture;
        target.channelCloseFuture = target.channel.closeFuture();

        target.routeIndex = route.getIndex();

        target.channelOpenFuture.addListener(future -> proxyChannelOpen(ctx, target, future));
        target.channelActiveFuture.addListener(future -> proxyChannelActive(ctx, target, future));
        target.channelCloseFuture.addListener(future -> proxyChannelClosed(ctx, target, future));
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

            reportProxyRouteError(ctx, future.cause(), CoreRouterLink.WRITE_DIRECTION);

            while (!target.outboundQueue.isEmpty()) {
                var queuedMsg = target.outboundQueue.poll();
                ReferenceCountUtil.release(queuedMsg);
            }

            targets.remove(target.routeIndex);
        }
    }

    private void proxyChannelActive(ChannelHandlerContext ctx, TargetChannelState target, Future<?> future) {

        // Errors here are unexpected, this means there could be inconsistent state
        // Take the nuclear option and kill the client connection

        if (!future.isSuccess()) {

            log.error("conn = {}, Unexpected error in proxy channel activation", connId, future.cause());

            reportProxyRouteError(ctx, future.cause(), CoreRouterLink.WRITE_DIRECTION);

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

    protected final void relayMessage(TargetChannelState target, Object msg) {

        if (target.channelActiveFuture.isSuccess()) {

            if (log.isTraceEnabled()) {

                log.trace("conn = {}, target = {}, relaying message",
                        connId, target.channel.remoteAddress());
            }

            target.channel.write(msg);
        }
        else {

            if (log.isTraceEnabled()) {

                log.trace("conn = {}, target = {}, queuing message, queue size = [{}]",
                        connId, target.channel.remoteAddress(),
                        target.outboundQueue.size());
            }

            target.outboundQueue.add(msg);
        }
    }

    protected final void flushMessages(TargetChannelState target) {

        if (target.channelActiveFuture.isSuccess()) {

            if (log.isTraceEnabled()) {

                log.trace("conn = {}, target = {}, flushing messages",
                        connId, target.channel.remoteAddress());
            }

            target.channel.flush();
        }
    }

    protected final TargetChannelState getOrCreateTarget(ChannelHandlerContext ctx, Route route) {

        var routeId = route.getIndex();
        var existingTarget = getTarget(routeId);

        if (existingTarget != null)
            return existingTarget;

        var target = new TargetChannelState();
        targets.put(routeId, target);

        openProxyChannel(target, route, ctx);

        return target;
    }

    protected final TargetChannelState getTarget(int routeId) {

        return targets.getOrDefault(routeId, null);
    }

    protected final void closeAndRemoveTarget(int routeId) {

        var target = this.targets.remove(routeId);

        if (target != null) {

            for (var obj : target.outboundQueue)
                ReferenceCountUtil.release(obj);

            if (target.channel.isOpen())
                target.channel.close();
        }
    }

    final void associateRoute(Object msg, int routeIndex) {
        this.routeAssociation.put(msg, routeIndex);
    }

    final Integer getAssociatedRoute(Object msg) {
        return this.routeAssociation.get(msg);
    }

    final void destroyAssociation(Object msg) {
        this.routeAssociation.remove(msg);
    }

    protected static final class TargetChannelState {

        int routeIndex;

        Channel channel;
        ChannelFuture channelOpenFuture;
        ChannelFuture channelCloseFuture;
        ChannelFuture channelActiveFuture;

        Queue<Object> outboundQueue = new LinkedList<>();
    }

}