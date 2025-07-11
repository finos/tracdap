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

package org.finos.tracdap.gateway.routing;

import io.netty.channel.embedded.EmbeddedChannel;
import org.finos.tracdap.config.RoutingProtocol;
import org.finos.tracdap.gateway.exec.Redirect;
import org.finos.tracdap.gateway.exec.Route;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
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
    protected final List<Redirect> redirects;
    protected final int connId;
    protected final String protocol;

    private final Map<Integer, TargetChannelState> targets;
    private final Map<Object, Integer> routeAssociation;

    protected Bootstrap bootstrap;

    public CoreRouter(List<Route> routes, int connId, String protocol) {

        this(routes, null, connId, protocol);
    }

    public CoreRouter(List<Route> routes, List<Redirect> redirects, int connId, String protocol) {

        this.routes = routes;
        this.redirects = redirects;
        this.connId = connId;
        this.protocol = protocol;

        this.targets = new HashMap<>();
        this.routeAssociation = new HashMap<>();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

        if (log.isDebugEnabled())
            log.debug("{} handlerAdded: conn = {}, protocol = {}", getClass().getSimpleName(), connId, protocol);

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

        if (log.isDebugEnabled())
            log.debug("{} handlerRemoved: conn = {}", getClass().getSimpleName(), connId);

        super.handlerRemoved(ctx);

        // Make sure any target connections that are still open are shut down cleanly
        var targetKeys = new ArrayList<>(this.targets.keySet());
        targetKeys.forEach(this::closeAndRemoveTarget);
    }

    protected abstract ChannelInitializer<Channel> initializeProxyRoute(
            ChannelHandlerContext ctx, CoreRouterLink link, Route routeConfig);

    protected abstract void reportProxyRouteError(
            ChannelHandlerContext ctx, Throwable error, boolean direction);


    protected final Redirect lookupRedirect(URI uri, HttpMethod method, long requestId) {

        if (this.redirects == null)
            return null;

        for (var redirect : this.redirects) {
            if (redirect.getMatcher().matches(method, uri)) {

                log.info("REDIRECT: conn = {}, req = {}, {} {} -> {} ({})",
                        connId, requestId,
                        method, uri,
                        redirect.getConfig().getTarget(),
                        redirect.getConfig().getStatus());

                return redirect;
            }
        }

        return null;
    }


    protected final Route lookupRoute(URI uri, HttpMethod method, long requestId) {

        for (var route : this.routes) {
            if (route.getMatcher().matches(method, uri)) {

                log.info("ROUTING: conn = {}, req = {}, {} {} -> {} ({})",
                        connId, requestId,
                        method, uri,
                        route.getConfig().getRouteName(),
                        route.getConfig().getRouteType());

                return route;
            }
        }

        // No route available, send a 404 back to the client
        log.error("ROUTE NOT FOUND: conn = {}, req = {}, {} {} ",
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

        if (route.getConfig().getRouteType() == RoutingProtocol.INTERNAL) {

            try {

                // Internal routes use an embedded channel instead of a real connection
                // Channel init will run synchronously
                var channel = new EmbeddedChannel(channelInit);
                target.channel = channel;

                // Embedded channel is already in opened state
                target.channelOpenFuture = ctx.newSucceededFuture();
                target.channelActiveFuture = routerLinkFuture;
                target.channelCloseFuture = channel.closeFuture();
            }
            catch (Throwable t) {

                // If there are errors during init, the cleanup actions still need to run
                // Set up the channel futures to make that happen
                target.channel = null;
                target.channelOpenFuture = ctx.newFailedFuture(t);
                target.channelActiveFuture = ctx.newFailedFuture(t);
                target.channelCloseFuture = ctx.newSucceededFuture();
            }
        }
        else {

            // Regular proxy connections are started with a client channel bootstrap
            target.channelOpenFuture = bootstrap
                    .handler(channelInit)
                    .connect(targetConfig.getHost(), targetConfig.getPort());

            // Set up channel event futures - init will run on successful open
            target.channel = target.channelOpenFuture.channel();
            target.channelActiveFuture = routerLinkFuture;
            target.channelCloseFuture = target.channel.closeFuture();
        }

        target.routeIndex = route.getIndex();

        // Set up handlers for channel lifecycle events
        target.channelOpenFuture.addListener(future -> proxyChannelOpen(ctx, target, future));
        target.channelActiveFuture.addListener(future -> proxyChannelActive(ctx, target, future));
        target.channelCloseFuture.addListener(future -> proxyChannelClosed(ctx, target, future));
    }

    private void proxyChannelOpen(ChannelHandlerContext ctx, TargetChannelState target, Future<?> future) {

        // TODO: Check reason for connect failure
        // TODO: Handle pipelining - there could be multiple queued requests, and/or requests to other targets

        var targetConfig = routes.get(target.routeIndex).getConfig().getTarget();

        if (future.isSuccess()) {

            log.info("PROXY CONNECT: conn = {}, target = {} {}", connId, targetConfig.getHost(), targetConfig.getPort());
        }
        else {

            if (future.cause() != null) {
                log.error(
                        "PROXY CONNECT FAILED: conn = {}, target = {} {}, {}",
                        connId, targetConfig.getHost(), targetConfig.getPort(), future.cause().getMessage(),
                        future.cause());
            }
            else {
                log.error(
                        "PROXY CONNECT FAILED: conn = {}, target = {} {}, {}",
                        connId, targetConfig.getHost(), targetConfig.getPort(), "No details available");
            }

            while (!target.outboundQueue.isEmpty()) {
                var queuedMsg = target.outboundQueue.poll();
                ReferenceCountUtil.release(queuedMsg);
            }

            // No need to close the target channel, it did not ever open
            // Remove the target, so closing the router does not try to clean it up
            targets.remove(target.routeIndex);

            reportProxyRouteError(ctx, future.cause(), CoreRouterLink.WRITE_DIRECTION);
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
            log.info("PROXY DISCONNECT: conn = {}, target = {} {}", connId, targetConfig.getHost(), targetConfig.getPort());
        else
            log.error("PROXY DISCONNECT FAILED: conn = {}, target = {} {}", connId, targetConfig.getHost(), targetConfig.getPort(), future.cause());

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