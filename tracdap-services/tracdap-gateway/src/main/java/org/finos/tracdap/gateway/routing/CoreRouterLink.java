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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;


/**
 * The core router link connects the server-side and client-side channels.
 *
 * <p>The router link handler will be the last handler on a server-side channel.
 * It is responsible for relaying messages and errors to and from the router handler,
 * which will be the last handler on a client-side channel. In the case of an unhandled
 * error on the server-side channel, the router link is also responsible for either
 * handling and relaying the error or, if the error cannot be handled cleanly, closing
 * the client-side channel.</>
 *
 * <p>There is only one router link implementation for all routed protocols.
 * The router link does not inspect or translate the content of messages.</p>
 */
public class CoreRouterLink extends ChannelDuplexHandler {

    public static final boolean WRITE_DIRECTION = true;
    public static final boolean READ_DIRECTION = false;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final CoreRouter router;
    private final ChannelHandlerContext routerCtx;
    private final ChannelPromise routeActivePromise;

    private final int routeIndex;
    private final int connId;

    public CoreRouterLink(
            CoreRouter router, ChannelHandlerContext routerCtx,
            ChannelPromise routeActivePromise,
            int routeIndex, int connId) {

        this.router = router;
        this.routerCtx = routerCtx;
        this.routeActivePromise = routeActivePromise;


        this.routeIndex = routeIndex;
        this.connId = connId;
    }

    @Override
    public void channelActive(@Nonnull ChannelHandlerContext ctx) throws Exception {

        // This is the top level handler on the proxy route
        // Once this handler is active, the route is active
        // This signal tells the router it can start sending messages on the route

        if (log.isDebugEnabled())
            log.debug("conn = {}, Route link handler now active", connId);

        routeActivePromise.setSuccess();

        super.channelActive(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if (log.isDebugEnabled())
            log.debug("conn = {}, Router link outbound message of type {}", connId, msg.getClass().getSimpleName());

        // For outbound messages, the router is already running on the target channel
        // So simply pass the message along the pipeline as-is

        ctx.write(msg, promise);

        // The router link is the top level handler on the server-side channel, errors can propagate no further
        // Any errors that do occur need to be handled and relayed to the client-side channel

        promise.addListener(fut -> {
            if (!fut.isSuccess())
                handleError(ctx, fut.cause(), WRITE_DIRECTION);
        });
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        if (log.isDebugEnabled())
            log.debug("conn = {}, Router link inbound message of type {}", connId, msg.getClass().getSimpleName());

        // For inbound messages, we want to relay through the whole of the client-side channel
        // Calling .pipeline().write() means the message will pass through all handlers, including the router handler
        // Calling routerCtx.write() will write the message into the pipeline after routerCtx, i.e. skipping the router

        router.associateRoute(msg, routeIndex);
        routerCtx.pipeline().write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {

        routerCtx.channel().flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        // Catch errors during read operations
        // Since this is the top level handler on the route, there is no further for them to propagate

        handleError(ctx, cause, READ_DIRECTION);
    }

    private void handleError(ChannelHandlerContext ctx, Throwable error, boolean direction) {

        // Errors that reach this handler have bubbled up to the top of the server-side pipeline
        // When this happens, we need to do two things:
        // - Relay an error response to the client-side router (or close the client-side channel)
        // - Close the server-side channel, i.e. this handler's channel

        // The router will detect the server side channel has closed. It may attempt to create
        // a new server side channel to answer future requests for this route.

        // How the client-side router chooses to report the error will depend on the protocol.
        // For example in HTTP/2 it may be possible to fail a single stream.

        try {

            if (direction == WRITE_DIRECTION)
                log.error("conn = {}, Failed writing messages to proxy backend", connId, error);
            else
                log.error("conn = {}, Failed reading messages from proxy backend", connId, error);

            router.reportProxyRouteError(routerCtx, error, direction);
        }
        finally {

            ctx.close();
        }
    }
}
