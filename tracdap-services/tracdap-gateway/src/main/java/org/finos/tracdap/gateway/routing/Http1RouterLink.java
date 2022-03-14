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
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This router link is for an HTTP/1 router with an HTTP/1 client-side connection.
 *
 * The router link handler will be the last handler on a server-side channel.
 * It is responsible for relaying messages and errors to and from the router handler,
 * which will be the last handler on a client-side channel. In the case of an unhandled
 * error on the server-side channel, the router link is also responsible for either
 * handling and relaying the error or, if the error cannot be handled cleanly, closing
 * the client-side channel. The router link performs no translation on the type or
 * content of messages.
 */
public class Http1RouterLink extends ChannelDuplexHandler {

    private static final boolean WRITE_DIRECTION = true;
    private static final boolean READ_DIRECTION = false;
    private static final HttpVersion PROTOCOL_VERSION = HttpVersion.HTTP_1_1;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ChannelHandlerContext routerCtx;
    private final ChannelPromise routeActivePromise;

    private final Http1Router router;
    private final int routeIndex;

    public Http1RouterLink(
            ChannelHandlerContext routerCtx, ChannelPromise routeActivePromise,
            Http1Router router, int routeIndex) {

        this.routerCtx = routerCtx;
        this.routeActivePromise = routeActivePromise;

        this.router = router;
        this.routeIndex = routeIndex;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        // This is the top level handler on the proxy route
        // Once this handler is active, the route is active
        // This signal tells the router it can start sending messages on the route

        log.info("Route link handler now active");
        routeActivePromise.setSuccess();

        super.channelActive(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if (log.isDebugEnabled())
            log.debug("Router link outbound message of type {}", msg.getClass().getSimpleName());

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
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        if (log.isDebugEnabled())
            log.debug("Router link inbound message of type {}", msg.getClass().getSimpleName());

        router.associateRoute(msg, routeIndex);

        // For inbound messages, we want to relay through the whole of the client-side channel
        // Calling .channel().write() means the message will pass through all handlers, including the router handler
        // Calling routerCtx.write() will write the message into the pipeline after routerCtx, i.e. skipping the router

        routerCtx.channel().write(msg);

        // On keep-alive connections, we want to flush when the last response
        // content for a single response is received, regardless of whether the
        // connection is going to close or not

        if (msg instanceof LastHttpContent)
            routerCtx.channel().flush();
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

    private void handleError(ChannelHandlerContext ctx, Throwable cause, boolean direction) {

        // Errors that reach this handler have bubbled up to the top of the server-side pipeline
        // When this happens, we need to do two things:
        // - Relay an error response to the client-side channel (or close the channel)
        // - Close the server-side channel, i.e. this handler's channel

        // The router will detect the server side channel has closed. It may attempt to create
        // a new server side channel to answer future requests for this route.

        // An error may occur straight away when a client tries to send a request to a route,
        // or at the beginning of a new request on a keep-alive connection. In either case this
        // approach is fine. However, it may also be that an error occurs midway through serving
        // a request, e.g. if a connection drops midway through sending a large file. In this case
        // there is no way notify the client with an error response and the client connection needs
        // to be forcibly closed.

        // For pipelined requests, an error may occur in response to a pipelined request while a response
        // to a previous request is still being sent. In this case, the error response should be sent at
        // the correct point in the pipeline. In practice no major browsers use pipelining in HTTP/1.1
        // under their default settings.

        // In practice, handling all of these conditions correctly may be difficult to implement and to debug.
        // One simpler strategy would be to close the whole client connection by default when an error bubbles
        // up to this point. Then special cases can be added for very common, well-defined cases to provide a
        // better end-user experience.

        // TODO: Implement more robust error forwarding in the router link handler

        if (direction == WRITE_DIRECTION)
            log.error("Failed writing messages to proxy backend", cause);
        else
            log.error("Failed reading messages from proxy backend", cause);

        var response = new DefaultHttpResponse(PROTOCOL_VERSION, HttpResponseStatus.BAD_GATEWAY);
        routerCtx.channel().writeAndFlush(response);

        ctx.close();
    }
}
