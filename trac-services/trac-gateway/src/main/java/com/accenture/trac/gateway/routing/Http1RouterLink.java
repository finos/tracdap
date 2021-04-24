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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Http1RouterLink extends ChannelDuplexHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final boolean WRITE_DIRECTION = true;
    private static final boolean READ_DIRECTION = false;

    private final ChannelHandlerContext routerCtx;
    private final ChannelPromise routeActivePromise;

    private int serverSeqId = 0;
    private final HttpVersion protocolVersion = HttpVersion.HTTP_1_1;

    Http1RouterLink(ChannelHandlerContext routerCtx, ChannelPromise routeActivePromise) {
        this.routerCtx = routerCtx;
        this.routeActivePromise = routeActivePromise;
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

        // Catch errors during write operations
        // Since this is the top level handler on the route, there is no further for them to propagate

        promise.addListener(fut -> {
            if (!fut.isSuccess())
                handleError(ctx, fut.cause(), WRITE_DIRECTION);
        });

        ctx.write(msg, promise);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        routerCtx.write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {

        routerCtx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        // Catch errors during read operations
        // Since this is the top level handler on the route, there is no further for them to propagate

        handleError(ctx, cause, READ_DIRECTION);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        super.userEventTriggered(ctx, evt);
    }

    private void handleError(ChannelHandlerContext ctx, Throwable cause, boolean direction) {

        // Errors that reach this handler have bubbled up to the top of the pipeline for the route
        // If this happens, we need to close the route channel,
        // and also send an error response to the router (i.e. to the client)

        // TODO: For pipelined requests, we would want to send the error at the right point in the pipeline
        // In practice no major browsers use pipelining in HTTP/1.1 under their default settings

        if (direction == WRITE_DIRECTION)
            log.error("Failed writing messages to proxy backend", cause);
        else
            log.error("Failed reading messages from proxy backend", cause);

        var response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.BAD_GATEWAY);
        routerCtx.writeAndFlush(response);

        ctx.close();
    }
}
