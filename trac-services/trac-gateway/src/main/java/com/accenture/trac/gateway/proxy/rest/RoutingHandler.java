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

package com.accenture.trac.gateway.proxy.rest;

import com.accenture.trac.common.exception.EUnexpected;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;


public class RoutingHandler extends ChannelInboundHandlerAdapter {

    private final Logger log;
    private final RoutingConfig routerConfig;

    private boolean routingDone;
    private HttpVersion protocolVersion = HttpVersion.HTTP_1_1;

    public RoutingHandler(RoutingConfig routerConfig) {

        this.log = LoggerFactory.getLogger(getClass());
        this.routerConfig = routerConfig;

        routingDone = false;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        if (!routingDone) {

            // RequestRouter can only be used in a pipeline after an HTTP codec
            if (!(msg instanceof HttpRequest))
                throw new EUnexpected();

            var request = (HttpRequest) msg;
            this.protocolVersion = request.protocolVersion();

            // Look up the handler for this request
            var uri = URI.create(request.uri());
            var method = request.method();
            var headers = request.headers();
            var handler = routerConfig.matchRequest(uri, method, headers);

            if (handler != null) {

                // A route is available, great!
                // Swap out the router for the selected handler and pass on the message
                // The router will not process any more messages for this request
                ctx.pipeline().addLast(handler);
                ctx.pipeline().remove(this);
                ctx.fireChannelRead(msg);
            }
            else {

                // No route available, send a 404 back to the client
                log.warn("No route available: " + method + " " + uri);

                var response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.NOT_FOUND);
                ctx.writeAndFlush(response);
                ctx.close();

                // In case the message has any content, we need to release it
                // E.g. if there is an aggregator in the pipeline, the first request will contain header and body
                ReferenceCountUtil.release(msg);
            }

            // Only do routing once per request
            routingDone = true;
        }
        else {

            // If there was no route available, there may still be inbound messages to consume
            // Make sure these are discarded
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        log.error("Unhandled error during routing", cause);

        var response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        ctx.writeAndFlush(response);
        ctx.close();
    }
}
