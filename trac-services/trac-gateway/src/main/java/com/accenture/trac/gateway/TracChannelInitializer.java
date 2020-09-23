/*
 * Copyright 2020 Accenture Global Solutions Limited
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

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.gateway.routing.RoutingConfig;
import com.accenture.trac.gateway.routing.RoutingHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final RoutingConfig routes;

    public TracChannelInitializer(RoutingConfig routes) {

        log.info("Created channel initializer");

        this.routes = routes;
    }

    @Override
    protected void initChannel(SocketChannel channel) {

        var pipeline = channel.pipeline();

        pipeline.remove(this);
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpRequestLogger());
        pipeline.addLast(new RoutingHandler(routes));
    }

    private class HttpRequestLogger extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {

            if (!(msg instanceof HttpRequest))
                throw new EUnexpected();

            var remoteSocket = ctx.channel().remoteAddress();
            var httpRequest = (HttpRequest) msg;
            var httpMethod = httpRequest.method();
            var url = httpRequest.uri();

            log.info("{} -> {} {}", remoteSocket, httpMethod, url);

            ctx.pipeline().remove(this);
            ctx.fireChannelRead(msg);
        }
    }
}
