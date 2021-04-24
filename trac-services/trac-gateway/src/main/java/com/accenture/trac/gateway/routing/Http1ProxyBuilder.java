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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpClientCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Http1ProxyBuilder extends ChannelInitializer<Channel> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ChannelHandlerContext routerCtx;
    private final ChannelPromise routeActivePromise;

    Http1ProxyBuilder(ChannelHandlerContext routerCtx, ChannelPromise routeActivePromise) {
        this.routerCtx = routerCtx;
        this.routeActivePromise = routeActivePromise;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {

        log.info("Init HTTP/1.1 proxy channel");

        var pipeline = channel.pipeline();
        pipeline.addLast(new HttpClientCodec());
        pipeline.addLast(new Http1Proxy());
        pipeline.addLast(new Http1RouterLink(routerCtx, routeActivePromise));
    }
}
