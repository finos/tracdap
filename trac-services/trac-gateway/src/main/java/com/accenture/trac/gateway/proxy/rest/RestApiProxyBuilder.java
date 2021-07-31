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
import com.accenture.trac.gateway.config.RouteConfig;
import com.accenture.trac.gateway.config.rest.MetaApiRestMapping;
import com.accenture.trac.gateway.proxy.http.Http1RouterLink;
import com.accenture.trac.gateway.proxy.http.Http1to2Framing;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;

import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RestApiProxyBuilder extends ChannelInitializer<Channel> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final RouteConfig routeConfig;
    private final int sourceHttpVersion;
    private final ChannelHandlerContext routerCtx;
    private final ChannelPromise routeActivePromise;
    private final EventExecutor executor;

    public RestApiProxyBuilder(
            RouteConfig routeConfig,
            int sourceHttpVersion,
            ChannelHandlerContext routerCtx,
            ChannelPromise routeActivePromise,
            EventExecutor executor) {

        this.routeConfig = routeConfig;
        this.sourceHttpVersion = sourceHttpVersion;
        this.routerCtx = routerCtx;
        this.routeActivePromise = routeActivePromise;
        this.executor = executor;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {

        log.info("Init REST proxy channel");

        var pipeline = channel.pipeline();

        // HTTP/2 Codec, required for channels using the HTTP frame objects

        var initialSettings = new Http2Settings()
                .maxFrameSize(16 * 1024);

        var http2Codec = Http2FrameCodecBuilder.forClient()
                .frameLogger(new Http2FrameLogger(LogLevel.INFO))
                .initialSettings(initialSettings)
                .autoAckSettingsFrame(true)
                .autoAckPingFrame(true)
                .build();

        pipeline.addLast(http2Codec);

        // REST proxy

        // TODO: Build this after reading service config and pass it in
        var restApiConfig = MetaApiRestMapping.metaApiRoutes();

        var grpcHost = routeConfig.getTarget().getHost();
        var grpcPort = routeConfig.getTarget().getPort();
        var restApiProxy = new RestApiProxy(grpcHost, grpcPort, restApiConfig, executor);
        pipeline.addLast(restApiProxy);

        // Router link

        if (sourceHttpVersion == 1) {

            pipeline.addLast(new Http1to2Framing());
            pipeline.addLast(new Http1RouterLink(routerCtx, routeActivePromise));
        }
        else if (sourceHttpVersion == 2) {

            throw new RuntimeException("HTTP/2 source connection for REST not implemented yet");
        }
        else
            throw new EUnexpected();
    }
}
