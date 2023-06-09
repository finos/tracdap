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

package org.finos.tracdap.gateway.proxy.rest;

import org.finos.tracdap.common.exception.ENetworkHttp;
import org.finos.tracdap.gateway.exec.Route;
import org.finos.tracdap.gateway.proxy.http.Http1to2Proxy;
import org.finos.tracdap.gateway.proxy.http.HttpProtocol;
import org.finos.tracdap.gateway.routing.CoreRouterLink;

import io.netty.channel.*;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RestApiProxyBuilder extends ChannelInitializer<Channel> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Route routeConfig;
    private final CoreRouterLink routerLink;
    private final int connId;
    private final HttpProtocol httpProtocol;

    public RestApiProxyBuilder(
            Route routeConfig,
            CoreRouterLink routerLink,
            int connId,
            HttpProtocol httpProtocol) {

        this.routeConfig = routeConfig;
        this.routerLink = routerLink;
        this.connId = connId;
        this.httpProtocol = httpProtocol;
    }

    @Override
    protected void initChannel(Channel channel) {

        log.info("conn = {}, Init REST proxy channel", connId);

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
        var restApiConfig = routeConfig.getRestMethods();
        var restApiProxy = new RestApiProxy(restApiConfig);
        pipeline.addLast(restApiProxy);

        // Router link

        if (httpProtocol == HttpProtocol.HTTP_1_0 || httpProtocol == HttpProtocol.HTTP_1_1) {

            pipeline.addLast(new Http1to2Proxy(routeConfig.getConfig(), connId));
            pipeline.addLast(routerLink);
        }
        else {

            var message = String.format(
                    "HTTP protocol version [%s] is not supported for target [%s]",
                    httpProtocol.name(), routeConfig.getConfig().getRouteName());

            throw new ENetworkHttp(HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED.code(), message);
        }
    }
}
