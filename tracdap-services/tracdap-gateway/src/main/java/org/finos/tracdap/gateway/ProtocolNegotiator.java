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

package org.finos.tracdap.gateway;

import org.finos.tracdap.common.auth.JwtValidator;
import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.config.ServiceProperties;
import org.finos.tracdap.common.netty.BaseProtocolNegotiator;
import org.finos.tracdap.gateway.auth.AuthHandlerSettings;
import org.finos.tracdap.gateway.auth.Http1AuthHandler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import org.finos.tracdap.gateway.exec.Redirect;
import org.finos.tracdap.gateway.exec.Route;
import org.finos.tracdap.gateway.routing.Http1Router;
import org.finos.tracdap.gateway.routing.Http2Router;
import org.finos.tracdap.gateway.routing.WebSocketsRouter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public class ProtocolNegotiator extends BaseProtocolNegotiator {

    private final AuthHandlerSettings authSettings;
    private final JwtValidator jwtValidator;
    private final List<Route> routes;
    private final List<Redirect> redirects;

    private final AtomicInteger connId = new AtomicInteger();

    public ProtocolNegotiator(
            Properties serviceProperties, AuthHandlerSettings authSettings,
            JwtValidator jwtValidator, List<Route> routes, List<Redirect> redirects) {

        super(true, true, true, getIdleTimeout(serviceProperties));

        this.authSettings = authSettings;
        this.jwtValidator = jwtValidator;
        this.routes = routes;
        this.redirects = redirects;
    }

    private static int getIdleTimeout(Properties serviceProperties) {

        return ConfigHelpers.readInt(
                "gateway service", serviceProperties,
                ServiceProperties.NETWORK_IDLE_TIMEOUT,
                ServiceProperties.NETWORK_READ_TIMEOUT_DEFAULT);
    }

    @Override
    protected ChannelInboundHandler http1AuthHandler() {
        return new Http1AuthHandler(authSettings, jwtValidator);
    }

    @Override
    protected ChannelInboundHandler http2AuthHandler() {
        return new ChannelDuplexHandler();
    }

    @Override
    protected ChannelHandler http1PrimaryHandler() {
        return new Http1Router(routes, redirects, connId.getAndIncrement());
    }

    @Override
    protected ChannelHandler http2PrimaryHandler() {
        return new Http2Router(routes);
    }

    @Override
    protected ChannelHandler wsPrimaryHandler() {
        return new WebSocketsRouter(routes, connId.getAndIncrement());
    }

    @Override
    protected WebSocketServerProtocolConfig wsProtocolConfig() {

        return WebSocketServerProtocolConfig.newBuilder()
                .subprotocols("grpc-websockets")
                .allowExtensions(true)
                .handleCloseFrames(false)
                .build();
    }
}
