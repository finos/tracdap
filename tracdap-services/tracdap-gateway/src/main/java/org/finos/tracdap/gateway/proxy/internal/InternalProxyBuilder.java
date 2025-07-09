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

package org.finos.tracdap.gateway.proxy.internal;

import io.netty.channel.ChannelDuplexHandler;
import org.finos.tracdap.common.exception.ENetworkHttp;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.util.LoggingHelpers;
import org.finos.tracdap.config.RouteConfig;
import org.finos.tracdap.gateway.proxy.http.HttpProtocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;


public class InternalProxyBuilder extends ChannelInitializer<Channel> {

    private static final String INTERNAL_PROXY = "INTERNAL_PROXY";
    private static final String CORE_ROUTER_LINK = "CORE_ROUTER_LINK";

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

    private final RouteConfig routeConfig;
    private final ChannelDuplexHandler routerLink;
    private final HttpProtocol httpProtocol;

    private final int connId;

    public InternalProxyBuilder(
            RouteConfig routeConfig, ChannelDuplexHandler routerLink,
            int connId, HttpProtocol httpProtocol) {

        this.httpProtocol = httpProtocol;
        this.routeConfig = routeConfig;
        this.routerLink = routerLink;
        this.connId = connId;
    }

    @Override
    protected void initChannel(Channel channel) {

        if (log.isDebugEnabled())
            log.debug("conn = {}, Init internal proxy channel", connId);

        if (!(channel instanceof EmbeddedChannel))
            throw new EUnexpected();

        var relayChannel = (EmbeddedChannel) channel;
        var targetChannel = new EmbeddedChannel();

        try {
            initRelayChannel(relayChannel, targetChannel);
            initTargetChannel(relayChannel, targetChannel);
        }
        catch (Throwable setupError) {
            // The relay handler will close target channel
            // Setup errors can happen before that handler is installed
            targetChannel.close();
            throw setupError;
        }
    }

    private void initRelayChannel(Channel relayChannel, EmbeddedChannel targetChannel) {

        var relayPipeline = relayChannel.pipeline();

        relayPipeline.addLast(INTERNAL_PROXY, new InternalProxyRelay(targetChannel, connId));
        relayPipeline.addLast(CORE_ROUTER_LINK, routerLink);
    }

    private void initTargetChannel(EmbeddedChannel relayChannel, Channel targetChannel) {

        var targetPipeline = targetChannel.pipeline();
        targetPipeline.addLast(INTERNAL_PROXY, new InternalProxyRelay(relayChannel, connId));

        var targetProtocol = routeConfig.getTarget().getScheme();

        if (targetProtocol.equals(HealthCheckHandler.PROTOCOL)) {
            targetPipeline.addLast(new HealthCheckHandler(httpProtocol, connId));
        }
        else {
            var message = String.format("Internal protocol [%s] is not supported", targetProtocol);
            throw new ENetworkHttp(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), message);
        }
    }
}
