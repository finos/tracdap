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

package org.finos.tracdap.gateway.proxy.http;

import org.finos.tracdap.common.util.LoggingHelpers;
import org.finos.tracdap.config.RouteConfig;

import io.netty.channel.*;
import io.netty.handler.codec.http.HttpClientCodec;
import org.slf4j.Logger;


public class Http1ProxyBuilder extends ChannelInitializer<Channel> {

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

    private final RouteConfig routeConfig;
    ChannelDuplexHandler routerLink;
    private final long connId;

    public Http1ProxyBuilder(
            RouteConfig routeConfig,
            ChannelDuplexHandler routerLink,
            long connId) {

        this.routeConfig = routeConfig;
        this.routerLink = routerLink;
        this.connId = connId;
    }

    @Override
    protected void initChannel(Channel channel) {

        if (log.isDebugEnabled())
            log.debug("conn = {}, Init HTTP/1.1 proxy channel", connId);

        var pipeline = channel.pipeline();
        pipeline.addLast(new HttpClientCodec());
        pipeline.addLast(new Http1Proxy(routeConfig, connId));
        pipeline.addLast(routerLink);
    }
}
