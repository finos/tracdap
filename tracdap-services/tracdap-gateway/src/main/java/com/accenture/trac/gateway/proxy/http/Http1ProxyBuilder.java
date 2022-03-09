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

package com.accenture.trac.gateway.proxy.http;

import com.accenture.trac.config.GwRoute;

import io.netty.channel.*;
import io.netty.handler.codec.http.HttpClientCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Http1ProxyBuilder extends ChannelInitializer<Channel> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final GwRoute routeConfig;
    ChannelDuplexHandler routerLink;

    public Http1ProxyBuilder(
            GwRoute routeConfig,
            ChannelDuplexHandler routerLink) {

        this.routeConfig = routeConfig;
        this.routerLink = routerLink;
    }

    @Override
    protected void initChannel(Channel channel) {

        log.info("Init HTTP/1.1 proxy channel");

        var pipeline = channel.pipeline();
        pipeline.addLast(new HttpClientCodec());
        pipeline.addLast(new Http1Proxy(routeConfig));
        pipeline.addLast(routerLink);
    }
}
