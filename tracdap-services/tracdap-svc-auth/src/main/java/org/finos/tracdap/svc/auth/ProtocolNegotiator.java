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

package org.finos.tracdap.svc.auth;

import io.netty.handler.codec.http.HttpRequest;
import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.config.ServiceProperties;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.netty.BaseProtocolNegotiator;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;

import java.util.Properties;


public class ProtocolNegotiator extends BaseProtocolNegotiator {

    private final ProviderLookup providerLookup;

    public ProtocolNegotiator(Properties serviceProperties, ProviderLookup providerLookup) {
        super(false, false, false, getIdleTimeout(serviceProperties));
        this.providerLookup = providerLookup;
    }

    private static int getIdleTimeout(Properties serviceProperties) {

        return ConfigHelpers.readInt(
                "authentication service", serviceProperties,
                ServiceProperties.NETWORK_IDLE_TIMEOUT,
                ServiceProperties.NETWORK_IDLE_TIMEOUT_DEFAULT);
    }

    @Override
    protected ChannelInboundHandler http1AuthHandler() {
        // No-op handler, this service will handle auth
        return new ChannelInboundHandlerAdapter();
    }

    @Override
    protected ChannelInboundHandler http2AuthHandler() {
        throw new EUnexpected();
    }

    @Override
    protected ChannelHandler http1PrimaryHandler() {
        return new Http1ProviderLookup(providerLookup);
    }

    @Override
    protected ChannelHandler http2PrimaryHandler() {
        throw new EUnexpected();
    }

    @Override
    protected WebSocketServerProtocolConfig wsProtocolConfig(HttpRequest upgradeRequest) {
        throw new EUnexpected();
    }

    @Override
    protected ChannelHandler wsPrimaryHandler() {
        throw new EUnexpected();
    }
}
