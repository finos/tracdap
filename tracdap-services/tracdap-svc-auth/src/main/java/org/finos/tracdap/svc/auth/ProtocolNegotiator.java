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

import org.finos.tracdap.common.auth.internal.JwtProcessor;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.netty.BaseProtocolNegotiator;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import org.finos.tracdap.config.PlatformConfig;


public class ProtocolNegotiator extends BaseProtocolNegotiator {

    private static final int DEFAULT_IDLE_TIMEOUT = 20;  // seconds, TODO

    private final PlatformConfig platformConfig;
    private final JwtProcessor jwtProcessor;

    public ProtocolNegotiator(PlatformConfig platformConfig, JwtProcessor jwtProcessor) {

        super(false, false, false, DEFAULT_IDLE_TIMEOUT);

        this.platformConfig = platformConfig;
        this.jwtProcessor = jwtProcessor;
    }

    @Override
    protected ChannelInboundHandler http1AuthHandler() {
        return new ChannelInboundHandlerAdapter();
    }

    @Override
    protected ChannelInboundHandler http2AuthHandler() {
        throw new EUnexpected();
    }

    @Override
    protected ChannelHandler http1PrimaryHandler() {

        var selector = new Http1AuthSelector();
        return new Http1AuthRouter(selector);
    }

    @Override
    protected ChannelHandler http2PrimaryHandler() {
        throw new EUnexpected();
    }

    @Override
    protected ChannelHandler wsPrimaryHandler() {
        throw new EUnexpected();
    }

    @Override
    protected WebSocketServerProtocolConfig wsProtocolConfig() {
        throw new EUnexpected();
    }
}
