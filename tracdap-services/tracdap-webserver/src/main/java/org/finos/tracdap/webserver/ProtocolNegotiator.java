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

package org.finos.tracdap.webserver;

import io.netty.handler.codec.http.HttpRequest;
import org.finos.tracdap.common.auth.Http1AuthValidator;
import org.finos.tracdap.common.auth.JwtValidator;
import org.finos.tracdap.common.netty.BaseProtocolNegotiator;
import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.config.PlatformConfig;

import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;

import org.apache.arrow.memory.BufferAllocator;


public class ProtocolNegotiator extends BaseProtocolNegotiator {

    private static final int DEFAULT_TIMEOUT = 60;

    private final AuthenticationConfig authConfig;
    private final JwtValidator jwtValidator;
    private final ContentServer contentServer;
    private final BufferAllocator arrowAllocator;

    public ProtocolNegotiator(
            PlatformConfig platformConfig,
            JwtValidator jwtValidator,
            ContentServer contentServer,
            BufferAllocator arrowAllocator) {

        super(false, false, false, DEFAULT_TIMEOUT);

        this.authConfig = platformConfig.getAuthentication();
        this.jwtValidator = jwtValidator;
        this.contentServer = contentServer;
        this.arrowAllocator = arrowAllocator;
    }

    @Override
    protected ChannelInboundHandler http1AuthHandler() {
        return new Http1AuthValidator(authConfig, jwtValidator);
    }

    @Override
    protected ChannelInboundHandler http2AuthHandler() {
        return null;
    }

    @Override
    protected ChannelHandler http1PrimaryHandler() {
        return new Http1Server(contentServer, arrowAllocator);
    }

    @Override
    protected ChannelHandler http2PrimaryHandler() {
        return null;
    }

    @Override
    protected WebSocketServerProtocolConfig wsProtocolConfig(HttpRequest upgradeRequest) {
        return null;
    }

    @Override
    protected ChannelHandler wsPrimaryHandler() {
        return null;
    }
}
