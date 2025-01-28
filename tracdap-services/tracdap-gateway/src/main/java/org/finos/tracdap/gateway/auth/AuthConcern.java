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

package org.finos.tracdap.gateway.auth;

import org.finos.tracdap.common.auth.JwtProcessor;
import org.finos.tracdap.common.auth.JwtSetup;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.middleware.*;
import org.finos.tracdap.config.PlatformConfig;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpServerCodec;


public class AuthConcern implements NettyConcern {

    public static final String AUTH_CONCERN_NAME = "trac_gw_authentication";

    private final AuthHandlerSettings authSettings;
    private final JwtProcessor jwtProcessor;

    public AuthConcern(ConfigManager configManager) {

        var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);

        this.authSettings = new AuthHandlerSettings(platformConfig);
        this.jwtProcessor = JwtSetup.createProcessor(platformConfig, configManager);
    }

    @Override
    public String concernName() {
        return AUTH_CONCERN_NAME;
    }

    @Override
    public void configureInboundChannel(ChannelPipeline pipeline, SupportedProtocol protocol) {

        if (protocol == SupportedProtocol.HTTP || protocol == SupportedProtocol.WEB_SOCKETS) {

            var httpCodec = pipeline.context(HttpServerCodec.class);
            var priorStage = httpCodec.name();
            var authHandler = new Http1AuthHandler(authSettings, jwtProcessor);
            pipeline.addAfter(priorStage, AUTH_CONCERN_NAME, authHandler);
        }
        else {

            throw new ETracInternal("No auth handler available for protocol: " + protocol);
        }
    }
}
