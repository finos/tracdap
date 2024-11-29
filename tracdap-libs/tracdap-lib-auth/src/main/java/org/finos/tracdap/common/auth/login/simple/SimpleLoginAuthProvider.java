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

package org.finos.tracdap.common.auth.login.simple;

import org.finos.tracdap.common.auth.internal.JwtProcessor;
import org.finos.tracdap.common.auth.internal.JwtSetup;
import org.finos.tracdap.common.auth.login.Http1LoginHandler;
import org.finos.tracdap.common.auth.login.ILoginProvider;
import org.finos.tracdap.common.auth.provider.IAuthProvider;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.config.PlatformConfig;


import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http2.Http2Headers;

public class SimpleLoginAuthProvider implements IAuthProvider {

    private final AuthenticationConfig authConfig;
    private final JwtProcessor jwtProcessor;
    private final ILoginProvider loginProvider;

    public SimpleLoginAuthProvider(
            ConfigManager configManager,
            ILoginProvider loginProvider) {

        var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);

        this.authConfig = platformConfig.getAuthentication();
        this.jwtProcessor = JwtSetup.createProcessor(platformConfig, configManager);

        this.loginProvider = loginProvider;
    }

    @Override
    public boolean canHandleRequest(HttpRequest request) {
        return true;
    }

    @Override
    public boolean canHandleRequest(Http2Headers headers) {
        return false;
    }

    @Override
    public ChannelInboundHandler handleRequest(HttpRequest request) {
        return new Http1LoginHandler(authConfig, jwtProcessor, loginProvider);
    }

    @Override
    public ChannelInboundHandler handleRequest(Http2Headers request) {
        return null;
    }
}
