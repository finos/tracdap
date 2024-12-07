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

package org.finos.tracdap.auth.login;

import org.finos.tracdap.auth.provider.IAuthProvider;
import org.finos.tracdap.common.auth.internal.JwtProcessor;
import org.finos.tracdap.config.AuthenticationConfig;


import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http2.Http2Headers;

public class LoginAuthProvider implements IAuthProvider {

    private final AuthenticationConfig authConfig;
    private final JwtProcessor jwtProcessor;
    private final ILoginProvider loginProvider;

    public LoginAuthProvider(
            AuthenticationConfig authConfig,
            JwtProcessor jwtProcessor,
            ILoginProvider loginProvider) {

        this.authConfig = authConfig;
        this.jwtProcessor = jwtProcessor;
        this.loginProvider = loginProvider;
    }

    @Override
    public boolean canHandleHttp1(HttpRequest request) {
        return request.uri().startsWith(LoginContent.LOGIN_PATH_PREFIX);
    }

    @Override
    public ChannelInboundHandler createHttp1Handler() {
        return new Http1LoginHandler(authConfig, jwtProcessor, loginProvider);
    }

    @Override
    public boolean canHandleHttp2(Http2Headers headers) {
        return false;
    }

    @Override
    public ChannelInboundHandler createHttp2Handler() {
        return null;
    }
}
