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

import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.finos.tracdap.common.auth.login.*;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.http.CommonHttpRequest;
import org.finos.tracdap.common.http.Http1Headers;

import io.netty.handler.codec.http.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


class BuiltInLoginProvider implements ILoginProvider {

    private static final Logger log = LoggerFactory.getLogger(BuiltInLoginProvider.class);

    private final IUserDatabase userDb;

    public BuiltInLoginProvider(ConfigManager configManager) {

        this.userDb = SimpleLoginPlugin.createUserDb(configManager);
    }

    @Override
    public AuthResult attemptLogin(CommonHttpRequest request) {

        var headers = Http1Headers.fromGenericHeaders(request.headers());

        // Only browser-based auth is supported with the built-in login provider
        if (!AuthHelpers.isBrowserRequest(headers))
            return AuthResult.FAILED("Session expired or not available");

        // Wait for content if it is not already available
        if (request.content() == null)
            return AuthResult.NEED_CONTENT();

        return checkLoginRequest(request);
    }

    private AuthResult checkLoginRequest(CommonHttpRequest request) {

        var content = request.content().toString(StandardCharsets.US_ASCII);
        var decoder = new QueryStringDecoder(LoginContent.LOGIN_URL + "?" + content);
        var loginParams = decoder.parameters();

        var usernameParam = loginParams.get("username");
        var passwordParam = loginParams.get("password");

        if (usernameParam == null || usernameParam.size() != 1 ||
            passwordParam == null || passwordParam.size() != 1) {

            var loginFormPage = LoginContent.getLoginFormPage();
            return AuthResult.OTHER_RESPONSE(loginFormPage);
        }

        var username = usernameParam.get(0);
        var password = passwordParam.get(0);

        if (LocalUsers.checkPassword(userDb, username, password, log)) {

            var userInfo = LocalUsers.getUserInfo(userDb, username);
            return AuthResult.AUTHORIZED(userInfo);
        }
        else {

            var loginFormPage = LoginContent.getLoginFormPage();
            return AuthResult.OTHER_RESPONSE(loginFormPage);
        }
    }
}
