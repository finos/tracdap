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

package org.finos.tracdap.auth.login.simple;

import org.finos.tracdap.auth.login.*;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.http.Http1Headers;
import org.finos.tracdap.common.http.CommonHttpRequest;
import org.finos.tracdap.common.http.CommonHttpResponse;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;


class BasicLoginProvider implements ILoginProvider {

    private static final Logger log = LoggerFactory.getLogger(BasicLoginProvider.class);

    private static final String BASIC_AUTH_HEADER = "Basic realm=\"%s\", charset=\"%s\"";
    private static final String BASIC_AUTH_PREFIX = "basic ";

    private static final String BASIC_AUTH_REALM = "trac-auth-realm";
    private static final String BASIC_AUTH_CHARSET = "UTF-8";

    private final String authenticateHeader;
    private final IUserDatabase userDb;

    public BasicLoginProvider(ConfigManager configManager) {

        this.authenticateHeader = String.format(BASIC_AUTH_HEADER, BASIC_AUTH_REALM, BASIC_AUTH_CHARSET);
        this.userDb = SimpleLoginPlugin.createUserDb(configManager);
    }

    @Override
    public LoginResult attemptLogin(CommonHttpRequest authRequest) {

        var headers = authRequest.headers();

        if (!headers.contains(HttpHeaderNames.AUTHORIZATION)) {
            log.info("No authorization provided, new authorization required");
            return requestAuth();
        }

        var authHeader = headers.get(HttpHeaderNames.AUTHORIZATION).toString();
        var prefixEnd = Math.min(BASIC_AUTH_PREFIX.length(), authHeader.length());
        var prefix = authHeader.substring(0, prefixEnd);

        // If the authorization header is not understood, trigger a new auth workflow
        if (!prefix.equalsIgnoreCase(BASIC_AUTH_PREFIX)) {
            log.warn("Invalid authorization header, re-authorization required");
            return requestAuth();
        }

        var basicAuthData = authHeader.substring(BASIC_AUTH_PREFIX.length());
        var decodedData = Base64.getDecoder().decode(basicAuthData);
        var userAndPass = new String(decodedData, StandardCharsets.UTF_8);
        var separator = userAndPass.indexOf(':');

        // Separator must be found and cannot be at position zero (i.e. no empty usernames)
        if (separator < 1) {
            log.warn("Invalid authorization header, re-authorization required");
            return requestAuth();
        }

        var username = userAndPass.substring(0, separator);
        var password = userAndPass.substring(separator + 1);

        if (LocalUsers.checkPassword(userDb, username, password, log)) {
            var userInfo = LocalUsers.getUserInfo(userDb, username);
            return LoginResult.AUTHORIZED(userInfo);
        }
        else {
            return requestAuth();
        }
    }

    public LoginResult requestAuth() {

        log.info("AUTHENTICATION: Using basic authentication");

        var headers = new Http1Headers();
        headers.add(HttpHeaderNames.WWW_AUTHENTICATE, authenticateHeader);

        var response = new CommonHttpResponse(
                HttpResponseStatus.UNAUTHORIZED,
                headers, Unpooled.EMPTY_BUFFER);

        return LoginResult.OTHER_RESPONSE(response);
    }
}
