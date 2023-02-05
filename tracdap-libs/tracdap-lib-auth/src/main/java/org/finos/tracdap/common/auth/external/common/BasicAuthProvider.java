/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.auth.external.common;

import org.finos.tracdap.common.auth.external.*;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.config.ConfigManager;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;


public class BasicAuthProvider implements IAuthProvider {

    private static final Logger log = LoggerFactory.getLogger(BasicAuthProvider.class);

    private static final String BASIC_AUTH_PREFIX = "basic ";

    private final IUserDatabase userDb;

    public BasicAuthProvider(ConfigManager configManager) {

        this.userDb = CommonAuthPlugin.createUserDb(configManager);
    }

    @Override
    public AuthResult attemptAuth(AuthRequest authRequest) {

        var headers = authRequest.getHeaders();

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
            return AuthResult.AUTHORIZED(userInfo);
        }
        else {
            return requestAuth();
        }
    }

    @Override
    public boolean postAuthMatch(String method, String uri) {
        return false;
    }

    @Override
    public AuthResponse postAuth(AuthRequest authRequest, UserInfo userInfo) {
        return null;
    }

    public AuthResult requestAuth() {

        log.info("AUTHENTICATION: Using basic authentication");

        var headers = new Http1AuthHeaders();
        headers.add(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"trac_auth_realm\", charset=\"UTF-8\"");

        var response = new AuthResponse(
                HttpResponseStatus.UNAUTHORIZED.code(),
                HttpResponseStatus.UNAUTHORIZED.reasonPhrase(),
                headers, Unpooled.EMPTY_BUFFER);

        return AuthResult.OTHER_RESPONSE(response);
    }


}
