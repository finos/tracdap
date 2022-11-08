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

package org.finos.tracdap.gateway.auth.basic;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.finos.tracdap.common.auth.JwtHelpers;
import org.finos.tracdap.common.auth.UserInfo;
import org.finos.tracdap.gateway.auth.AuthProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Base64;

public class BasicAuthProvider implements AuthProvider {

    private static final Logger log = LoggerFactory.getLogger(BasicAuthProvider.class);

    private static final String BASIC_AUTH_PREFIX = "basic ";

    // TODO: Single copy of this taken from config
    private static final Duration TOKEN_EXPIRY = Duration.of(6, ChronoUnit.HOURS);

    @Override
    public String newAuth(ChannelHandlerContext ctx, HttpRequest req) {

        log.info("AUTHENTICATION: Using basic authentication");

        var headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"trac_auth_realm\", charset=\"UTF-8\"");

        var response = new DefaultHttpResponse(
                req.protocolVersion(),
                HttpResponseStatus.UNAUTHORIZED,
                headers);

        ctx.writeAndFlush(response);
        ctx.close();

        return null;
    }

    @Override
    public String translateAuth(ChannelHandlerContext ctx, HttpRequest req, String authInfo) {

        var prefixEnd = Math.min(BASIC_AUTH_PREFIX.length(), authInfo.length());
        var prefix = authInfo.substring(0, prefixEnd);

        // If the authorization header is not understood, trigger a new auth workflow
        if (!prefix.equalsIgnoreCase(BASIC_AUTH_PREFIX)) {
            log.warn("Invalid authorization header, re-authorization required");
            return newAuth(ctx, req);
        }

        var basicAuthData = authInfo.substring(BASIC_AUTH_PREFIX.length());
        var decodedData = Base64.getDecoder().decode(basicAuthData);
        var userAndPass = new String(decodedData, StandardCharsets.UTF_8);
        var separator = userAndPass.indexOf(':');

        // Separator must be found and cannot be at position zero (i.e. no empty usernames)
        if (separator < 1) {
            log.warn("Invalid authorization header, re-authorization required");
            return newAuth(ctx, req);
        }

        var user = userAndPass.substring(0, separator);
        var pass = userAndPass.substring(separator + 1);

        if (!checkPassword(user, pass)) {
            return newAuth(ctx, req);
        }

        var userInfo = getUserInfo(user);

        return JwtHelpers.encodeToken(userInfo, TOKEN_EXPIRY);
    }

    private boolean checkPassword(String user, String pass) {

        // TODO: Real auth using a local source

        log.info("AUTHENTICATION: Succeeded [{}]", user);
        return true;
    }

    private UserInfo getUserInfo(String user) {

        // TODO: Real info using a local source

        var userInfo = new UserInfo();
        userInfo.setUserId(user);
        userInfo.setDisplayName("The user called " + user);

        return userInfo;
    }
}
