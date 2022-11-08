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

package org.finos.tracdap.gateway.auth.guest;

import org.finos.tracdap.common.auth.JwtHelpers;
import org.finos.tracdap.common.auth.UserInfo;
import org.finos.tracdap.gateway.auth.AuthProvider;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;


public class GuestAuthProvider implements AuthProvider {

    private static final Logger log = LoggerFactory.getLogger(GuestAuthProvider.class);

    // TODO: ID and name from config
    private static final String GUEST_ID = "guest";
    private static final String GUEST_NAME = "Guest User";

    // TODO: Single copy of this taken from config
    private static final Duration TOKEN_EXPIRY = Duration.of(6, ChronoUnit.HOURS);

    @Override
    public String newAuth(ChannelHandlerContext ctx, HttpRequest req) {

        log.info("AUTHENTICATION: Using guest authentication [{}]", GUEST_ID);

        var user = new UserInfo();
        user.setUserId(GUEST_ID);
        user.setDisplayName(GUEST_NAME);

        return JwtHelpers.encodeToken(user, TOKEN_EXPIRY);
    }

    @Override
    public String translateAuth(ChannelHandlerContext ctx, HttpRequest req, String authInfo) {
        return null;
    }
}
