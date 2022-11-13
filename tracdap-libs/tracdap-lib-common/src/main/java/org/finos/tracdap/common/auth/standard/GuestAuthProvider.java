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

package org.finos.tracdap.common.auth.standard;

import org.finos.tracdap.common.auth.UserInfo;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.auth.IAuthProvider;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class GuestAuthProvider implements IAuthProvider {

    private static final Logger log = LoggerFactory.getLogger(GuestAuthProvider.class);

    public static final String USER_ID_CONFIG_KEY = "userId";
    public static final String USER_NAME_CONFIG_KEY = "userName";

    private final String guestId;
    private final String guestName;

    GuestAuthProvider(Properties properties) {

        if (!properties.containsKey(USER_ID_CONFIG_KEY) || !properties.containsKey(USER_NAME_CONFIG_KEY)) {

            var messageTemplate = "The [GUEST] auth provider is missing required config properties [%s] and [%s]";
            var message = String.format(messageTemplate, USER_ID_CONFIG_KEY, USER_NAME_CONFIG_KEY);
            log.error(message);
            throw new EStartup(message);
        }

        guestId = properties.getProperty(USER_ID_CONFIG_KEY);
        guestName = properties.getProperty(USER_NAME_CONFIG_KEY);
    }

    @Override
    public UserInfo newAuth(ChannelHandlerContext ctx, HttpRequest req) {

        log.info("AUTHENTICATION: Using guest authentication [{}]", guestId);

        var user = new UserInfo();
        user.setUserId(guestId);
        user.setDisplayName(guestName);

        return user;
    }

    @Override
    public UserInfo translateAuth(ChannelHandlerContext ctx, HttpRequest req, String authInfo) {
        return null;
    }
}
