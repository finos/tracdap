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

import org.finos.tracdap.common.auth.login.*;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;

import io.netty.handler.codec.http.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;


class BuiltInLoginProvider implements ILoginProvider {

    public static final String MAIN_PAGE_KEY = "mainPage";

    public static final String BUILT_IN_AUTH_ROOT = "/trac-auth/";
    public static final String BUILT_IN_AUTH_PAGE = "/trac-auth/login";


    private static final Logger log = LoggerFactory.getLogger(BuiltInLoginProvider.class);

    private final String mainPage;
    private final IUserDatabase userDb;

    public BuiltInLoginProvider(Properties properties, ConfigManager configManager) {

        if (!properties.containsKey(MAIN_PAGE_KEY)) {

            var messageTemplate = "The [BUILTIN] auth provider is missing required config property [%s]";
            var message = String.format(messageTemplate, MAIN_PAGE_KEY);
            log.error(message);
            throw new EStartup(message);
        }

        mainPage = properties.getProperty(MAIN_PAGE_KEY);

        this.userDb = SimpleLoginPlugin.createUserDb(configManager);
    }

    @Override
    public AuthResult attemptLogin(AuthRequest request) {

        // API auth by redirect does not work nicely out of the box!
        // For now send an auth failure on API routes
        // Anyway for browser re-directs there should be some control in the client layer
        // Perhaps this can be done in the web bindings package, with suitable config options available?

        var headers = request.getHeaders();
        var isApi =
                headers.contains(HttpHeaderNames.CONTENT_TYPE) &&
                headers.get(HttpHeaderNames.CONTENT_TYPE).toString().startsWith("application/") &&
                !headers.get(HttpHeaderNames.CONTENT_TYPE).equals("application/x-www-form-urlencoded");

        if (isApi)
            return AuthResult.FAILED("Session expired or not available");

        if (!request.getUrl().startsWith(BUILT_IN_AUTH_ROOT))
            return LoginContent.redirectToLogin(request);

        if (request.getMethod().equals(HttpMethod.POST.name()) &&
            request.getUrl().equals(BUILT_IN_AUTH_PAGE)) {

            if (request.getContent() == null)
                return AuthResult.NEED_CONTENT();

            return checkLoginRequest(request);
        }
        else {

            return LoginContent.serveLoginContent(request, false);
        }
    }

    @Override
    public boolean postLoginmatch(String method, String uri) {

        return uri.startsWith(BUILT_IN_AUTH_ROOT);
    }

    @Override
    public AuthResponse postLogin(AuthRequest request, UserInfo userInfo) {

        if (request.getUrl().startsWith(BUILT_IN_AUTH_ROOT)) {

            return LoginContent.serveLoginContent(request, true).getOtherResponse();
        }
        else {

            return null;
        }
    }

    private AuthResult checkLoginRequest(AuthRequest request) {

        var content = new String(request.getContent(), StandardCharsets.US_ASCII);
        var decoder = new QueryStringDecoder(BUILT_IN_AUTH_PAGE + "?" + content);
        var loginParams = decoder.parameters();

        var usernameParam = loginParams.get("username");
        var passwordParam = loginParams.get("password");

        if (usernameParam == null || usernameParam.size() != 1 ||
                passwordParam == null || passwordParam.size() != 1) {
            return LoginContent.redirectToLogin(request);
        }

        var username = usernameParam.get(0);
        var password = passwordParam.get(0);

        if (LocalUsers.checkPassword(userDb, username, password, log)) {
            var userInfo = LocalUsers.getUserInfo(userDb, username);
            return AuthResult.AUTHORIZED(userInfo);
        }
        else {
            return LoginContent.redirectToLogin(request);
        }
    }
}
