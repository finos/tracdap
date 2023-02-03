/*
 * Copyright 2023 Accenture Global Solutions Limited
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
import org.finos.tracdap.common.config.ISecretLoader;
import org.finos.tracdap.common.exception.EResourceNotFound;
import org.finos.tracdap.common.util.ResourceHelpers;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;


public class BuiltInAuthProvider implements IAuthProvider {

    public static final String BUILT_IN_AUTH_ROOT = "/trac-auth/";
    public static final String BUILT_IN_AUTH_PAGE = "/trac-auth/login";

    public static final String BUILT_IN_CONTENT_PATH = "/builtin/content/";
    public static final String BUILT_IN_LOGIN_PAGE = "/builtin/content/login.html";
    public static final String BUILT_IN_LOGIN_OK_PAGE = "/builtin/content/login_ok.html";


    private static final Logger log = LoggerFactory.getLogger(BuiltInAuthProvider.class);

    private final ISecretLoader userDb;

    public BuiltInAuthProvider(ConfigManager configManager) {

        this.userDb = configManager.getUserDb();
    }

    @Override
    public AuthResult attemptAuth(AuthRequest request) {

        if (!request.getUrl().startsWith(BUILT_IN_AUTH_ROOT))
            return redirectToLogin(request);

        if (request.getMethod().equals(HttpMethod.POST.name()) &&
            request.getUrl().equals(BUILT_IN_AUTH_PAGE)) {

            if (request.getContent() == null)
                return AuthResult.NEED_CONTENT();

            return checkLoginRequest(request);
        }
        else {

            return serveLoginContent(request, false);
        }
    }

    @Override
    public boolean postAuthMatch(String method, String uri) {

        return uri.startsWith(BUILT_IN_AUTH_ROOT);
    }

    @Override
    public AuthResponse postAuth(AuthRequest request, UserInfo userInfo) {

        if (request.getUrl().startsWith(BUILT_IN_AUTH_ROOT)) {

            return serveLoginContent(request, true).getOtherResponse();
        }
        else {

            return null;
        }
    }

    private AuthResult redirectToLogin(AuthRequest request) {

        log.info("AUTHENTICATION: Using built-in authentication");

        if (request.getUrl().equals(BUILT_IN_AUTH_PAGE))
            return serveLoginContent(request, false);

        else {

            var headers = new Http1AuthHeaders();
            headers.add(HttpHeaderNames.LOCATION, BUILT_IN_AUTH_PAGE);

            var response = new AuthResponse(
                    HttpResponseStatus.TEMPORARY_REDIRECT.code(), "Login redirect",
                    headers, Unpooled.EMPTY_BUFFER);

            return AuthResult.OTHER_RESPONSE(response);
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
            return redirectToLogin(request);
        }

        var username = usernameParam.get(0);
        var password = passwordParam.get(0);

        if (!LocalUsers.checkPassword(userDb, username, password, log)) {
            return redirectToLogin(request);
        }
        else {
            var userInfo = LocalUsers.getUserInfo(userDb, username);
            return AuthResult.AUTHORIZED(userInfo);
        }
    }

    private AuthResult serveLoginContent(AuthRequest request, boolean loggedIn) {

        try {
            var uri = URI.create(request.getUrl());

            var resourcePath = uri.getPath().replace(BUILT_IN_AUTH_ROOT, BUILT_IN_CONTENT_PATH);

            if (uri.getPath().equals(BUILT_IN_AUTH_PAGE))
                resourcePath = loggedIn ? BUILT_IN_LOGIN_OK_PAGE : BUILT_IN_LOGIN_PAGE;

            var resourceBytes = ResourceHelpers.loadResourceAsBytes(resourcePath, getClass());

            if (loggedIn && resourcePath.equals(BUILT_IN_LOGIN_OK_PAGE))
                resourceBytes = insertRedirect(resourceBytes);

            var resourceContent = Unpooled.wrappedBuffer(resourceBytes);
            var resourceType = mimeTypeMapping(resourcePath);

            var headers = new Http1AuthHeaders();
            headers.add(HttpHeaderNames.CONTENT_TYPE, resourceType);
            headers.add(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(resourceContent.readableBytes()));

            var response = new AuthResponse(
                    HttpResponseStatus.OK.code(),
                    HttpResponseStatus.OK.reasonPhrase(),
                    new Http1AuthHeaders(),
                    resourceContent);

            return AuthResult.OTHER_RESPONSE(response);
        }
        catch (IllegalArgumentException | EResourceNotFound e) {
            return redirectToLogin(request);
        }
    }

    private byte[] insertRedirect(byte[] pageBytes) {

        var pageText = new String(pageBytes, StandardCharsets.UTF_8);
        pageText = pageText.replace("${REDIRECT}", "/trac-ui/app");
        return pageText.getBytes(StandardCharsets.UTF_8);
    }

    private String mimeTypeMapping(String path) {

        var sep = path.lastIndexOf(".");
        var ext = path.substring(sep + 1);

        if (ext.equals("html"))
            return "text/html";

        if (ext.equals("css"))
            return "text/css";

        if (ext.equals("png"))
            return "image/png";

        return "text/html";

    }
}
