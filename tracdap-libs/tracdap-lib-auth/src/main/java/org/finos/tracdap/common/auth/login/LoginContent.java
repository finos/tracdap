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

package org.finos.tracdap.common.auth.login;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.finos.tracdap.common.util.ResourceHelpers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.MissingResourceException;


public class LoginContent {

    public static final String MAIN_PAGE_KEY = "mainPage";

    public static final String BUILT_IN_AUTH_ROOT = "/trac-auth/";
    public static final String BUILT_IN_AUTH_PAGE = "/trac-auth/login";

    public static final String BUILT_IN_CONTENT_PATH = "/builtin/content/";
    public static final String BUILT_IN_LOGIN_PAGE = "/builtin/content/login.html";
    public static final String BUILT_IN_LOGIN_OK_PAGE = "/builtin/content/login_ok.html";

    private static final String mainPage = "/trac-ui/app/";

    // TODO: Review these constants, some should come from config

    public static AuthResult serveLoginContent(AuthRequest request, boolean loggedIn) {

        try {
            var uri = URI.create(request.getUrl());

            var resourcePath = uri.getPath().replace(BUILT_IN_AUTH_ROOT, BUILT_IN_CONTENT_PATH);

            if (uri.getPath().equals(BUILT_IN_AUTH_PAGE))
                resourcePath = loggedIn ? BUILT_IN_LOGIN_OK_PAGE : BUILT_IN_LOGIN_PAGE;

            var resourceBytes = ResourceHelpers.loadResourceAsBytes(resourcePath, LoginContent.class);

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
        catch (IllegalArgumentException | MissingResourceException e) {
            return redirectToLogin(request);
        }
    }

    public static AuthResult redirectToLogin(AuthRequest request) {

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

    private static byte[] insertRedirect(byte[] pageBytes) {

        var pageText = new String(pageBytes, StandardCharsets.UTF_8);
        pageText = pageText.replace("${REDIRECT}", mainPage);
        return pageText.getBytes(StandardCharsets.UTF_8);
    }

    private static String mimeTypeMapping(String path) {

        var sep = path.lastIndexOf(".");
        var ext = path.substring(sep + 1);

        switch (ext) {
        case "html":
            return "text/html";
        case "css":
            return "text/css";
        case "png":
            return "image/png";
        }

        return "text/html";
    }
}
