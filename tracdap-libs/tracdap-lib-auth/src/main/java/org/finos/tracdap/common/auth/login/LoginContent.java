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
import org.finos.tracdap.config.AuthenticationConfig;

import javax.annotation.Nonnull;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.MissingResourceException;


public class LoginContent {

    public static final String DEFAULT_LOGIN_PATH = "/login";
    public static final String DEFAULT_RETURN_PATH = "/";

    public static final String DEFAULT_LOGIN_MAIN_PAGE = "/login/browser/login";

    public static final String BUILT_IN_CONTENT_PATH = "/login/content/";
    public static final String BUILT_IN_LOGIN_PAGE = "/login/content/login.html";
    public static final String BUILT_IN_LOGIN_OK_PAGE = "/login/content/login_ok.html";

    private final String configLoginPath;
    private final String configReturnPath;

    public LoginContent(AuthenticationConfig authConfig) {

        if (authConfig.hasLoginPath())
            configLoginPath = authConfig.getLoginPath();
        else
            configLoginPath = DEFAULT_LOGIN_PATH;

        if (authConfig.hasReturnPath())
            configReturnPath = authConfig.getReturnPath();
        else
            configReturnPath = DEFAULT_RETURN_PATH;
    }

    @Nonnull
    public AuthResponse serveLoginContent(AuthRequest request, boolean loggedIn) {

        try {
            var uri = URI.create(request.getUrl());
            var query = uri.getQuery();
            var queryParams = query != null ? Arrays.asList(query.split("&")) : List.<String>of();

            var resourcePath = uri.getPath().replace(configLoginPath, BUILT_IN_CONTENT_PATH);

            if (uri.getPath().equals(DEFAULT_LOGIN_MAIN_PAGE))
                resourcePath = loggedIn ? BUILT_IN_LOGIN_OK_PAGE : BUILT_IN_LOGIN_PAGE;

            var resourceBytes = ResourceHelpers.loadResourceAsBytes(resourcePath, LoginContent.class);

            if (loggedIn && resourcePath.equals(BUILT_IN_LOGIN_OK_PAGE)) {

                var returnPathParam = queryParams.stream()
                        .filter(p -> p.startsWith("return-path"))
                        .findFirst();

                if (returnPathParam.isPresent()) {
                    var returnPath = returnPathParam.get().substring(returnPathParam.get().indexOf('=') + 1);
                    resourceBytes = insertRedirect(resourceBytes, returnPath);
                }
                else {
                    resourceBytes = insertRedirect(resourceBytes, configReturnPath);
                }
            }

            var resourceContent = Unpooled.wrappedBuffer(resourceBytes);
            var resourceType = mimeTypeMapping(resourcePath);

            var headers = new Http1AuthHeaders();
            headers.add(HttpHeaderNames.CONTENT_TYPE, resourceType);
            headers.add(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(resourceContent.readableBytes()));

            return new AuthResponse(
                    HttpResponseStatus.OK.code(),
                    HttpResponseStatus.OK.reasonPhrase(),
                    new Http1AuthHeaders(),
                    resourceContent);
        }
        catch (IllegalArgumentException | MissingResourceException e) {
            return redirectToLogin(request);
        }
    }

    @Nonnull
    public AuthResponse redirectToLogin(AuthRequest request) {

        if (request.getUrl().equals(DEFAULT_LOGIN_MAIN_PAGE))
            return serveLoginContent(request, false);

        else {

            var headers = new Http1AuthHeaders();
            headers.add(HttpHeaderNames.LOCATION, DEFAULT_LOGIN_MAIN_PAGE);

            return new AuthResponse(
                    HttpResponseStatus.TEMPORARY_REDIRECT.code(), "Login redirect",
                    headers, Unpooled.EMPTY_BUFFER);
        }
    }

    private static byte[] insertRedirect(byte[] pageBytes, String returnPath) {

        var pageText = new String(pageBytes, StandardCharsets.UTF_8);
        pageText = pageText.replace("${REDIRECT}", returnPath);
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
