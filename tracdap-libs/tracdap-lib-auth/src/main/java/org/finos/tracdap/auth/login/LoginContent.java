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

package org.finos.tracdap.auth.login;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.http.Http1Headers;
import org.finos.tracdap.common.http.CommonHttpResponse;
import org.finos.tracdap.common.util.ResourceHelpers;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


public final class LoginContent {

    public static final String LOGIN_PATH_PREFIX = "/login/";
    public static final String LOGIN_URL = "/login/browser";
    public static final String REFRESH_URL = "/login/refresh";

    public static final HttpResponseStatus LOGIN_REDIRECT_STATUS = HttpResponseStatus.valueOf(
            HttpResponseStatus.TEMPORARY_REDIRECT.code(), "Login redirect");

    private static final String STATIC_CONTENT_PATH = "/login/static/";
    private static final String PAGE_CONTENT_PATH = "/login/pages/";
    private static final String LOGIN_OK_PAGE = "login_ok.html";
    private static final String LOGIN_FORM_PAGE = "login_form.html";
    private static final String REDIRECT_VARIABLE = "${REDIRECT}";

    private final String pathPrefix;
    private final Map<String, byte[]> staticContent;
    private final Map<String, String> pageContent;

    public LoginContent() {
        this(LOGIN_PATH_PREFIX, STATIC_CONTENT_PATH, PAGE_CONTENT_PATH, LoginContent.class);
    }

    public LoginContent(String pathPrefix, String staticDir, String pageDir, Class<?> resourceOwner) {

        this.pathPrefix = pathPrefix;
        this.staticContent = preloadContent(staticDir, resourceOwner);
        this.pageContent = stringValues(preloadContent(pageDir, resourceOwner));
    }

    private Map<String, byte[]> preloadContent(String resourceDir, Class<?> resourceOwner) {

        var resourceNames = ResourceHelpers.getResourcesNames(resourceDir, resourceOwner);

        if (resourceNames == null)
            throw new EUnexpected();

        var resources = new HashMap<String, byte[]>();

        for (var resource : resourceNames) {

            var path = resourceDir + resource;
            var bytes = ResourceHelpers.loadResourceAsBytes(path, resourceOwner);

            resources.put(resource, bytes);
        }

        return resources;
    }

    private Map<String, String> stringValues(Map<String, byte[]> byteMap) {

        return byteMap.entrySet().stream()
                .map(kv -> Map.entry(kv.getKey(), new String(kv.getValue(), StandardCharsets.UTF_8)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    }

    public CommonHttpResponse getStaticContent(HttpRequest request) {

        var uri = URI.create(request.uri());
        var path = uri.getPath();
        var fileKey = path.replace(pathPrefix, "").toLowerCase();

        var content = staticContent.get(fileKey);

        if (content != null) {

            var buffer = Unpooled.wrappedBuffer(content, 0, content.length);
            var mimeType = mimeTypeMapping(fileKey);
            var length = content.length;
            var headers = new Http1Headers();
            headers.add(HttpHeaderNames.CONTENT_TYPE, mimeType);
            headers.add(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(length));

            return new CommonHttpResponse(HttpResponseStatus.OK, headers, buffer);
        }
        else {

            var buffer = Unpooled.EMPTY_BUFFER;
            var headers = new Http1Headers();

            return new CommonHttpResponse(HttpResponseStatus.NOT_FOUND, headers, buffer);
        }
    }

    public CommonHttpResponse getLoginOkPage(String returnPath) {

        var substitutions = Map.of(REDIRECT_VARIABLE, returnPath);
        return servePage(LOGIN_OK_PAGE, substitutions);
    }

    public CommonHttpResponse getLoginFormPage() {

        return servePage(LOGIN_FORM_PAGE);
    }

    public CommonHttpResponse servePage(String pageName) {

        var page = pageContent.get(pageName);
        return buildPageResponse(page);
    }

    public CommonHttpResponse servePage(String pageName, Map<String, String> substitutions) {

        var templatePage = pageContent.get(pageName);

        for (var substitution : substitutions.entrySet())
            templatePage = templatePage.replace(substitution.getKey(), substitution.getValue());

        return buildPageResponse(templatePage);
    }

    private CommonHttpResponse buildPageResponse(String page) {

        var content = page.getBytes(StandardCharsets.UTF_8);
        var buffer = Unpooled.wrappedBuffer(content, 0, content.length);
        var headers = new Http1Headers();
        headers.add(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=utf-8");
        headers.add(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(content.length));

        return new CommonHttpResponse(HttpResponseStatus.OK, headers, buffer);
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
