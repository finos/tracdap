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

    private static final Map<String, byte[]> STATIC_CONTENT = preloadContent(STATIC_CONTENT_PATH);
    private static final Map<String, String> PAGE_CONTENT = stringValues(preloadContent(PAGE_CONTENT_PATH));

    private LoginContent() {}

    private static Map<String, byte[]> preloadContent(String resourceDir) {

        var resourceNames = ResourceHelpers.getResourcesNames(resourceDir, LoginContent.class);

        if (resourceNames == null)
            throw new EUnexpected();

        var resources = new HashMap<String, byte[]>();

        for (var resource : resourceNames) {

            var path = resourceDir + resource;
            var bytes = ResourceHelpers.loadResourceAsBytes(path, LoginContent.class);

            resources.put(resource, bytes);
        }

        return resources;
    }

    private static Map<String, String> stringValues(Map<String, byte[]> byteMap) {

        return byteMap.entrySet().stream()
                .map(kv -> Map.entry(kv.getKey(), new String(kv.getValue(), StandardCharsets.UTF_8)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    }

    public static CommonHttpResponse getStaticContent(HttpRequest request) {

        var uri = URI.create(request.uri());
        var path = uri.getPath();
        var fileKey = path.replace(LOGIN_PATH_PREFIX, "").toLowerCase();

        var content = STATIC_CONTENT.get(fileKey);

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

    public static CommonHttpResponse getLoginOkPage(String returnPath) {

        var templatePage = PAGE_CONTENT.get(LOGIN_OK_PAGE);
        var page = templatePage.replace(REDIRECT_VARIABLE, returnPath);

        return servePage(page);
    }

    public static CommonHttpResponse getLoginFormPage() {

        var page = PAGE_CONTENT.get(LOGIN_FORM_PAGE);

        return servePage(page);
    }

    private static CommonHttpResponse servePage(String page) {

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
