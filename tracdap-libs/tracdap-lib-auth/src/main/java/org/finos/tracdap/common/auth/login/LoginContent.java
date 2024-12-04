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

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.util.ResourceHelpers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


public final class LoginContent {

    private static final String LOGIN_PATH = "/login/";
    private static final String STATIC_CONTENT_PATH = "/login/static/";
    private static final String PAGE_CONTENT_PATH = "/login/pages/";
    private static final String LOGIN_OK_PAGE = "login_ok.html";
    private static final String LOGIN_FORM_PAGE = "login_form.html";
    private static final String REDIRECT_VARIABLE = "${REDIRECT}";

    private static final Map<String, byte[]> STATIC_CONTENT = preloadContent(STATIC_CONTENT_PATH);
    private static final Map<String, String> PAGE_CONTENT = stringValues(preloadContent(PAGE_CONTENT_PATH));

    boolean exists;
    ByteBuf content;
    AuthHeaders headers;

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

    public static LoginContent getStaticContent(HttpRequest request) {

        var uri = URI.create(request.uri());
        var path = uri.getPath();
        var fileKey = path.replace(LOGIN_PATH, "").toLowerCase();

        var content = STATIC_CONTENT.get(fileKey);
        var response = new LoginContent();

        if (content != null) {

            var mimeType = mimeTypeMapping(fileKey);
            var length = content.length;

            response.exists = true;
            response.content = Unpooled.wrappedBuffer(content, 0, length);
            response.headers = new Http1AuthHeaders();
            response.headers.add(HttpHeaderNames.CONTENT_TYPE, mimeType);
            response.headers.add(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(length));
        }
        else {

            response.exists = false;
        }

        return response;
    }

    public static LoginContent getLoginOkPage(String returnPath) {

        System.out.println(returnPath);

        var templatePage = PAGE_CONTENT.get(LOGIN_OK_PAGE);
        var page = templatePage.replace(REDIRECT_VARIABLE, returnPath);

        return servePage(page);
    }

    public static LoginContent getLoginFormPage() {

        var page = PAGE_CONTENT.get(LOGIN_FORM_PAGE);

        return servePage(page);
    }

    private static LoginContent servePage(String page) {

        var content = page.getBytes(StandardCharsets.UTF_8);

        var response = new LoginContent();
        response.exists = true;
        response.content = Unpooled.wrappedBuffer(content, 0, content.length);
        response.headers = new Http1AuthHeaders();
        response.headers.add(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=utf-8");
        response.headers.add(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(content.length));

        return response;
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
