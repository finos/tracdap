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

package org.finos.tracdap.common.auth.internal;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http2.Http2Headers;

import java.util.ArrayList;
import java.util.List;


public class HttpAuthHelpers {

    public static String findTracAuthToken(HttpHeaders headers) {

        var rawToken = findRawAuthToken(headers);

        if (rawToken == null)
            return null;

        // Remove the "bearer" prefix if the auth token header is stored that way

        if (rawToken.toLowerCase().startsWith(AuthConstants.BEARER_PREFIX))
            return rawToken.substring(AuthConstants.BEARER_PREFIX.length());
        else
            return rawToken;
    }

    public static String findTracAuthToken(Http2Headers headers) {

        var rawToken = findRawAuthToken(headers);

        if (rawToken == null)
            return null;

        // Remove the "bearer" prefix if the auth token header is stored that way

        if (rawToken.toLowerCase().startsWith(AuthConstants.BEARER_PREFIX))
            return rawToken.substring(AuthConstants.BEARER_PREFIX.length());
        else
            return rawToken;
    }

    public static boolean isBrowserRequest(HttpRequest request) {

        return ! isApiRequest(request);
    }

    public static boolean isApiRequest(HttpRequest request) {

        var headers = request.headers();

        if (!headers.contains(HttpHeaderNames.CONTENT_TYPE))
            return false;

        var contentType = headers.get(HttpHeaderNames.CONTENT_TYPE);

        return contentType.startsWith("application/") && !contentType.equals("application/x-www-form-urlencoded");
    }

    public static boolean isBrowserRequest(Http2Headers headers) {

        return ! isApiRequest(headers);
    }

    public static boolean isApiRequest(Http2Headers headers) {

        if (!headers.contains(HttpHeaderNames.CONTENT_TYPE))
            return false;

        var contentType = headers.get(HttpHeaderNames.CONTENT_TYPE).toString();

        return contentType.startsWith("application/") && !contentType.equals("application/x-www-form-urlencoded");
    }

    private static String findRawAuthToken(HttpHeaders headers) {

        var tracAuthHeader = headers.get(AuthConstants.TRAC_AUTH_TOKEN);

        if (tracAuthHeader != null)
            return tracAuthHeader;

        var authorizationHeader = headers.get(HttpHeaderNames.AUTHORIZATION.toString());

        if (authorizationHeader != null)
            return authorizationHeader;

        var cookies = extractCookies(headers);

        var tracAuthCookie = findCookie(cookies, AuthConstants.TRAC_AUTH_TOKEN);

        if (tracAuthCookie != null)
            return tracAuthCookie;

        return findCookie(cookies, HttpHeaderNames.AUTHORIZATION.toString());
    }

    private static String findRawAuthToken(Http2Headers headers) {

        var tracAuthHeader = headers.get(AuthConstants.TRAC_AUTH_TOKEN);

        if (tracAuthHeader != null)
            return tracAuthHeader.toString();

        var authorizationHeader = headers.get(HttpHeaderNames.AUTHORIZATION.toString());

        if (authorizationHeader != null)
            return authorizationHeader.toString();

        var cookies = extractCookies(headers);

        var tracAuthCookie = findCookie(cookies, AuthConstants.TRAC_AUTH_TOKEN);

        if (tracAuthCookie != null)
            return tracAuthCookie;

        return findCookie(cookies, HttpHeaderNames.AUTHORIZATION.toString());
    }

    private static List<Cookie> extractCookies(HttpHeaders headers) {

        var cookieHeaders = headers.getAll(HttpHeaderNames.COOKIE);
        var cookies = new ArrayList<Cookie>();

        for (var header : cookieHeaders)
            cookies.addAll(ServerCookieDecoder.LAX.decodeAll(header));

        return cookies;
    }

    private static List<Cookie> extractCookies(Http2Headers headers) {

        var cookieHeaders = headers.getAll(HttpHeaderNames.COOKIE);
        var cookies = new ArrayList<Cookie>();

        for (var header : cookieHeaders)
            cookies.addAll(ServerCookieDecoder.LAX.decodeAll(header.toString()));

        return cookies;
    }

    private static String findCookie(List<Cookie> cookies, String cookieName) {

        for (var cookie : cookies) {
            if (cookie.name().equals(cookieName)) {
                return cookie.value();
            }
        }

        return null;
    }
}
