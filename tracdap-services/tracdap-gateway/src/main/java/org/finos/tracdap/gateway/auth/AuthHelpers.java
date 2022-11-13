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

package org.finos.tracdap.gateway.auth;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http2.Http2Headers;

import java.util.List;
import java.util.stream.Collectors;


public class AuthHelpers {

    private static final String BEARER_AUTH_PREFIX = "bearer ";

    public static String getAuthToken(HttpHeaders headers) {

        if (headers.contains(HttpHeaderNames.AUTHORIZATION)) {

            var authHeader = headers.get(HttpHeaderNames.AUTHORIZATION);

            if (authHeader.startsWith(BEARER_AUTH_PREFIX))
                return authHeader.substring(BEARER_AUTH_PREFIX.length());
        }

        if (headers.contains(HttpHeaderNames.COOKIE)) {

            var cookieData = headers.getAll(HttpHeaderNames.COOKIE);

            var cookies = cookieData.stream()
                    .map(ServerCookieDecoder.STRICT::decodeAll)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());

            var authCookie = cookies.stream()
                    .filter(c -> HttpHeaderNames.AUTHORIZATION.toString().equals(c.name().toLowerCase()))
                    .findFirst();

            if (authCookie.isPresent()) {
                return authCookie.get().value();
            }
        }

        return null;
    }

    public static String getAuthToken(Http2Headers headers) {

        if (headers.contains(HttpHeaderNames.AUTHORIZATION)) {

            var authHeader = headers.get(HttpHeaderNames.AUTHORIZATION).toString();

            if (authHeader.startsWith(BEARER_AUTH_PREFIX))
                return authHeader.substring(BEARER_AUTH_PREFIX.length());
        }

        if (headers.contains(HttpHeaderNames.COOKIE)) {

            var cookieData = headers.getAll(HttpHeaderNames.COOKIE);

            var cookies = cookieData.stream()
                    .map(CharSequence::toString)
                    .map(ServerCookieDecoder.STRICT::decodeAll)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());

            var authCookie = cookies.stream()
                    .filter(c -> HttpHeaderNames.AUTHORIZATION.toString().equals(c.name().toLowerCase()))
                    .findFirst();

            if (authCookie.isPresent()) {
                return authCookie.get().value();
            }
        }

        return null;
    }
}
