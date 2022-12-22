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

package org.finos.tracdap.common.auth.external;


import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;

import org.finos.tracdap.common.auth.internal.JwtValidator;
import org.finos.tracdap.common.auth.internal.SessionInfo;
import org.finos.tracdap.common.auth.internal.UserInfo;

import org.finos.tracdap.common.exception.EUnexpected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class AuthLogic<THeaders extends AuthHeaders> {

    private static final String TRAC_AUTH_TOKEN_HEADER = "trac_auth_token";
    private static final String TRAC_AUTH_SESSION_EXPIRY_HEADER = "trac_auth_session_expiry";
    private static final String TRAC_USER_ID_HEADER = "trac_user_idn";
    private static final String TRAC_USER_NAME_HEADER = "trac_user_name";

    private static final String BEARER_PREFIX = "bearer ";

    private static final boolean API_ROUTE = true;
    private static final boolean WEB_ROUTE = false;

    private static final List<String> TRAC_GRPC_FILTERED_HEADERS = List.of(
            TRAC_AUTH_TOKEN_HEADER,
            TRAC_AUTH_SESSION_EXPIRY_HEADER,
            TRAC_USER_ID_HEADER,
            TRAC_USER_NAME_HEADER,
            HttpHeaderNames.AUTHORIZATION.toString(),
            HttpHeaderNames.COOKIE.toString());

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JwtValidator jwt = null;


    public SessionInfo checkExistingAuth(AuthHeaders headers) {

//        var cookies = extractCookies(headers);
//
//        var token = findTokenInHeaders(headers);
//        var session = jwt.decodeAndValidate(token);

        return null;
    }

    public SessionInfo newSession(UserInfo userInfo) {
        return null;
    }

    public SessionInfo updateSession(SessionInfo sessionInfo) {
        return null;
    }

    public THeaders updateAuthHeaders(THeaders headers, SessionInfo session, RouteType routeType) {

        var filtered = removeAllAuthHeaders(headers);

        switch (routeType) {
            case BROWSER_ROUTE:
                return addHeadersForBrowser(filtered, session);
            case API_ROUTE:
                return addHeadersForApi(filtered, session);
            case PLATFORM_ROUTE:
                return addHeadersForPlatform(filtered, session);
            default:
                throw new EUnexpected();
        }
    }

    public THeaders removeAllAuthHeaders(THeaders headers) {

        var cookies = extractCookies(headers);

        var filteredHeaders = filterHeaders(headers);
        var filteredCookies = filterCookies(cookies);

        return null;
    }

    public THeaders addHeadersForPlatform(THeaders header, SessionInfo session) {
        return null;
    }

    public THeaders addHeadersForBrowser(THeaders header, SessionInfo session) {
        return null;
    }

    public THeaders addHeadersForApi(THeaders headers, SessionInfo session) {
        return null;
    }

    private String findTokenInHeaders(AuthHeaders headers, List<Cookie> cookies) {

        var tracAuthHeader = findHeader(headers, TRAC_AUTH_TOKEN_HEADER);
        if (tracAuthHeader != null) return tracAuthHeader;

        var tracAuthCookie = findCookie(cookies, TRAC_AUTH_TOKEN_HEADER);
        if (tracAuthCookie != null) return tracAuthCookie;

        var authorizationHeader = findHeader(headers, HttpHeaderNames.AUTHORIZATION.toString());
        if (authorizationHeader != null) return authorizationHeader;

        var authorizationCookie = findCookie(cookies, HttpHeaderNames.AUTHORIZATION.toString());
        if (authorizationCookie != null) return authorizationCookie;

        // Run out of places to look!

        return null;
    }

    private String findHeader(AuthHeaders headers, String headerName) {

        if (headers.contains(headerName))
            return headers.get(headerName).toString();

        return null;
    }

    private String findCookie(List<Cookie> cookies, String cookieName) {

        for (var cookie : cookies) {
            if (cookie.name().equals(cookieName)) {
                // TODO: Other fields in the cooke can make it specific, domain etc.
                return cookie.value();
            }
        }

        return null;
    }

    private List<Cookie> extractCookies(THeaders headers) {

        var cookieHeaders = headers.getAll(HttpHeaderNames.COOKIE);
        var cookies = new ArrayList<Cookie>();

        for (var header : cookieHeaders)
            cookies.addAll(ServerCookieDecoder.LAX.decodeAll(header.toString()));

        return cookies;
    }

    private THeaders filterHeaders(THeaders headers) {

        var filtered = headers; //  todo (THeaders) headers.getClass().getConstructor().newInstance();

        for (var header : headers) {

            var headerName = header.getKey().toString().toLowerCase();

            if (TRAC_GRPC_FILTERED_HEADERS.contains(headerName))
                continue;

            filtered.add(header.getKey(), header.getValue());
        }

        return filtered;
    }

    private List<Cookie> filterCookies(List<Cookie> cookies) {

        var filtered = new ArrayList<Cookie>();

        for (var cookie : cookies) {

            var cookieName = cookie.name().toLowerCase();

            if (TRAC_GRPC_FILTERED_HEADERS.contains(cookieName))
                continue;

            filtered.add(cookie);
        }

        return filtered;
    }
}
