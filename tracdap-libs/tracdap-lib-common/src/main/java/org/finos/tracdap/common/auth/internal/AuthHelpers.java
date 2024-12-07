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

import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cookie.*;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


public class AuthHelpers {

    public static final boolean CLIENT_COOKIE = true;
    public static final boolean SERVER_COOKIE = false;

    private static final String TRAC_AUTH_PREFIX = "trac-auth-";
    private static final String TRAC_USER_PREFIX = "trac-user-";

    private static final List<String> RESTRICTED_HEADERS = List.of(
            HttpHeaderNames.AUTHORIZATION.toString(),
            HttpHeaderNames.COOKIE.toString(),
            HttpHeaderNames.SET_COOKIE.toString());

    public static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    String findTracAuthToken(THeaders headers, boolean cookieDirection) {

        var rawToken = findRawAuthToken(headers, cookieDirection);

        if (rawToken == null)
            return null;

        // Remove the "bearer" prefix if the auth token header is stored that way

        if (rawToken.toLowerCase().startsWith(AuthConstants.BEARER_PREFIX))
            return rawToken.substring(AuthConstants.BEARER_PREFIX.length());
        else
            return rawToken;
    }

    private static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    String findRawAuthToken(THeaders headers, boolean cookieDirection) {

        var authorizationHeader = headers.get(HttpHeaderNames.AUTHORIZATION.toString());

        if (authorizationHeader != null)
            return authorizationHeader.toString();

        var tracAuthHeader = headers.get(AuthConstants.TRAC_AUTH_TOKEN);

        if (tracAuthHeader != null)
            return tracAuthHeader.toString();

        var cookies = extractCookies(headers, cookieDirection);

        return findAuthCookie(cookies);
    }

    private static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    List<Cookie> extractCookies(THeaders headers, boolean cookieDirection) {

        var cookieHeader = cookieDirection == CLIENT_COOKIE ? HttpHeaderNames.SET_COOKIE : HttpHeaderNames.COOKIE;
        var cookieHeaders = headers.getAll(cookieHeader);
        var cookies = new ArrayList<Cookie>();

        if (cookieDirection == CLIENT_COOKIE) {
            for (var header : cookieHeaders) {
                cookies.add(ClientCookieDecoder.STRICT.decode(header.toString()));
            }
        }
        else {
            for (var header : cookieHeaders)
                cookies.addAll(ServerCookieDecoder.STRICT.decode(header.toString()));
        }

        return cookies;
    }

    private static String findAuthCookie(List<Cookie> cookies) {

        for (var cookie : cookies) {
            if (cookie.name().equals(AuthConstants.TRAC_AUTH_TOKEN)) {
                return cookie.value();
            }
        }

        return null;
    }

    public static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    boolean isBrowserRequest(THeaders headers) {

        var userAgent = headers.get(HttpHeaderNames.USER_AGENT);
        var contentType = headers.get(HttpHeaderNames.CONTENT_TYPE);
        var accept = headers.get(HttpHeaderNames.ACCEPT);
        var protocol = contentType != null ? contentType.toString() : accept != null ? accept.toString() : null;

        if (userAgent == null)
            return false;

        if (protocol == null)
            return true;

        return !protocol.startsWith("application/") || protocol.equals("application/x-www-form-urlencoded");
    }

    public static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    boolean wantCookies(THeaders headers) {

        var explicitWantCookie = headers.getBoolean(AuthConstants.TRAC_AUTH_COOKIES_HEADER, false);

        if (explicitWantCookie)
            return true;

        var serverCookies = extractCookies(headers, SERVER_COOKIE);
        var authCookie = findAuthCookie(serverCookies);

        return authCookie != null;
    }

    public static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    void removeAuthHeaders(THeaders headers, boolean cookieDirection) {

        var originalCookies = extractCookies(headers, cookieDirection);
        var cookies = filterCookies(originalCookies);

        filterHeaders(headers);

        var cookieHeader = cookieDirection == CLIENT_COOKIE ? HttpHeaderNames.SET_COOKIE : HttpHeaderNames.COOKIE;

        for (var cookie : cookies) {
            headers.add(cookieHeader, ServerCookieEncoder.STRICT.encode(cookie));
        }
    }

    private static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    void filterHeaders(THeaders headers) {

        for (var header : headers) {

            var headerName = header.getKey().toString().toLowerCase();

            if (headerName.startsWith(TRAC_AUTH_PREFIX) || headerName.startsWith(TRAC_USER_PREFIX))
                headers.remove(headerName);

            if (RESTRICTED_HEADERS.contains(headerName))
                headers.remove(headerName);
        }
    }

    private static List<Cookie> filterCookies(List<Cookie> cookies) {

        var filteredCookies = new ArrayList<Cookie>(cookies.size());

        for (var cookie : cookies) {

            var cookieName = cookie.name().toLowerCase();

            if (cookieName.startsWith(TRAC_AUTH_PREFIX) || cookieName.startsWith(TRAC_USER_PREFIX))
                continue;

            if (RESTRICTED_HEADERS.contains(cookieName))
                continue;

            filteredCookies.add(cookie);
        }

        return filteredCookies;
    }

    public static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    void addPlatformAuthHeaders(THeaders headers, String token) {

        // The platform only cares about the token, that is the definitive source of session info

        headers.add(AuthConstants.TRAC_AUTH_TOKEN_HEADER, token);
    }

    public static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    void addClientAuthHeaders(THeaders headers, String token, SessionInfo session) {

        // For API calls send session info back in headers, these come through as gRPC metadata
        // The web API package will use JavaScript to store these as cookies (cookies get lost over grpc-web)
        // They are also easier to work with in non-browser contexts

        // We use URL encoding to avoid issues with non-ascii characters
        // This also matches the way auth headers are sent back to browsers in cookies

        var expiry = DateTimeFormatter.ISO_INSTANT.format(session.getExpiryTime());
        var userId = URLEncoder.encode(session.getUserInfo().getUserId(), StandardCharsets.US_ASCII);
        var userName = URLEncoder.encode(session.getUserInfo().getDisplayName(), StandardCharsets.US_ASCII);

        headers.add(AuthConstants.TRAC_AUTH_TOKEN_HEADER, token);
        headers.add(AuthConstants.TRAC_AUTH_EXPIRY_HEADER, expiry);
        headers.add(AuthConstants.TRAC_USER_ID_HEADER, userId);
        headers.add(AuthConstants.TRAC_USER_NAME_HEADER, userName);
    }

    public static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    void addClientAuthCookies(THeaders headers, String token, SessionInfo session) {

        // For browser requests, send the session info back as cookies, this is by far the easiest approach
        // The web API package will look for a valid auth token cookie and send it as a header if available

        // We use URL encoding to avoid issues with non-ascii characters
        // Cookies are a lot stricter than regular headers so this is required
        // The other option is base 64, but URL encoding is more readable for humans

        var expiry = DateTimeFormatter.ISO_INSTANT.format(session.getExpiryTime());
        var userId = URLEncoder.encode(session.getUserInfo().getUserId(), StandardCharsets.US_ASCII);
        var userName = URLEncoder.encode(session.getUserInfo().getDisplayName(), StandardCharsets.US_ASCII);

        setClientCookie(headers, AuthConstants.TRAC_AUTH_TOKEN_HEADER, token, session.getExpiryTime(), true);
        setClientCookie(headers, AuthConstants.TRAC_AUTH_EXPIRY_HEADER, expiry, session.getExpiryTime(), false);
        setClientCookie(headers, AuthConstants.TRAC_USER_ID_HEADER, userId, session.getExpiryTime(), false);
        setClientCookie(headers, AuthConstants.TRAC_USER_NAME_HEADER, userName, session.getExpiryTime(), false);
    }

    private static <THeaders extends Headers<CharSequence, CharSequence, THeaders>>
    void setClientCookie(
            THeaders headers, CharSequence cookieName, String cookieValue,
            Instant expiry, boolean isAuthToken) {

        var cookie = new DefaultCookie(cookieName.toString(), cookieValue);

        // TODO: Can we know the value to set for domain?

        // Do not allow sending TRAC tokens to other end points
        cookie.setSameSite(CookieHeaderNames.SameSite.Strict);

        // Make sure cookies are sent to the API endpoints, even if the UI is served from a sub path
        cookie.setPath("/");

        // TRAC session cookies will expire when the session expires
        if (expiry != null) {
            var secondsToExpiry = Duration.between(Instant.now(), expiry).getSeconds();
            var maxAge = Math.max(secondsToExpiry, 0);
            cookie.setMaxAge(maxAge);
        }
        // Otherwise let the cookie live for the lifetime of the browser session
        else
            cookie.setMaxAge(Cookie.UNDEFINED_MAX_AGE);  // remove cookie on browser close

        // Restrict to HTTP access for the auth token, allow JavaScript access for everything else
        cookie.setHttpOnly(isAuthToken);

        headers.add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
    }
}
