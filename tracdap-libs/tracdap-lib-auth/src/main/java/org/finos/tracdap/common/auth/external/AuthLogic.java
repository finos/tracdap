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

import io.netty.handler.codec.http.cookie.*;
import org.finos.tracdap.common.auth.AuthConstants;
import org.finos.tracdap.common.auth.internal.SessionInfo;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.config.ConfigDefaults;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.AuthenticationConfig;

import io.netty.handler.codec.http.HttpHeaderNames;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


public class AuthLogic {

    public static final String TRAC_AUTH_TOKEN_HEADER = AuthConstants.TRAC_AUTH_TOKEN;
    public static final String TRAC_AUTH_EXPIRY_HEADER = "trac_auth_expiry";
    public static final String TRAC_USER_ID_HEADER = "trac_user_id";
    public static final String TRAC_USER_NAME_HEADER = "trac_user_name";

    public static final String TRAC_AUTH_PREFIX = "trac_auth_";
    public static final String TRAC_USER_PREFIX = "trac_user_";

    public static final boolean CLIENT_COOKIE = true;
    public static final boolean SERVER_COOKIE = false;

    private static final List<String> RESTRICTED_HEADERS = List.of(
            HttpHeaderNames.AUTHORIZATION.toString(),
            HttpHeaderNames.COOKIE.toString(),
            HttpHeaderNames.SET_COOKIE.toString());

    private static final String BEARER_PREFIX = "bearer ";

    private static final String NULL_AUTH_TOKEN = null;


    // Logic class, do not allow creating instances
    private AuthLogic() {}


    public static String findTracAuthToken(IAuthHeaders headers, boolean cookieDirection) {

        var rawToken = findRawAuthToken(headers, cookieDirection);

        if (rawToken == null)
            return null;

        // Remove the "bearer" prefix if the auth token header is stored that way

        if (rawToken.toLowerCase().startsWith(BEARER_PREFIX))
            return rawToken.substring(BEARER_PREFIX.length());
        else
            return rawToken;
    }

    private static String findRawAuthToken(IAuthHeaders headers, boolean cookieDirection) {

        var cookies = extractCookies(headers, cookieDirection);

        var tracAuthHeader = findHeader(headers, TRAC_AUTH_TOKEN_HEADER);
        if (tracAuthHeader != null) return tracAuthHeader;

        var tracAuthCookie = findCookie(cookies, TRAC_AUTH_TOKEN_HEADER);
        if (tracAuthCookie != null) return tracAuthCookie;

        var authorizationHeader = findHeader(headers, HttpHeaderNames.AUTHORIZATION.toString());
        if (authorizationHeader != null) return authorizationHeader;

        var authorizationCookie = findCookie(cookies, HttpHeaderNames.AUTHORIZATION.toString());
        if (authorizationCookie != null) return authorizationCookie;

        // Run out of places to look!

        return NULL_AUTH_TOKEN;
    }

    private static String findHeader(IAuthHeaders headers, String headerName) {

        if (headers.contains(headerName))
            return headers.get(headerName).toString();

        return null;
    }

    private static String findCookie(List<Cookie> cookies, String cookieName) {

        for (var cookie : cookies) {
            if (cookie.name().equals(cookieName)) {
                // TODO: Other fields in the cooke can make it specific, domain etc.
                return cookie.value();
            }
        }

        return null;
    }

    private static List<Cookie> extractCookies(IAuthHeaders headers, boolean cookieDirection) {

        var cookieHeader = cookieDirection == CLIENT_COOKIE ? HttpHeaderNames.SET_COOKIE : HttpHeaderNames.COOKIE;

        var cookieHeaders = headers.getAll(cookieHeader);
        var cookies = new ArrayList<Cookie>();

        for (var header : cookieHeaders)
            cookies.addAll(ServerCookieDecoder.LAX.decodeAll(header.toString()));

        return cookies;
    }

    public static SessionInfo newSession(UserInfo userInfo, AuthenticationConfig authConfig) {

        var configExpiry = ConfigDefaults.readOrDefault(authConfig.getJwtExpiry(), ConfigDefaults.DEFAULT_JWT_EXPIRY);
        var configLimit = ConfigDefaults.readOrDefault(authConfig.getJwtLimit(), ConfigDefaults.DEFAULT_JWT_LIMIT);

        var issue = Instant.now();
        var expiry = issue.plusSeconds(configExpiry);
        var limit = issue.plusSeconds(configLimit);

        var session = new SessionInfo();
        session.setUserInfo(userInfo);
        session.setIssueTime(issue);
        session.setExpiryTime(expiry);
        session.setExpiryLimit(limit);
        session.setValid(true);

        return session;
    }

    public static SessionInfo refreshSession(SessionInfo session, AuthenticationConfig authConfig) {

        var latestIssue = session.getIssueTime();
        var originalLimit = session.getExpiryLimit();

        var configRefresh = ConfigDefaults.readOrDefault(authConfig.getJwtRefresh(), ConfigDefaults.DEFAULT_JWT_REFRESH);
        var configExpiry = ConfigDefaults.readOrDefault(authConfig.getJwtExpiry(), ConfigDefaults.DEFAULT_JWT_EXPIRY);

        // If the refresh time hasn't elapsed yet, return the original session without modification
        if (latestIssue.plusSeconds(configRefresh).isAfter(Instant.now()))
            return session;

        var newIssue = Instant.now();
        var newExpiry = newIssue.plusSeconds(configExpiry);
        var limitedExpiry = newExpiry.isBefore(originalLimit) ? newExpiry : originalLimit;

        var newSession = new SessionInfo();
        newSession.setUserInfo(session.getUserInfo());
        newSession.setIssueTime(newIssue);
        newSession.setExpiryTime(limitedExpiry);
        newSession.setExpiryLimit(originalLimit);

        // Session remains valid until time ticks past the original limit time, i.e. issue < limit
        newSession.setValid(newIssue.isBefore(originalLimit));

        return newSession;
    }

    public static <THeaders extends IAuthHeaders>
    THeaders removeAllAuthHeaders(THeaders headers, THeaders emptyHeaders, boolean cookieDirection) {

        var cookies = extractCookies(headers, cookieDirection);

        var filteredHeaders = filterHeaders(headers, emptyHeaders);
        var filteredCookies = filterCookies(cookies);

        var cookieHeader = cookieDirection == CLIENT_COOKIE ? HttpHeaderNames.SET_COOKIE : HttpHeaderNames.COOKIE;

        for (var cookie : filteredCookies) {
            filteredHeaders.add(cookieHeader, ServerCookieEncoder.STRICT.encode(cookie));
        }

        return filteredHeaders;
    }

    public static <THeaders extends IAuthHeaders>
    THeaders updateAuthHeaders(
            THeaders headers, THeaders emptyHeaders,
            String token, SessionInfo session,
            RouteType routeType, boolean cookieDirection) {

        var filtered = removeAllAuthHeaders(headers, emptyHeaders, cookieDirection);

        switch (routeType) {
            case BROWSER_ROUTE:
                return addHeadersForBrowser(filtered, token, session);
            case API_ROUTE:
                return addHeadersForApi(filtered, token, session);
            case PLATFORM_ROUTE:
                return addHeadersForPlatform(filtered, token);
            default:
                throw new EUnexpected();
        }
    }

    private static <THeaders extends IAuthHeaders>
    THeaders filterHeaders(THeaders headers, THeaders newHeaders) {

        for (var header : headers) {

            var headerName = header.getKey().toString().toLowerCase();

            if (headerName.startsWith(TRAC_AUTH_PREFIX) || headerName.startsWith(TRAC_USER_PREFIX))
                continue;

            if (RESTRICTED_HEADERS.contains(headerName))
                continue;

            newHeaders.add(headerName, header.getValue());
        }

        return newHeaders;
    }

    private static List<Cookie> filterCookies(List<Cookie> cookies) {

        var filtered = new ArrayList<Cookie>();

        for (var cookie : cookies) {

            var cookieName = cookie.name().toLowerCase();

            if (cookieName.startsWith(TRAC_AUTH_PREFIX) || cookieName.startsWith(TRAC_USER_PREFIX))
                continue;

            if (RESTRICTED_HEADERS.contains(cookieName))
                continue;

            filtered.add(cookie);
        }

        return filtered;
    }

    public static <THeaders extends IAuthHeaders>
    THeaders addHeadersForPlatform(THeaders headers, String token) {

        // The platform only cares about the token, that is the definitive source of session info

        headers.add(TRAC_AUTH_TOKEN_HEADER, token);
        return headers;
    }

    public static <THeaders extends IAuthHeaders>
    THeaders addHeadersForApi(THeaders headers, String token, SessionInfo session) {

        // For API calls send session info back in headers, these come through as gRPC metadata
        // The web API package will use JavaScript to store these as cookies (cookies get lost over grpc-web)
        // They are also easier to work with in non-browser contexts

        // We use URL encoding to avoid issues with non-ascii characters
        // This also matches the way auth headers are sent back to browsers in cookies

        var expiry = DateTimeFormatter.ISO_INSTANT.format(session.getExpiryTime());
        var userId = URLEncoder.encode(session.getUserInfo().getUserId(), StandardCharsets.US_ASCII);
        var userName = URLEncoder.encode(session.getUserInfo().getDisplayName(), StandardCharsets.US_ASCII);

        headers.add(TRAC_AUTH_TOKEN_HEADER, token);
        headers.add(TRAC_AUTH_EXPIRY_HEADER, expiry);
        headers.add(TRAC_USER_ID_HEADER, userId);
        headers.add(TRAC_USER_NAME_HEADER, userName);

        return headers;
    }

    public static <THeaders extends IAuthHeaders>
    THeaders addHeadersForBrowser(THeaders headers, String token, SessionInfo session) {

        // For browser requests, send the session info back as cookies, this is by far the easiest approach
        // The web API package will look for a valid auth token cookie and send it as a header if available

        // We use URL encoding to avoid issues with non-ascii characters
        // Cookies are a lot stricter than regular headers so this is required
        // The other option is base 64, but URL encoding is more readable for humans

        var expiry = DateTimeFormatter.ISO_INSTANT.format(session.getExpiryTime());
        var userId = URLEncoder.encode(session.getUserInfo().getUserId(), StandardCharsets.US_ASCII);
        var userName = URLEncoder.encode(session.getUserInfo().getDisplayName(), StandardCharsets.US_ASCII);

        setClientCookie(headers, TRAC_AUTH_TOKEN_HEADER, token, session.getExpiryTime(), true);
        setClientCookie(headers, TRAC_AUTH_EXPIRY_HEADER, expiry, session.getExpiryTime(), false);
        setClientCookie(headers, TRAC_USER_ID_HEADER, userId, session.getExpiryTime(), false);
        setClientCookie(headers, TRAC_USER_NAME_HEADER, userName, session.getExpiryTime(), false);

        return headers;
    }

    private static void setClientCookie(
            IAuthHeaders headers, CharSequence cookieName, String cookieValue,
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
