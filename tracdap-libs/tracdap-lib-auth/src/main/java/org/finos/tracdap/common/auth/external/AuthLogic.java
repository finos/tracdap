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
import org.finos.tracdap.common.auth.internal.SessionInfo;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.config.ConfigDefaults;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.AuthenticationConfig;

import io.netty.handler.codec.http.HttpHeaderNames;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


public class AuthLogic<THeaders extends AuthHeaders> {

    private static final String TRAC_AUTH_TOKEN_HEADER = "trac_auth_token";
    private static final String TRAC_AUTH_EXPIRY_GMT = "trac_auth_expiry_gmt";
    private static final String TRAC_USER_ID_HEADER = "trac_user_idn";
    private static final String TRAC_USER_NAME_HEADER = "trac_user_name";

    private static final String TRAC_AUTH_PREFIX = "trac_auth_";
    private static final String TRAC_USER_PREFIX = "trac_user_";

    public static final boolean CLIENT_COOKIE = true;
    public static final boolean SERVER_COOKIE = false;

    private static final List<String> RESTRICTED_HEADERS = List.of(
            HttpHeaderNames.AUTHORIZATION.toString(),
            HttpHeaderNames.COOKIE.toString(),
            HttpHeaderNames.SET_COOKIE.toString());

    private static final String BEARER_PREFIX = "bearer ";


    public String findTracAuthToken(THeaders headers, boolean cookieDirection) {

        var bearerToken = findTracBearerAuthToken(headers, cookieDirection);

        if (bearerToken == null)
            return null;

        if (bearerToken.toLowerCase().startsWith(BEARER_PREFIX))
            return bearerToken.substring(BEARER_PREFIX.length());
        else
            return bearerToken;
    }

    private String findTracBearerAuthToken(THeaders headers, boolean cookieDirection) {

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

        return null;
    }

    private String findHeader(THeaders headers, String headerName) {

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

    private List<Cookie> extractCookies(THeaders headers, boolean cookieDirection) {

        var cookieHeader = cookieDirection == CLIENT_COOKIE ? HttpHeaderNames.SET_COOKIE : HttpHeaderNames.COOKIE;

        var cookieHeaders = headers.getAll(cookieHeader);
        var cookies = new ArrayList<Cookie>();

        for (var header : cookieHeaders)
            cookies.addAll(ServerCookieDecoder.LAX.decodeAll(header.toString()));

        return cookies;
    }

    public SessionInfo newSession(UserInfo userInfo, AuthenticationConfig authConfig) {

        var configExpiry = ConfigDefaults.readOrDefault(authConfig.getJwtExpiry(), ConfigDefaults.DEFAULT_JWT_EXPIRY);
        var configLimit = ConfigDefaults.readOrDefault(authConfig.getJwtLimit(), ConfigDefaults.DEFAULT_JWT_LIMIT);

        var issue = Instant.now();
        var expiry = issue.plusSeconds(configExpiry);
        var limit = issue.plusSeconds(configLimit);

        var session = new SessionInfo();
        session.setUserInfo(userInfo);
        session.setIssueTime(issue);
        session.setExpiryTime(expiry);
        session.setLimitTime(limit);
        session.setValid(true);

        return session;
    }

    public SessionInfo refreshSession(SessionInfo session, AuthenticationConfig authConfig) {

        var latestIssue = session.getIssueTime();
        var originalLimit = session.getLimitTime();

        var refreshTime = ConfigDefaults.readOrDefault(authConfig.getJwtRefresh(), ConfigDefaults.DEFAULT_JWT_REFRESH);
        var configExpiry = ConfigDefaults.readOrDefault(authConfig.getJwtExpiry(), ConfigDefaults.DEFAULT_JWT_EXPIRY);

        // If the refresh time hasn't elapsed yet, return the original session without modification
        if (latestIssue.plusSeconds(refreshTime).isAfter(Instant.now()))
            return session;

        var newIssue = Instant.now();
        var newExpiry = newIssue.plusSeconds(configExpiry);
        var limitedExpiry = newExpiry.isBefore(originalLimit) ? newExpiry : originalLimit;

        var newSession = new SessionInfo();
        newSession.setUserInfo(session.getUserInfo());
        newSession.setIssueTime(newIssue);
        newSession.setExpiryTime(limitedExpiry);
        newSession.setLimitTime(originalLimit);

        // Session remains valid until time ticks past the original limit time, i.e. issue > limit
        newSession.setValid(newIssue.isBefore(originalLimit));

        return newSession;
    }

    public THeaders updateAuthHeaders(
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

    public THeaders removeAllAuthHeaders(THeaders headers, THeaders emptyHeaders, boolean cookieDirection) {

        var cookies = extractCookies(headers, cookieDirection);

        var filteredHeaders = filterHeaders(headers, emptyHeaders);
        var filteredCookies = filterCookies(cookies);

        var cookieHeader = cookieDirection == CLIENT_COOKIE ? HttpHeaderNames.SET_COOKIE : HttpHeaderNames.COOKIE;

        for (var cookie : filteredCookies) {
            filteredHeaders.add(cookieHeader, ServerCookieEncoder.STRICT.encode(cookie));
        }

        return filteredHeaders;
    }

    private THeaders filterHeaders(THeaders headers, THeaders newHeaders) {

        for (var header : headers) {

            var headerName = header.getKey().toLowerCase();

            if (headerName.startsWith(TRAC_AUTH_PREFIX) || headerName.startsWith(TRAC_USER_PREFIX))
                continue;

            if (RESTRICTED_HEADERS.contains(headerName))
                continue;

            newHeaders.add(headerName, header.getValue());
        }

        return newHeaders;
    }

    private List<Cookie> filterCookies(List<Cookie> cookies) {

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

    public THeaders addHeadersForPlatform(THeaders headers, String token) {

        // The platform only cares about the token, that is the definitive source of session info

        headers.add(TRAC_AUTH_TOKEN_HEADER, token);
        return headers;
    }

    public THeaders addHeadersForApi(THeaders headers, String token, SessionInfo session) {

        // For API calls send session info back in headers, these come through as gRPC metadata
        // The web API package will use JavaScript to store these as cookies (cookies get lost over grpc-web)
        // They are also easier to work with in non-browser contexts

        var expiryFmt = DateTimeFormatter.ISO_INSTANT.format(session.getExpiryTime());

        headers.add(TRAC_AUTH_TOKEN_HEADER, token);
        headers.add(TRAC_AUTH_EXPIRY_GMT, expiryFmt);
        headers.add(TRAC_USER_ID_HEADER, session.getUserInfo().getUserId());
        headers.add(TRAC_USER_NAME_HEADER, session.getUserInfo().getDisplayName());

        return headers;
    }

    public THeaders addHeadersForBrowser(THeaders headers, String token, SessionInfo session) {

        // For browser requests, send the session info back as cookies, this is by far the easiest approach
        // The web API package will look for a valid auth token cookie and send it as a header if available

        var expiryFmt = DateTimeFormatter.ISO_INSTANT.format(session.getExpiryTime());

        setClientCookie(headers, TRAC_AUTH_TOKEN_HEADER, token);
        setClientCookie(headers, TRAC_AUTH_EXPIRY_GMT, expiryFmt);
        setClientCookie(headers, TRAC_USER_ID_HEADER, session.getUserInfo().getUserId());
        setClientCookie(headers, TRAC_USER_NAME_HEADER, session.getUserInfo().getDisplayName());

        return headers;
    }

    private void setClientCookie(THeaders headers, CharSequence cookieName, String cookieValue) {

        var cookie = new DefaultCookie(cookieName.toString(), cookieValue);
        cookie.setMaxAge(Cookie.UNDEFINED_MAX_AGE);  // remove cookie on browser close
        cookie.setSameSite(CookieHeaderNames.SameSite.Strict);
        cookie.setSecure(true);
        cookie.setHttpOnly(false);  // allow access to JavaScript

        headers.add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
    }
}
