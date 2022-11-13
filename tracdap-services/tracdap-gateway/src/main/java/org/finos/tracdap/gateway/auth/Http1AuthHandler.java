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

import org.finos.tracdap.common.auth.IAuthProvider;
import org.finos.tracdap.common.auth.JwtProcessor;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.*;
import io.netty.handler.codec.http.cookie.ClientCookieEncoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.util.ReferenceCountUtil;

import org.finos.tracdap.common.auth.SessionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;


public class Http1AuthHandler extends ChannelDuplexHandler {

    private static final String BEARER_AUTH_PREFIX = "bearer ";

    private static final Duration TOKEN_REFRESH_TIME = Duration.of(1, ChronoUnit.HOURS);
    private static final Duration TOKEN_EXPIRY = Duration.of(6, ChronoUnit.HOURS);

    private static final String TRAC_USER_ID_COOKIE = "trac_user_id";
    private static final String TRAC_USER_NAME_COOKIE = "trac_user_name";
    private static final String TRAC_SESSION_EXPIRY_UTC = "trac_session_expiry_utc";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IAuthProvider provider;
    private final JwtProcessor jwtProcessor;

    private boolean authOk = false;
    private String sessionToken;
    private SessionInfo sessionInfo;


    public Http1AuthHandler(IAuthProvider provider, JwtProcessor jwtProcessor) {
        this.provider = provider;
        this.jwtProcessor = jwtProcessor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        try {

            // After the initial request, let content through if the request message was authorized
            if (!(msg instanceof HttpRequest)) {
                if (authOk) {
                    ReferenceCountUtil.retain(msg);
                    ctx.fireChannelRead(msg);
                }
                return;
            }

            // New request, start in an unauthorized state
            authOk = false;

            // Look for any auth information that is in the request
            var req = (HttpRequest) msg;
            var headers = req.headers();
            var authInfo = getAuthToken(headers);

            authOk = doAuthenticate(ctx, req, authInfo);

            if (authOk) {
                ReferenceCountUtil.retain(req);
                ctx.fireChannelRead(req);
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }


    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        // After the initial request, let content through if the request message was authorized
        if (!(msg instanceof HttpResponse)) {
            ctx.write(msg, promise);
            return;
        }

        var resp = (HttpResponse) msg;
        setClientCookies(resp);

        ctx.write(msg, promise);
    }

    private boolean doAuthenticate(ChannelHandlerContext ctx, HttpRequest req, String authInfo) {

        // If there is no auth token at all, trigger a new auth workflow
        if (authInfo == null) {

            log.debug("AUTHENTICATION: Required");

            var userInfo = provider.newAuth(ctx, req);

            if (userInfo != null)
                authInfo = BEARER_AUTH_PREFIX + jwtProcessor.encodeToken(userInfo);
            else
                return false;
        }

        var prefixEnd = Math.min(BEARER_AUTH_PREFIX.length(), authInfo.length());
        var prefix = authInfo.substring(0, prefixEnd);

        if (!prefix.equalsIgnoreCase(BEARER_AUTH_PREFIX)) {

            log.trace("AUTHENTICATION: Translation required");

            var userInfo = provider.translateAuth(ctx, req, authInfo);

            if (userInfo != null)
                authInfo = BEARER_AUTH_PREFIX + jwtProcessor.encodeToken(userInfo);
            else
                return false;
        }

        var token = authInfo.substring(BEARER_AUTH_PREFIX.length());
        var session = jwtProcessor.decodeAndValidate(token);

        if (!session.isValid()) {

            log.warn("AUTHENTICATION: Previous login is no longer valid: {}", session.getErrorMessage());

            var userInfo = provider.newAuth(ctx, req);
            if (userInfo == null)
                return false;
            token = jwtProcessor.encodeToken(userInfo);
            session = jwtProcessor.decodeAndValidate(token);
        }

        if (session.getIssueTime().plus(TOKEN_REFRESH_TIME).isAfter(Instant.now())) {

            token = jwtProcessor.encodeToken(session.getUserInfo());
        }

        log.trace("AUTHENTICATION: Succeeded");

        this.sessionToken = token;
        this.sessionInfo = session;

        setTargetCookies(req);

        return true;
    }

    private String getAuthToken(HttpHeaders headers) {

        if (headers.contains(HttpHeaderNames.AUTHORIZATION)) {

            return headers.get(HttpHeaderNames.AUTHORIZATION);
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
                return BEARER_AUTH_PREFIX + authCookie.get().value();
            }
        }

        return null;
    }

    private void setTargetCookies(HttpRequest req) {

        setTargetCookie(req, HttpHeaderNames.AUTHORIZATION, sessionToken);
    }

    private void setTargetCookie(HttpRequest req, CharSequence cookieName, String cookieValue) {

        var cookie = new DefaultCookie(cookieName.toString(), cookieValue);
        cookie.setMaxAge(TOKEN_EXPIRY.getSeconds());
        cookie.setSameSite(CookieHeaderNames.SameSite.Strict);
        cookie.setSecure(true);
        cookie.setHttpOnly(true);

        req.headers().add(HttpHeaderNames.COOKIE, ClientCookieEncoder.STRICT.encode(cookie));
    }

    private void setClientCookies(HttpResponse resp) {

        setClientCookie(resp, HttpHeaderNames.AUTHORIZATION, sessionToken);
        setClientCookie(resp, TRAC_USER_ID_COOKIE, sessionInfo.getUserInfo().getUserId());

        var displayName = URLEncoder.encode(sessionInfo.getUserInfo().getDisplayName(), StandardCharsets.US_ASCII);
        setClientCookie(resp, TRAC_USER_NAME_COOKIE, displayName);

        var expiryUtc = DateTimeFormatter.ISO_INSTANT.format(sessionInfo.getExpiryTime());
        setClientCookie(resp, TRAC_SESSION_EXPIRY_UTC, expiryUtc);
    }

    private void setClientCookie(HttpResponse resp, CharSequence cookieName, String cookieValue) {

        var cookie = new DefaultCookie(cookieName.toString(), cookieValue);
        cookie.setMaxAge(Cookie.UNDEFINED_MAX_AGE);  // remove cookie on browser close
        cookie.setSameSite(CookieHeaderNames.SameSite.Strict);
        cookie.setSecure(true);
        cookie.setHttpOnly(false);  // allow access to JavaScript

        resp.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
    }
}
