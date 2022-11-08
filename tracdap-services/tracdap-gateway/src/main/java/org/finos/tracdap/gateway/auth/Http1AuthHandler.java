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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.*;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.util.ReferenceCountUtil;

import org.finos.tracdap.common.auth.JwtHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;


public class Http1AuthHandler extends ChannelDuplexHandler {

    private static final String BEARER_AUTH_PREFIX = "bearer ";

    private static final Duration TOKEN_REFRESH_TIME = Duration.of(1, ChronoUnit.HOURS);
    private static final Duration TOKEN_EXPIRY = Duration.of(6, ChronoUnit.HOURS);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AuthProvider provider;

    private boolean authOk = false;
    private String sessionToken;


    public Http1AuthHandler(AuthProvider provider) {
        this.provider = provider;
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
        setAuthToken(resp);

        ctx.write(msg, promise);
    }

    private boolean doAuthenticate(ChannelHandlerContext ctx, HttpRequest req, String authInfo) {

        // If there is no auth token at all, trigger a new auth workflow
        if (authInfo == null) {

            log.debug("AUTHENTICATION: Required");

            var token = provider.newAuth(ctx, req);

            if (token != null)
                authInfo = BEARER_AUTH_PREFIX + token;
            else
                return false;
        }

        var prefixEnd = Math.min(BEARER_AUTH_PREFIX.length(), authInfo.length());
        var prefix = authInfo.substring(0, prefixEnd);

        if (!prefix.equalsIgnoreCase(BEARER_AUTH_PREFIX)) {

            log.trace("AUTHENTICATION: Translation required");

            var token = provider.translateAuth(ctx, req, authInfo);

            if (token != null)
                authInfo = BEARER_AUTH_PREFIX + token;
            else
                return false;
        }

        var token = authInfo.substring(BEARER_AUTH_PREFIX.length());
        var session = JwtHelpers.decodeAndValidate(token);

        if (!session.isValid()) {

            log.warn("AUTHENTICATION: Previous login is no longer valid: {}", session.getErrorMessage());

            token = provider.newAuth(ctx, req);
            if (token == null)
                return false;
            session = JwtHelpers.decodeAndValidate(token);
        }

        if (session.getIssueTime().plus(TOKEN_REFRESH_TIME).isAfter(Instant.now())) {

            token = JwtHelpers.encodeToken(session.getUserInfo(), TOKEN_EXPIRY);
        }

        log.trace("AUTHENTICATION: Succeeded");

        this.sessionToken = token;

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

    private void setAuthToken(HttpResponse resp) {

        try {

            var authInfo = sessionToken;
            resp.headers().set(HttpHeaderNames.AUTHORIZATION, authInfo);

            var authCookie = new DefaultCookie(HttpHeaderNames.AUTHORIZATION.toString(), authInfo);
            authCookie.setMaxAge(Cookie.UNDEFINED_MAX_AGE);  // remove cookie on browser close
            authCookie.setSameSite(CookieHeaderNames.SameSite.Strict);
            authCookie.setSecure(true);
            authCookie.setHttpOnly(false);  // allow JavaScript to see the auth token

            resp.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(authCookie));
        }
        catch (Throwable e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }
}
