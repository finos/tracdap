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

package org.finos.tracdap.gateway.auth;

import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.finos.tracdap.common.auth.internal.JwtValidator;
import org.finos.tracdap.common.auth.internal.SessionInfo;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.http.Http1Headers;
import org.finos.tracdap.common.netty.ConnectionId;
import org.finos.tracdap.common.util.LoggingHelpers;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;


/**
 * Gateway auth handler based on the regular HTTP/1 auth validator
 * <br/>
 *
 * The base validator just requires a valid login token, anything else
 * is rejected. There is no logic for processing logins, that is handled
 * by the auth service.
 * <br/>
 *
 * This version is Updated to allow unauthenticated requests to the
 * auth service, and to redirect unauthenticated requests from browsers
 * to the browser login URL.
 */
public class Http1AuthHandler extends ChannelDuplexHandler {

    private static final String RESULT_PASS = "PASS";
    private static final String RESULT_ALLOW = "ALLOW_LOGIN";
    private static final String RESULT_FAIL = "FAIL";

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

    private final AuthHandlerSettings handlerSettings;
    private final JwtValidator jwtValidator;

    private long connId = -1;
    private long reqId = -1;
    private RequestState state = null;

    private static class RequestState {

        Instant requestTime;
        String token;
        SessionInfo session;
        boolean authenticated;
        boolean refreshWanted;
        boolean isLogin;
        boolean wantCookies;
    }


    public Http1AuthHandler(AuthHandlerSettings handlerSettings, JwtValidator jwtValidator) {

        this.handlerSettings = handlerSettings;
        this.jwtValidator = jwtValidator;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {

        connId = ConnectionId.get(ctx.channel());

        if (log.isTraceEnabled())
            log.trace("Http1AuthHandler handlerAdded: conn = {}", connId);

        if (handlerSettings.authConfig().getDisableAuth()) {

            log.warn("conn = {}, Authentication disabled in config, auth handler will be removed for this connection", connId);

            // Channel initializer gets confused if a newly added handler is not available
            // Instead, replace this handler with a no-op and remove it later
            // Anyway this setting cannot be enabled in a production deployment

            var handlerName = ctx.name();
            ctx.pipeline().replace(handlerName, handlerName, new ChannelDuplexHandler());
            ctx.executor().execute(() -> ctx.pipeline().remove(handlerName));
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {

        if (log.isTraceEnabled())
            log.trace("Http1AuthHandler handlerRemoved: conn = {}", connId);
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

        try {

            if (log.isTraceEnabled())
                log.trace("Http1AuthHandler channelRead: conn = {}, msg = {}", connId, msg);

            if (!(msg instanceof HttpObject || msg instanceof WebSocketFrame)) {
                ctx.close();
                throw new EUnexpected();
            }

            if (msg instanceof HttpRequest) {

                var request = (HttpRequest) msg;
                reqId++;

                newState();
                checkAuthentication(request);
                checkIsLogin(request);
                checkRefreshWanted();

                logAuthentication(request, state);

                if (state.authenticated || state.isLogin)
                    translateRequestHeaders(request);
                else
                    sendFailResponse(ctx, request);
            }

            if (state.authenticated || state.isLogin) {
                ReferenceCountUtil.retain(msg);
                ctx.fireChannelRead(msg);
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        try {

            if (log.isTraceEnabled())
                log.trace("Http1AuthHandler write: conn = {}, msg = {}", connId, msg);

            if (!(msg instanceof HttpObject || msg instanceof WebSocketFrame)) {
                ctx.close();
                throw new EUnexpected();
            }

            if (msg instanceof HttpResponse) {

                var response = (HttpResponse) msg;
                translateResponseHeaders(response);
            }

            ReferenceCountUtil.retain(msg);
            ctx.write(msg, promise);
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    void newState() {

        var state = new RequestState();
        state.requestTime = Instant.now();
        state.token = null;
        state.session = null;
        state.authenticated = false;
        state.refreshWanted = false;

        this.state = state;
    }

    private void checkAuthentication(HttpRequest request) {

        // Look for an existing session token in the request
        // If the token gives a valid session then authentication has succeeded

        var headers = Http1Headers.wrapHttpHeaders(request.headers());
        var token = AuthHelpers.findTracAuthToken(headers, AuthHelpers.SERVER_COOKIE);
        var session = token != null ? jwtValidator.decodeAndValidate(token) : null;

        if (session != null && session.isValid()) {
            state.authenticated = true;
            state.token = token;
            state.session = session;
        }

        if (AuthHelpers.isBrowserRequest(headers) || AuthHelpers.wantCookies(headers))
            state.wantCookies = true;
    }

    private void checkIsLogin(HttpRequest request) {

        if (request.uri().startsWith(handlerSettings.publicLoginPrefix()))
            state.isLogin = true;
    }

    private void checkRefreshWanted() {

        if (state.authenticated && !state.isLogin) {

            var issueTime = state.session.getIssueTime();
            var refreshTime = issueTime.plus(handlerSettings.refreshInterval());

            if (state.requestTime.isAfter(refreshTime))
                state.refreshWanted = true;
        }
    }

    private void translateRequestHeaders(HttpRequest request) {

        var headers = Http1Headers.wrapHttpHeaders(request.headers());

        AuthHelpers.removeAuthHeaders(headers, AuthHelpers.SERVER_COOKIE);

        if (state.token != null)
            AuthHelpers.addPlatformAuthHeaders(headers, state.token);
    }

    private void translateResponseHeaders(HttpResponse response) {

        var headers = Http1Headers.wrapHttpHeaders(response.headers());
        var newToken = AuthHelpers.findTracAuthToken(headers, AuthHelpers.CLIENT_COOKIE);

        if (newToken != null) {
            state.token = newToken;
            state.session = jwtValidator.decodeAndValidate(newToken);
        }

        AuthHelpers.removeAuthHeaders(headers, AuthHelpers.CLIENT_COOKIE);

        if (state.token != null)
            if (state.wantCookies)
                AuthHelpers.addClientAuthCookies(headers, state.token, state.session);
            else
                AuthHelpers.addClientAuthHeaders(headers, state.token, state.session);
    }

    private void sendFailResponse(ChannelHandlerContext ctx, HttpRequest request) {

        var requestHeaders = Http1Headers.wrapHttpHeaders(request.headers());

        if (AuthHelpers.isBrowserRequest(requestHeaders)) {

            var loginUrl = handlerSettings.publicLoginUrl();
            var returnPath = URLEncoder.encode(request.uri(), StandardCharsets.US_ASCII);
            var loginRedirect = loginUrl.replace(AuthHandlerSettings.RETURN_PATH_VARIABLE, returnPath);

            var redirectResponse = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.TEMPORARY_REDIRECT);
            redirectResponse.headers().set(HttpHeaderNames.LOCATION, loginRedirect);

            ctx.writeAndFlush(redirectResponse);
        }
        else {

            var unauthorizedResponse = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.UNAUTHORIZED);
            ctx.writeAndFlush(unauthorizedResponse);
        }
    }

    private void logAuthentication(HttpRequest request, RequestState state) {

        String result;
        String userId;

        if (state.authenticated)
            result = RESULT_PASS;
        else if (state.isLogin)
            result = RESULT_ALLOW;
        else if (state.token == null)
            result = RESULT_FAIL + " (no token)";
        else if (state.session == null || !state.session.isValid())
            result = RESULT_FAIL + " (invalid token)";
        else if (state.session.getExpiryTime().isAfter(Instant.now()))
            result = RESULT_FAIL + " (expired)";
        else
            result = RESULT_FAIL;

        if (state.session != null && state.session.getUserInfo() != null)
            userId = state.session.getUserInfo().getUserId();
        else
            userId = null;

        if (userId != null)
            log.info("AUTHENTICATE: conn = {}, req = {}, result = {}, user= {}, {}", connId, reqId, result, userId, request.uri());
        else
            log.info("AUTHENTICATE: conn = {}, req = {}, result = {}, {}", connId, reqId, result, request.uri());
    }
}
