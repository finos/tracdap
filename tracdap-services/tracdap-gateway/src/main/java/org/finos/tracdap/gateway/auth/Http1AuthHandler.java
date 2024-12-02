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

import org.finos.tracdap.common.auth.internal.HttpAuthHelpers;
import org.finos.tracdap.common.auth.internal.JwtValidator;
import org.finos.tracdap.common.auth.internal.SessionInfo;
import org.finos.tracdap.common.config.ConfigDefaults;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.util.LoggingHelpers;
import org.finos.tracdap.config.AuthenticationConfig;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;


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

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

    private final AuthenticationConfig authConfig;
    private final JwtValidator jwtValidator;

    // TODO: Get these config values

    private static final String browserLoginUri = "/trac-auth/login/browser?return-path=%s";

    private final List<Map.Entry<String, Boolean>> openRoutes = List.of(
            Map.entry("/trac-auth/login/refresh", false),
            Map.entry("/trac-auth/login/", true));

    private final int configRefresh;
    private final Duration refreshTimeout = Duration.ofSeconds(2);

    private Channel refreshChannel;
    private RequestState state;

    private static class RequestState {

        Instant requestTime;
        String token;
        SessionInfo session;
        boolean authenticated;
        boolean refreshWanted;

        Queue<Map.Entry<Object, ChannelPromise>> refreshQueue;
        ChannelPromise refreshPromise;
        HttpHeaders refreshHeaders;
    }

    public Http1AuthHandler(AuthenticationConfig authConfig, JwtValidator jwtValidator) {

        this.authConfig = authConfig;
        this.jwtValidator = jwtValidator;

        this.configRefresh = ConfigDefaults.readOrDefault(authConfig.getJwtRefresh(), ConfigDefaults.DEFAULT_JWT_REFRESH);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {

        if (authConfig.getDisableAuth()) {

            log.warn("Authentication disabled in config, auth handler will be removed for this connection");

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

        if (state != null)
            releaseState();

        if (refreshChannel != null) {
            refreshChannel.close();
            refreshChannel = null;
        }
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

        try {

            if (msg instanceof HttpRequest) {

                var request = (HttpRequest) msg;

                newState();
                checkAuthentication(request);
                checkRefreshWanted();

                if (!state.authenticated)
                    sendFailResponse(ctx, request);
                else if (state.refreshWanted)
                    sendRefreshRequest(ctx);
            }

            if (msg instanceof HttpObject || msg instanceof WebSocketFrame) {

                if (state.authenticated) {
                    ReferenceCountUtil.retain(msg);
                    ctx.fireChannelRead(msg);
                }
            }
            else
                throw new EUnexpected();
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if (state.refreshPromise != null  && !state.refreshPromise.isDone()) {
            state.refreshQueue.add(Map.entry(msg, promise));
            return;
        }

        try {

            if (msg instanceof HttpResponse && state.refreshHeaders != null) {

                var response = (HttpResponse) msg;
                var headers = response.headers();

                // TODO: Is this ok?
                for (var header : state.refreshHeaders)
                    headers.add(header.getKey(), header.getValue());
            }

            if (msg instanceof HttpObject || msg instanceof WebSocketFrame) {

                ReferenceCountUtil.retain(msg);
                ctx.write(msg, promise);
            }
            else
                throw new EUnexpected();
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    void newState() {

        if (state != null)
            releaseState();

        var state = new RequestState();
        state.requestTime = Instant.now();
        state.token = null;
        state.session = null;
        state.authenticated = false;
        state.refreshWanted = false;
        state.refreshHeaders = null;

        this.state = state;
    }

    private void releaseState() {

        if (state.refreshQueue != null) {

            var queueItem = state.refreshQueue.poll();

            while (queueItem != null) {

                var msg = queueItem.getKey();
                var promise = queueItem.getValue();

                ReferenceCountUtil.release(msg);
                promise.setSuccess();

                queueItem = state.refreshQueue.poll();
            }

            state.refreshQueue = null;
        }

        if (state.refreshPromise != null) {

            if (!state.refreshPromise.isDone())
                state.refreshPromise.cancel(false);

            state.refreshPromise = null;
        }
    }

    private void checkAuthentication(HttpRequest request) {

        // Look for an existing session token in the request
        // If the token gives a valid session then authentication has succeeded

        log.info("Check auth for: {}", request.uri());

        var headers = request.headers();
        var token = HttpAuthHelpers.findTracAuthToken(headers);
        var session = token != null ? jwtValidator.decodeAndValidate(token) : null;

        if (session != null && session.isValid()) {
            state.authenticated = true;
            state.token = token;
            state.session = session;
            return;
        }

        for (var route : openRoutes) {

            var prefix = route.getKey();
            var allowOpen = route.getValue();

            if (request.uri().startsWith(prefix)) {
                state.authenticated = allowOpen;
                return;
            }
        }
    }

    void checkRefreshWanted() {

        if (state.session != null && state.session.isValid()) {

            var issueTime = state.session.getIssueTime();
            var refreshTime = issueTime.plusSeconds(configRefresh);

            if (state.requestTime.isAfter(refreshTime))
                state.refreshWanted = true;
        }
    }

    private void sendFailResponse(ChannelHandlerContext ctx, HttpRequest request) {

        if (HttpAuthHelpers.isBrowserRequest(request)) {

            var returnPath = URLEncoder.encode(request.uri(), StandardCharsets.US_ASCII);
            var redirectUri = String.format(browserLoginUri, returnPath);

            var redirectResponse = new DefaultFullHttpResponse(
                    request.protocolVersion(),
                    HttpResponseStatus.TEMPORARY_REDIRECT);

            redirectResponse.headers().set(HttpHeaderNames.LOCATION, redirectUri);

            ctx.writeAndFlush(redirectResponse);
        }
        else {

            var unauthorizedResponse = new DefaultFullHttpResponse(
                    request.protocolVersion(),
                    HttpResponseStatus.UNAUTHORIZED);

            ctx.writeAndFlush(unauthorizedResponse);
        }
    }

    private void sendRefreshRequest(ChannelHandlerContext ctx) {

        if (state.refreshPromise == null) {

            // Set up a promise for the token refresh response
            state.refreshPromise = ctx.newPromise();
            state.refreshPromise.addListener(future -> refreshComplete(ctx));

            // Set a short timeout on the refresh request
            // If the response doesn't come back quickly, the existing token is still good
            ctx.executor().schedule(() -> {
                if (state.refreshPromise != null && !state.refreshPromise.isDone()) {
                    log.warn("Token refresh did not complete in the allotted time (the previous token is still valid)");
                    state.refreshPromise.setSuccess();  // TODO
                }
            }, refreshTimeout.toMillis(), TimeUnit.MILLISECONDS);

            // Set up a queue to hold outbound messages until the refresh is ready
            state.refreshQueue = new ArrayDeque<>();
        }

        if (refreshChannel == null) {

            // If the client channel is not available, create it and call back into this function
            prepareRefreshChannel(ctx, () -> this.sendRefreshRequest(ctx));
        }
        else {

            // Channel is available - send a refresh request to the auth service
            // TODO: Request params
            var requestHeaders = new DefaultHttpHeaders();
            requestHeaders.add(HttpHeaderNames.AUTHORIZATION, state.token);

            log.info("Sending refresh request");

            var request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.GET,
                    "/login/refresh", Unpooled.EMPTY_BUFFER,
                    requestHeaders, new DefaultHttpHeaders());

            refreshChannel.pipeline().writeAndFlush(request);
        }
    }

    private void prepareRefreshChannel(ChannelHandlerContext ctx, Runnable sendRefreshRequest) {

        var channelClass = ctx.channel().getClass();
        var eventLoop = ctx.channel().eventLoop();
        var allocator = ctx.alloc();

        var refreshConnect = new Bootstrap()
                .group(eventLoop)
                .channel(channelClass)
                .option(ChannelOption.ALLOCATOR, allocator)
                .handler(new Http1RefreshInit())
                .connect("localhost", 8084);  // TODO

        refreshConnect.addListener(future -> {

            if (future.isSuccess()) {

                sendRefreshRequest.run();
            }
            else {

                if (!state.refreshPromise.isDone())
                    state.refreshPromise.setFailure(future.cause());

                refreshChannel.close();
                refreshChannel = null;
            }
        });

        refreshChannel = refreshConnect.channel();
    }

    private void refreshComplete(ChannelHandlerContext ctx) {

        if (state.refreshQueue != null) {

            var queueItem = state.refreshQueue.poll();

            while (queueItem != null) {

                var msg = queueItem.getKey();
                var promise = queueItem.getValue();

                ctx.pipeline().write(msg, promise);

                queueItem = state.refreshQueue.poll();
            }

            ctx.pipeline().flush();
        }

        state.refreshPromise = null;
        state.refreshQueue = null;
    }

    private class Http1RefreshInit extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel channel) {

            channel.pipeline().addLast(new Http1RefreshHandler());
            channel.pipeline().addLast(new HttpClientCodec());
        }
    }

    private class Http1RefreshHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {

            try {

                if (state.refreshPromise.isDone())
                    return;

                if (msg instanceof HttpResponse) {

                    var refreshResponse = (HttpResponse) msg;
                    var refreshToken = HttpAuthHelpers.findTracAuthToken(refreshResponse.headers());
                    var refreshSession = refreshToken != null ? jwtValidator.decodeAndValidate(refreshToken) : null;

                    if (refreshToken != null && refreshSession != null && refreshSession.isValid()) {

                        if (state.token == null || state.session == null || !state.session.isValid()) {
                            state.token = refreshToken;
                            state.session = refreshSession;
                        }
                        else if (refreshSession.getExpiryTime().isAfter(state.session.getExpiryTime())) {
                            state.token = refreshToken;
                            state.session = refreshSession;
                        }
                    }
                }

                if (msg instanceof LastHttpContent)
                    state.refreshPromise.setSuccess();
            }
            finally {
                ReferenceCountUtil.release(msg);
            }
        }
    }
}
