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

package org.finos.tracdap.common.auth.login;

import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.finos.tracdap.common.auth.internal.JwtProcessor;
import org.finos.tracdap.common.auth.internal.SessionInfo;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.http.CommonHttpResponse;
import org.finos.tracdap.common.http.Http1Headers;
import org.finos.tracdap.common.http.CommonHttpRequest;
import org.finos.tracdap.config.AuthenticationConfig;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;


public class Http1LoginHandler extends ChannelInboundHandlerAdapter {

    private static final int PENDING_CONTENT_LIMIT = 64 * 1024;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AuthenticationConfig authConfig;
    private final JwtProcessor jwtProcessor;
    private final ILoginProvider loginProvider;

    private final String defaultReturnPath;

    private HttpRequest pendingRequest;
    private CompositeByteBuf pendingContent;

    public Http1LoginHandler(
            AuthenticationConfig authConfig,
            JwtProcessor jwtProcessor,
            ILoginProvider loginProvider) {

        this.authConfig = authConfig;
        this.jwtProcessor = jwtProcessor;
        this.loginProvider = loginProvider;

        this.defaultReturnPath = authConfig.hasReturnPath()
                ? authConfig.getReturnPath()
                : "/";
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        try {

            // Some auth mechanisms require content as well as headers
            // These mechanisms set the result NEED_CONTENT, to trigger aggregation
            // Aggregated messages are fed through the normal flow once they are ready

            if (!(msg instanceof HttpObject))
                throw new EUnexpected();

            if (pendingContent != null)
                msg = handleAggregateContent(msg);

            if (msg instanceof HttpRequest) {

                var request = (HttpRequest) msg;
                var requestUri = URI.create(request.uri());

                if (requestUri.getPath().equals(LoginContent.LOGIN_URL))
                    processLogin(ctx, request);

                else if (requestUri.getPath().equals(LoginContent.REFRESH_URL))
                    processRefresh(ctx, request);

                else if (requestUri.getPath().startsWith(LoginContent.LOGIN_PATH_PREFIX))
                    serveStaticContent(ctx, request);

                else
                    serveNotFound(ctx, request);
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private Object handleAggregateContent(Object msg) {

        if (!(msg instanceof HttpContent) || pendingContent.readableBytes() > PENDING_CONTENT_LIMIT) {
            pendingContent.release();
            throw new EUnexpected();
        }

        var content = (HttpContent) msg;
        pendingContent.addComponent(content.content());
        pendingContent.writerIndex(pendingContent.writerIndex() + content.content().writerIndex());

        if (content instanceof LastHttpContent) {

            var fullRequest = new DefaultFullHttpRequest(
                    pendingRequest.protocolVersion(),
                    pendingRequest.method(),
                    pendingRequest.uri(),
                    pendingContent,
                    pendingRequest.headers(),
                    ((LastHttpContent) content).trailingHeaders());

            pendingRequest = null;
            pendingContent = null;

            return fullRequest;
        }

        return null;
    }

    private void processLogin(ChannelHandlerContext ctx, HttpRequest request) {

        // Only one auth provider available atm, for both browser and API routes

        var commonRequest = CommonHttpRequest.fromHttpRequest(request);
        var authResult = loginProvider.attemptLogin(commonRequest);

        switch (authResult.getCode()) {

            case AUTHORIZED:

                // If primary auth succeeded, set up the session token
                var session = AuthLogic.newSession(authResult.getUserInfo(), authConfig);
                var token = jwtProcessor.encodeToken(session);

                serveLoginOk(ctx, request, session, token);

                break;

            case FAILED:

                log.error("authentication failed ({})", authResult.getMessage());

                // Send a basic error response for authentication failures for now
                // If the result is REDIRECTED the auth provider already responded, so no need to respond again here
                var failedResponse = buildFailedResponse(request, authResult);
                ctx.writeAndFlush(failedResponse);
                ctx.close();

                break;

            case OTHER_RESPONSE:

                var otherResponse = buildOtherResponse(request, authResult);
                ctx.writeAndFlush(otherResponse);

                break;

            case NEED_CONTENT:

                pendingRequest = request;
                pendingContent = ByteBufAllocator.DEFAULT.compositeBuffer();

                break;

            default:
                throw new EUnexpected();
        }
    }

    private void processRefresh(ChannelHandlerContext ctx, HttpRequest request) {

        var headers = Http1Headers.fromHttpHeaders(request.headers());
        var token = AuthHelpers.findTracAuthToken(headers);
        var session = (token != null) ? jwtProcessor.decodeAndValidate(token) : null;

        if (session != null && session.isValid()) {

            var sessionUpdate = AuthLogic.refreshSession(session, authConfig);
            var tokenUpdate = jwtProcessor.encodeToken(sessionUpdate);

            serveLoginOk(ctx, request, sessionUpdate, tokenUpdate);
        }
        else if (AuthHelpers.isBrowserRequest(headers)) {

            var redirect = buildLoginRedirect(request);
            ctx.writeAndFlush(redirect);
        }
        else {

            var failedResponse = buildFailedResponse(request, AuthResult.FAILED());
            ctx.writeAndFlush(failedResponse);
            ctx.close();
        }
    }

    private void serveLoginOk(ChannelHandlerContext ctx, HttpRequest request, SessionInfo session, String token) {

        var requestHeaders = Http1Headers.fromHttpHeaders(request.headers());

        CommonHttpResponse content;
        Http1Headers headers;

        if (AuthHelpers.isBrowserRequest(requestHeaders)) {

            var uri = URI.create(request.uri());
            var query = uri.getQuery();

            var queryParams = query != null
                    ? Arrays.asList(query.split("&"))
                    : List.<String>of();

            var returnPath = queryParams.stream()
                    .filter(p -> p.startsWith("return-path="))
                    .findFirst()
                    .map(s -> s.substring(s.indexOf('=') + 1))
                    .map(s -> URLDecoder.decode(s, StandardCharsets.UTF_8))
                    .orElse(defaultReturnPath);

            content = LoginContent.getLoginOkPage(returnPath);
            headers = Http1Headers.fromGenericHeaders(content.headers());

            AuthHelpers.addClientAuthCookies(headers, token, session);
        }
        else {

            headers = new Http1Headers();
            content = new CommonHttpResponse(HttpResponseStatus.OK, headers, Unpooled.EMPTY_BUFFER);

            if (AuthHelpers.wantCookies(requestHeaders))
                AuthHelpers.addClientAuthCookies(headers, token, session);
            else
                AuthHelpers.addClientAuthHeaders(headers, token, session);
        }

        var response = new DefaultFullHttpResponse(
                request.protocolVersion(),
                content.status(),
                content.content(),
                headers.toHttpHeaders(),
                EmptyHttpHeaders.INSTANCE);

        ctx.writeAndFlush(response);
    }

    private void serveStaticContent(ChannelHandlerContext ctx, HttpRequest request) {

        var content = LoginContent.getStaticContent(request);
        var headers = Http1Headers.fromGenericHeaders(content.headers());

        var response = new DefaultFullHttpResponse(
                request.protocolVersion(),
                content.status(),
                content.content(),
                headers.toHttpHeaders(),
                EmptyHttpHeaders.INSTANCE);

        ctx.writeAndFlush(response);
    }

    private void serveNotFound(ChannelHandlerContext ctx, HttpRequest request) {

        var response = new DefaultFullHttpResponse(
                request.protocolVersion(),
                HttpResponseStatus.NOT_FOUND);

        ctx.writeAndFlush(response);
    }

    private FullHttpResponse buildLoginRedirect(HttpRequest request) {

        var status = LoginContent.LOGIN_REDIRECT_STATUS;
        var headers = new Http1Headers();
        headers.set(HttpHeaderNames.LOCATION, LoginContent.LOGIN_URL);

        return new DefaultFullHttpResponse(
                request.protocolVersion(), status,
                Unpooled.EMPTY_BUFFER,
                headers.toHttpHeaders(),
                EmptyHttpHeaders.INSTANCE);
    }

    private FullHttpResponse buildFailedResponse(HttpRequest request, AuthResult authResult) {

        var statusMessage = authResult.getMessage();
        var status = statusMessage != null
                ? HttpResponseStatus.valueOf(HttpResponseStatus.UNAUTHORIZED.code(), statusMessage)
                : HttpResponseStatus.UNAUTHORIZED;

        // Needs a real headers instance even if no headers are set
        // Otherwise encoded HTTP response will not be valid
        var headers = new Http1Headers();

        return new DefaultFullHttpResponse(
                request.protocolVersion(), status,
                Unpooled.EMPTY_BUFFER,
                headers.toHttpHeaders(),
                EmptyHttpHeaders.INSTANCE);
    }

    private FullHttpResponse buildOtherResponse(HttpRequest request, AuthResult authResult) {

        var content = authResult.getOtherResponse();
        var headers = Http1Headers.fromGenericHeaders(content.headers());

        return new DefaultFullHttpResponse(
                request.protocolVersion(),
                content.status(),
                content.content(),
                headers.toHttpHeaders(),
                EmptyHttpHeaders.INSTANCE);
    }
}
