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

import org.finos.tracdap.common.auth.internal.JwtProcessor;
import org.finos.tracdap.common.auth.internal.SessionInfo;
import org.finos.tracdap.config.AuthenticationConfig;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;


public class Http1AuthHandler extends ChannelDuplexHandler {


    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AuthenticationConfig authConfig;

    private final int connId;

    private final JwtProcessor jwtProcessor;
    private final IAuthProvider browserAuthProvider;
    private final IAuthProvider apiAuthProvider;

    private AuthResult authResult = AuthResult.FAILED();
    private SessionInfo session;
    private String token;
    private boolean wantCookies;

    public Http1AuthHandler(
            AuthenticationConfig authConfig, int connId,
            JwtProcessor jwtProcessor,
            IAuthProvider browserAuthProvider,
            IAuthProvider apiAuthProvider) {

        this.authConfig = authConfig;
        this.connId = connId;

        this.jwtProcessor = jwtProcessor;
        this.browserAuthProvider = browserAuthProvider;
        this.apiAuthProvider = apiAuthProvider;
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        try {
            // HTTP/1 auth works purely on the request object
            // Each new request will re-run the auth processing

            if ((msg instanceof HttpRequest)) {
                var request = (HttpRequest) msg;
                processAuthentication(ctx, request);
            }

            // If authorization failed a response has already been sent
            // Do not pass any further messages down the pipe

            if (authResult.getCode() != AuthResultCode.AUTHORIZED)
                return;

            // Authentication succeeded, allow messages to flow on the connection

            // Special handling for request objects, apply translation to the headers
            if (msg instanceof HttpRequest) {
                var request = (HttpRequest) msg;
                processRequest(ctx, request);
            }

            // Everything else flows straight through
            else {
                ReferenceCountUtil.retain(msg);
                ctx.fireChannelRead(msg);
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        try {

            // Authentication has always succeeded by this point
            // Otherwise no request is made to the platform, so no response would be sent

            // This does not account for pipelining, which is disabled by default in modern browsers

            // Special handling for response objects, apply translation to the headers
            if (msg instanceof HttpResponse) {
                var response = (HttpResponse) msg;
                processResponse(ctx, response, promise);
            }

            // Everything else flows straight through
            else {
                ReferenceCountUtil.retain(msg);
                ctx.write(msg, promise);
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void processAuthentication(ChannelHandlerContext ctx, HttpRequest request) {

        // Start the auth process by looking for the TRAC auth token
        // If there is already a valid session, this takes priority

        var headers = new Http1AuthHeaders(request.headers());

        log.info("Inbound auth headers: {}", request.headers().toString());

        token = AuthLogic.findTracAuthToken(headers, AuthLogic.SERVER_COOKIE);
        session = (token != null) ? jwtProcessor.decodeAndValidate(token) : null;

        // If the TRAC token is not available or not valid, fall back to a primary auth mechanism (if possible)

        // We want to know if this is request is for web browsing or an API call
        // It will affect how primary auth is done and how auth responses are sent back
        // This method is crude but effective, to be 100% we need the matcher used int the routers...

        var isApi =
                headers.contains(HttpHeaderNames.CONTENT_TYPE) &&
                headers.get(HttpHeaderNames.CONTENT_TYPE).startsWith("application/");

        if (session != null && session.isValid()) {
            authResult = AuthResult.AUTHORIZED(session.getUserInfo());
        }
        else if (isApi && apiAuthProvider != null) {
            authResult = apiAuthProvider.attemptAuth(ctx, headers);
        }
        else if (!isApi && browserAuthProvider != null) {
            authResult = browserAuthProvider.attemptAuth(ctx, headers);
        }
        else {
            authResult = AuthResult.FAILED();
        }

        // If necessary, create or update the user session depending on the auth result

        if (authResult.getCode() == AuthResultCode.AUTHORIZED) {

            // Session update is created for new sessions, or sessions that have ticket past their refresh time
            var sessionUpdate = (session == null)
                    ? AuthLogic.newSession(authResult.getUserInfo(), authConfig)
                    : AuthLogic.refreshSession(session, authConfig);

            // If the session has been updated, regenerate the token and store the new details
            if (sessionUpdate != session) {
                token = jwtProcessor.encodeToken(sessionUpdate);
                session = sessionUpdate;
            }
        }

        if (authResult.getCode() == AuthResultCode.FAILED) {

            // Basic unauthorized response
            // Could try to put the auth result message here maybe?

            var responseHeaders = new DefaultHttpHeaders();
            var response = new DefaultHttpResponse(
                    request.protocolVersion(),
                    HttpResponseStatus.UNAUTHORIZED,
                    responseHeaders);

            // Sending a response, so invert the auth direction

            ctx.write(response);
            ctx.flush();
        }

        // Decide whether to send the auth response as headers or cookies
        // Always send cookies for browser routes
        // For API routes the client can set a header to prefer cookies in the response

        wantCookies = !isApi || headers.contains(AuthLogic.TRAC_AUTH_COOKIES_HEADER);
    }

    private void processRequest(ChannelHandlerContext ctx, HttpRequest request) {

        var headers = new Http1AuthHeaders(request.headers());
        var emptyHeaders = new Http1AuthHeaders();

        var relayHeaders = AuthLogic.setPlatformAuthHeaders(headers, emptyHeaders, token);

        if (request instanceof FullHttpRequest) {

            var relayContent = ((FullHttpRequest) request).content().retain();

            var relayRequest = new DefaultFullHttpRequest(
                    request.protocolVersion(),
                    request.method(),
                    request.uri(),
                    relayContent,
                    relayHeaders.headers(),
                    new DefaultHttpHeaders());

            ctx.fireChannelRead(relayRequest);
        }
        else {

            var relayRequest = new DefaultHttpRequest(
                    request.protocolVersion(),
                    request.method(),
                    request.uri(),
                    relayHeaders.headers());

            ctx.fireChannelRead(relayRequest);
        }
    }

    private void processResponse(ChannelHandlerContext ctx, HttpResponse response, ChannelPromise promise) {

        var headers = new Http1AuthHeaders(response.headers());
        var emptyHeaders = new Http1AuthHeaders();

        var relayHeaders = AuthLogic.setClientAuthHeaders(headers, emptyHeaders, token, session, wantCookies);

        if (response instanceof FullHttpResponse) {

            var relayContent = ((FullHttpResponse) response).content().retain();

            var relayResponse = new DefaultFullHttpResponse(
                    response.protocolVersion(),
                    response.status(),
                    relayContent,
                    relayHeaders.headers(),
                    new DefaultHttpHeaders());

            ctx.write(relayResponse, promise);
        }
        else {

            var relayResponse = new DefaultHttpResponse(
                    response.protocolVersion(),
                    response.status(),
                    relayHeaders.headers());

            ctx.write(relayResponse, promise);
        }
    }
}
