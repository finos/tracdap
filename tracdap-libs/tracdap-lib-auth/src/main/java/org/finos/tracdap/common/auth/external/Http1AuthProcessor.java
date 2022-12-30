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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;

import io.netty.util.ReferenceCountUtil;
import org.finos.tracdap.common.auth.internal.JwtProcessor;
import org.finos.tracdap.common.auth.internal.SessionInfo;
import org.finos.tracdap.common.exception.EAuthorization;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.AuthenticationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;


public class Http1AuthProcessor extends ChannelDuplexHandler {

    public static final boolean FRONT_FACING = true;
    public static final boolean BACK_FACING = false;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AuthenticationConfig authConfig;
    private final boolean authDirection;

    private final int connId;

    private final JwtProcessor jwtProcessor;
    private final IAuthProvider browserAuthProvider;
    private final IAuthProvider apiAuthProvider;

    private AuthResult authResult = AuthResult.FAILED();
    private SessionInfo session;
    private String token;
    private boolean isApi;

    public Http1AuthProcessor(
            AuthenticationConfig authConfig,
            boolean direction,int connId,
            JwtProcessor jwtProcessor,
            IAuthProvider browserAuthProvider,
            IAuthProvider apiAuthProvider) {

        this.authConfig = authConfig;
        this.authDirection = direction;
        this.connId = connId;

        this.jwtProcessor = jwtProcessor;
        this.browserAuthProvider = browserAuthProvider;
        this.apiAuthProvider = apiAuthProvider;
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        bidiMessageHandler(ctx, msg, null, FRONT_FACING);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        bidiMessageHandler(ctx, msg, promise, BACK_FACING);
    }

    private void bidiMessageHandler(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, boolean msgDirection) {

        try {
            // HTTP/1 auth works purely on the request object

            if ((msg instanceof HttpRequest)) {
                var request = (HttpRequest) msg;
                processAuthentication(ctx, request);
                processRequest(ctx, request, promise);
            }

            // If authorization failed, drop messages silently after the request

            if (authResult.getCode() != AuthResultCode.AUTHORIZED)
                return;

            if (msg instanceof HttpResponse) {
                var response = (HttpResponse) msg;
                processResponse(ctx, response, promise);
            }

            // Content messages need special handling, to account for "full" HTTP request / response messages
            // These include the headers and body all in one message
            // Since the headers are already processed, we need to separate the body

            if (msg instanceof HttpContent) {

                var originalMsg = (HttpContent) msg;
                var content = originalMsg.content().retain();

                var relayMsg = (msg instanceof LastHttpContent)
                        ? new DefaultLastHttpContent(content)
                        : new DefaultHttpContent(content);

                bidiSend(ctx, relayMsg, promise, msgDirection);
            }

            // Any other message types are unexpected, are not impossible and should be allowed
            // E.g. signalling with custom events or control frames for alternate protocols
            // Anyway we know we have request, response and auth already

            else if (!(msg instanceof HttpRequest) && !(msg instanceof HttpResponse)) {

                bidiSend(ctx, msg, promise, msgDirection);
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void bidiSend(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, boolean direction) {

        if (direction == FRONT_FACING)
            ctx.fireChannelRead(msg);
        else
            ctx.write(msg, promise);
    }

    private void bidiFlush(ChannelHandlerContext ctx, boolean direction) {

        if (direction == FRONT_FACING)
            ctx.fireChannelReadComplete();
        else
            ctx.flush();
    }

    private void processAuthentication(ChannelHandlerContext ctx, HttpRequest request) {

        var headers = new Http1AuthHeaders(request.headers());

        // We want to know if this is request is for web browsing or an API call
        // It will affect how some responses get sent back, this method is crude but effective!
        // To be 100% we could pass in the matcher used by the routers.....

        if (headers.contains(HttpHeaderNames.CONTENT_TYPE)) {
            var contentType = headers.get(HttpHeaderNames.CONTENT_TYPE);
            isApi = contentType.startsWith("application/");
        }

        // Start the auth process by looking for the TRAC auth token
        // If there is already a valid session, this takes priority

        token = AuthLogic.findTracAuthToken(headers, AuthLogic.SERVER_COOKIE);
        session = (token != null) ? jwtProcessor.decodeAndValidate(token) : null;

        // If the TRAC token is not available or not valid, fall back to a primary auth mechanism (if possible)

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


    }

    private void processRequest(ChannelHandlerContext ctx, HttpRequest request, ChannelPromise promise) {

        switch (authResult.getCode()) {

            case AUTHORIZED:

                // Strip out any existing auth headers and other noise
                // This message is heading to the core platform, so it only needs the TRAC auth info

                var headers = new Http1AuthHeaders(request.headers());
                var emptyHeaders = new Http1AuthHeaders();

                var updatedHeaders = AuthLogic.updateAuthHeaders(
                        headers, emptyHeaders, token, session,
                        RouteType.PLATFORM_ROUTE, AuthLogic.SERVER_COOKIE);

                var updatedRequest = new DefaultHttpRequest(
                        request.protocolVersion(), request.method(),
                        request.uri(), updatedHeaders.headers());

                // For front-facing handlers requests come on the read side, back-facing is reversed

                bidiSend(ctx, updatedRequest, promise, authDirection);

                break;

            case REDIRECTED:

                // primary auth provider already responded to the client
                // We don't need to do anything, just drop the messages

                // What should we do with the promise on a back-facing channel?
                // It should fail, but if the user is redirecting, we don't want to crash and hang up the connection!

                if (authDirection == BACK_FACING)
                    promise.setFailure(new EAuthorization("Authorization in progress, please retry after"));

                break;

            case FAILED:

                // Basic unauthorized response
                // Could try to put the auth result message here maybe?

                var responseHeaders = new DefaultHttpHeaders();
                var response = new DefaultHttpResponse(
                        request.protocolVersion(),
                        HttpResponseStatus.UNAUTHORIZED,
                        responseHeaders);

                bidiSend(ctx, response, promise, authDirection);
                bidiFlush(ctx, authDirection);

                break;

            default:

                throw new EUnexpected();
        }
    }

    private void processResponse(ChannelHandlerContext ctx, HttpResponse response, ChannelPromise promise) {

        // First filter out any auth-related headers from the response

        var headers = new Http1AuthHeaders(response.headers());
        var emptyHeaders = new Http1AuthHeaders();
        var routeType = isApi ? RouteType.API_ROUTE : RouteType.BROWSER_ROUTE;

        var updatedHeaders = AuthLogic.updateAuthHeaders(
                headers, emptyHeaders, token, session,
                routeType, AuthLogic.CLIENT_COOKIE);

        var updatedResponse = new DefaultHttpResponse(
                response.protocolVersion(),
                response.status(), updatedHeaders.headers());

        // This is a response message, so invert the direction flag !!

        bidiSend(ctx, updatedResponse, promise, !authDirection);
    }
}
