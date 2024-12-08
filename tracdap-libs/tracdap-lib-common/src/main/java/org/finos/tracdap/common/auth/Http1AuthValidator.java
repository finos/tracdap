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

package org.finos.tracdap.common.auth;

import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.http.Http1Headers;
import org.finos.tracdap.common.util.LoggingHelpers;
import org.finos.tracdap.config.AuthenticationConfig;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;

import javax.annotation.Nonnull;


public class Http1AuthValidator extends ChannelInboundHandlerAdapter {

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log;

    private final AuthenticationConfig authConfig;
    private final JwtValidator jwtValidator;

    private boolean requestAuthenticated;

    public Http1AuthValidator(AuthenticationConfig authConfig, JwtValidator jwtValidator) {

        this.log = LoggingHelpers.threadLocalLogger(this, logMap);

        this.authConfig = authConfig;
        this.jwtValidator = jwtValidator;

        // Initial state
        requestAuthenticated = false;
    }

    /// Override this method to allow unauthenticated access for some requests, e.g. login requests
    @SuppressWarnings("unused")
    protected boolean allowUnauthenticated(HttpRequest request) {
        return false;
    }

    /// Override this method to customize the failure response, e.g. to redirect browsers to a login page
    @SuppressWarnings("unused")
    protected FullHttpResponse customResponse(HttpRequest request) {
        return null;
    }

    @Override final
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

        try {

            if (msg instanceof HttpRequest) {

                // Check authentication for each new request on the channel

                var request = (HttpRequest) msg;

                if (checkAuthentication(request)) {

                    // If the request is authorized, let it flow down the pipeline

                    requestAuthenticated = true;

                    ReferenceCountUtil.retain(msg);
                    ctx.fireChannelRead(msg);
                }
                else {

                    // If the request is not authorized, build a failure response to send to the client
                    // Messages for the incoming request will be discarded

                    requestAuthenticated = false;

                    var response = buildFailResponse(request);

                    ctx.write(response);
                    ctx.flush();
                    ctx.close();
                }
            }
            else if (msg instanceof HttpObject || msg instanceof WebSocketFrame) {

                // For other message types that are not new requests,
                // Refer to the authentication status of the most recent request message

                if (requestAuthenticated) {
                    ReferenceCountUtil.retain(msg);
                    ctx.fireChannelRead(msg);
                }

                // If the request is not authenticated a response is already sent
                // Discard any inbound data until the start of the next request
            }
            else
                throw new EUnexpected();
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override final
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        requestAuthenticated = false;

        ctx.fireExceptionCaught(cause);
    }

    private boolean checkAuthentication(HttpRequest request) {

        if (allowUnauthenticated(request)) {

            return true;
        }

        if (authConfig.getDisableAuth()) {

            log.warn("AUTHENTICATE: {} {} AUTHENTICATION DISABLED", request.method(), request.uri());
            return true;
        }

        // Look for an existing session token in the request
        // If the token gives a valid session then authentication has succeeded

        var headers = Http1Headers.wrapHttpHeaders(request.headers());
        var token = AuthHelpers.findTracAuthToken(headers, AuthHelpers.SERVER_COOKIE);

        if (token == null) {

            log.error("AUTHENTICATE: {} {} FAILED - No authentication provided", request.method(), request.uri());
            return false;
        }

        var session = jwtValidator.decodeAndValidate(token);

        if (!session.isValid()) {

            log.error("AUTHENTICATE: {} {} FAILED - {}", request.method(), request.uri(), session.getErrorMessage());
            return false;
        }
        else if (authConfig.getDisableSigning()) {

            log.warn("AUTHENTICATE: {} {} SUCCEEDED WITHOUT VALIDATION", request.method(), request.uri());
            return true;
        }
        else {

            log.info("AUTHENTICATE: {} {} SUCCEEDED", request.method(), request.uri());
            return true;
        }
    }

    private FullHttpResponse buildFailResponse(HttpRequest request) {

        var customResponse = customResponse(request);

        if (customResponse != null)
            return customResponse;

        return new DefaultFullHttpResponse(
                request.protocolVersion(),
                HttpResponseStatus.UNAUTHORIZED);
    }
}
