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

package org.finos.tracdap.common.auth;

import org.finos.tracdap.common.auth.internal.JwtValidator;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.config.AuthenticationConfig;

import io.grpc.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GrpcServerAuth implements ServerInterceptor {

    private static final String BEARER_AUTH_PREFIX = "bearer ";

    private static final String AUTH_DISABLED_USER_ID = "no_auth";
    private static final String AUTH_DISABLED_USER_NAME = "Authentication Disabled";

    private static final Logger log = LoggerFactory.getLogger(GrpcServerAuth.class);

    private final AuthenticationConfig authConfig;
    private final JwtValidator jwt;

    public GrpcServerAuth(AuthenticationConfig authConfig, JwtValidator jwt) {
        this.authConfig = authConfig;
        this.jwt = jwt;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT>
    interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        // This is a basic check for valid credential, i.e. authentication only

        if (authConfig.getDisableAuth()) {

            log.warn("AUTHENTICATE: DISABLED {}", call.getMethodDescriptor().getFullMethodName());

            var userInfo = new UserInfo();
            userInfo.setUserId(AUTH_DISABLED_USER_ID);
            userInfo.setDisplayName(AUTH_DISABLED_USER_NAME);

            var ctx = Context.current()
                    .withValue(AuthConstants.AUTH_TOKEN_KEY, "")
                    .withValue(AuthConstants.USER_INFO_KEY, userInfo);

            return Contexts.interceptCall(ctx, call, headers, next);
        }

        var token = headers.get(AuthConstants.AUTH_TOKEN_METADATA_KEY);

        if (token == null) {

            var message = "No authentication provided";

            log.error("AUTHENTICATE: FAILED {} [{}]",
                    call.getMethodDescriptor().getFullMethodName(),
                    message);

            var status = Status.UNAUTHENTICATED.withDescription(message);
            var trailers = new Metadata();

            call.close(status, trailers);
            return new ServerCall.Listener<>() {};
        }

        // Expect BEARER auth scheme, but also allow raw JSON tokens

        if (token.length() >= BEARER_AUTH_PREFIX.length()) {
            var prefix = token.substring(0, BEARER_AUTH_PREFIX.length());
            if (prefix.equalsIgnoreCase(BEARER_AUTH_PREFIX))
                token = token.substring(BEARER_AUTH_PREFIX.length());
        }

        var sessionInfo = jwt.decodeAndValidate(token);

        // If the auth session is invalid, this is still an UNAUTHENTICATED condition
        // PERMISSION_DENIED would be if auth succeeds, but roles / permissions deny access to resources

        if (!sessionInfo.isValid()) {

            log.error("AUTHENTICATE: FAILED {} [{}]",
                    call.getMethodDescriptor().getFullMethodName(),
                    sessionInfo.getErrorMessage());

            var status = Status.UNAUTHENTICATED.withDescription(sessionInfo.getErrorMessage());
            var trailers = new Metadata();

            call.close(status, trailers);
            return new ServerCall.Listener<>() {};
        }

        // Authentication succeeded!
        // Put the user info object into the context to make it available in the service implementation

        var userInfo = sessionInfo.getUserInfo();

        if (authConfig.getDisableSigning()) {
            log.warn("AUTHENTICATE: SUCCEEDED WITHOUT VALIDATION {} [{} <{}>]",
                    call.getMethodDescriptor().getFullMethodName(),
                    userInfo.getDisplayName(),
                    userInfo.getUserId());
        }
        else {
            log.info("AUTHENTICATE: SUCCEEDED {} [{} <{}>]",
                    call.getMethodDescriptor().getFullMethodName(),
                    userInfo.getDisplayName(),
                    userInfo.getUserId());
        }

        // Auth complete, put details into the current call context

        var ctx = Context.current()
                .withValue(AuthConstants.AUTH_TOKEN_KEY, token)
                .withValue(AuthConstants.USER_INFO_KEY, userInfo);

        return Contexts.interceptCall(ctx, call, headers, next);
    }
}
