/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.auth.internal;

import org.finos.tracdap.common.auth.AuthConstants;
import org.finos.tracdap.common.auth.AuthHelpers;
import org.finos.tracdap.common.auth.UserInfo;
import org.finos.tracdap.config.AuthenticationConfig;

import io.grpc.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InternalAuthValidator implements ServerInterceptor {

    private static final String AUTH_DISABLED_USER_ID = "no_auth";
    private static final String AUTH_DISABLED_USER_NAME = "Authentication Disabled";

    private static final Logger log = LoggerFactory.getLogger(InternalAuthValidator.class);

    private final AuthenticationConfig authConfig;
    private final JwtValidator jwt;

    public InternalAuthValidator(AuthenticationConfig authConfig, JwtValidator jwt) {
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

            log.warn("AUTHENTICATE: {}() AUTHENTICATION DISABLED", call.getMethodDescriptor().getBareMethodName());

            var userInfo = new UserInfo();
            userInfo.setUserId(AUTH_DISABLED_USER_ID);
            userInfo.setDisplayName(AUTH_DISABLED_USER_NAME);

            var ctx = Context.current()
                    .withValue(AuthConstants.TRAC_AUTH_USER_KEY, userInfo);

            return Contexts.interceptCall(ctx, call, headers, next);
        }

        var token = headers.get(AuthConstants.TRAC_AUTH_TOKEN_KEY);

        if (token == null) {

            var message = "No authentication provided";

            log.error("AUTHENTICATE: {}() [{}] FAILED ",
                    call.getMethodDescriptor().getBareMethodName(),
                    message);

            var status = Status.UNAUTHENTICATED.withDescription(message);
            var trailers = new Metadata();

            call.close(status, trailers);
            return new ServerCall.Listener<>() {};
        }

        var sessionInfo = jwt.decodeAndValidate(token);

        // If the auth session is invalid, this is still an UNAUTHENTICATED condition
        // PERMISSION_DENIED would be if auth succeeds, but roles / permissions deny access to resources

        if (!sessionInfo.isValid()) {

            log.error("AUTHENTICATE: {}() [{}] FAILED",
                    call.getMethodDescriptor().getBareMethodName(),
                    sessionInfo.getErrorMessage());

            var status = Status.UNAUTHENTICATED.withDescription(sessionInfo.getErrorMessage());
            var trailers = new Metadata();

            call.close(status, trailers);
            return new ServerCall.Listener<>() {};
        }

        // Authentication succeeded!
        // Put the user info object into the context to make it available in the service implementation

        var userInfo = sessionInfo.getUserInfo();
        var delegate = sessionInfo.getDelegate();

        if (authConfig.getDisableSigning()) {
            log.warn("AUTHENTICATE: {}() [{}] SUCCEEDED WITHOUT VALIDATION",
                    call.getMethodDescriptor().getBareMethodName(),
                    AuthHelpers.printUserInfoWithDelegate(userInfo, delegate));
        }
        else {
            log.info("AUTHENTICATE: {}() [{}] SUCCEEDED",
                    call.getMethodDescriptor().getBareMethodName(),
                    AuthHelpers.printUserInfoWithDelegate(userInfo, delegate));
        }

        // Auth complete, put details into the current call context

        var ctx = Context.current()
                .withValue(AuthConstants.TRAC_AUTH_USER_KEY, userInfo)
                .withValue(AuthConstants.TRAC_DELEGATE_KEY, delegate);

        return Contexts.interceptCall(ctx, call, headers, next);
    }
}
