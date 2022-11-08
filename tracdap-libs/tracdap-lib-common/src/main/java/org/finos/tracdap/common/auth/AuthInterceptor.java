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

import io.grpc.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.config.AuthenticationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AuthInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AuthInterceptor.class);

    private final JwtValidator jwt;

    public static AuthInterceptor setupAuth(AuthenticationConfig authConfig, ConfigManager configManager) {

        if (configManager.hasSecret(ConfigKeys.TRAC_AUTH_PUBLIC_KEY)) {

            var publicKey = configManager.loadPublicKey(ConfigKeys.TRAC_AUTH_PUBLIC_KEY);
            var jwt = JwtValidator.configure(authConfig, publicKey);

            return new AuthInterceptor(jwt);
        }
        else {

            // Allowing the service to run without validating authentication is hugely risky
            // Especially because claims can still be added to JWT
            // The auth-tool utility makes it really easy to set up auth keys on local JKS

            var error = "Root authentication keys are not available, the service will not start";
            log.error(error);
            throw new EStartup(error);
        }
    }

    AuthInterceptor(JwtValidator jwt) {
        this.jwt = jwt;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT>
    interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        // This is a basic check for valid credential, i.e. authentication only

        var token = headers.get(AuthConstants.AUTH_METADATA_KEY);

        if (token == null) {

            log.error("No authentication provided");

            var trailers = new Metadata();
            call.close(Status.UNAUTHENTICATED, trailers);
        }

        var sessionInfo = jwt.decodeAndValidate(token);

        // If the auth session is invalid, this is still an UNAUTHENTICATED condition
        // PERMISSION_DENIED would be if auth succeeds, but roles / permissions deny access to resources

        if (!sessionInfo.isValid()) {

            log.error("Authentication failed: {}", sessionInfo.getErrorMessage());

            var status = Status.UNAUTHENTICATED.withDescription(sessionInfo.getErrorMessage());
            var trailers = new Metadata();

            call.close(status, trailers);
        }

        // Authentication succeeded!
        // Put the user info object into the context to make it available in the service implementation

        var userInfo = sessionInfo.getUserInfo();
        var ctx = Context.current().withValue(AuthConstants.AUTH_CONTEXT_KEY, userInfo);

        return Contexts.interceptCall(ctx, call, headers, next);
    }
}
