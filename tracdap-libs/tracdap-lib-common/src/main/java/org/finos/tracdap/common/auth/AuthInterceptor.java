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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AuthInterceptor implements ServerInterceptor {

    private final Logger log = LoggerFactory.getLogger(getClass());

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

        var sessionInfo = JwtHelpers.decodeAndValidate(token);

        if (!sessionInfo.isValid()) {

            log.error("Authentication is not valid: {}", sessionInfo.getErrorMessage());

            var status = Status.PERMISSION_DENIED.withDescription(sessionInfo.getErrorMessage());
            var trailers = new Metadata();

            call.close(status, trailers);
        }

        var userInfo = sessionInfo.getUserInfo();
        var ctx = Context.current().withValue(AuthConstants.AUTH_CONTEXT_KEY, userInfo);

        return Contexts.interceptCall(ctx, call, headers, next);
    }
}
