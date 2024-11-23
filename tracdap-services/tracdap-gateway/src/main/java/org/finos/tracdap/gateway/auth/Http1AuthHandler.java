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

import org.finos.tracdap.common.auth.internal.Http1AuthValidator;
import org.finos.tracdap.common.auth.internal.HttpAuthHelpers;
import org.finos.tracdap.common.auth.internal.JwtValidator;
import org.finos.tracdap.config.AuthenticationConfig;

import io.netty.handler.codec.http.*;


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
public class Http1AuthHandler extends Http1AuthValidator {

    public Http1AuthHandler(AuthenticationConfig authConfig, JwtValidator jwtValidator) {
        super(authConfig, jwtValidator);
    }

    @Override
    protected boolean allowUnauthenticated(HttpRequest request) {

        if (request.uri().startsWith(StaticAuthConfig.AUTH_SERVICE_PATH))
            return true;

        return super.allowUnauthenticated(request);
    }

    @Override
    protected FullHttpResponse customResponse(HttpRequest request) {

        if (HttpAuthHelpers.isBrowserRequest(request)) {

            var redirect = new DefaultFullHttpResponse(
                    request.protocolVersion(),
                    HttpResponseStatus.TEMPORARY_REDIRECT);

            redirect.headers().set(HttpHeaderNames.LOCATION, StaticAuthConfig.BROWSER_LOGIN_PATH);

            return redirect;
        }

        return super.customResponse(request);
    }
}
