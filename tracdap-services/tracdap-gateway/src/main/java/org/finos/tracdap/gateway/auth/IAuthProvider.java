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

package org.finos.tracdap.gateway.auth;

import org.finos.tracdap.common.auth.JwtProcessor;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;


public interface IAuthProvider {

    /**
     * Start a new authentication workflow, assuming no prior details
     *
     * <p>If the auth provider can authenticate directly using only data in the
     * incoming request, it should create and return and encoded JWT token.</p>
     *
     * <p>If an asynchronous workflow is required, the provider should make any
     * required requests and return null. The provider is also responsible for
     * responding to the client, e.g. with REDIRECT or UNAUTHORIZED.</p>
     *
     * <p>If authentication fails, the provider should write an error message
     * to the client and return null.</p>
     *
     * @param ctx Netty channel handler context
     * @param req Incoming HTTP/1 request to be authenticated
     * @param jwtProcessor JWT signing and validation, encoding and decoding
     *
     * @return A JWT token, or null if the authentication workflow is deferred or failed
     */
    String newAuth(ChannelHandlerContext ctx, HttpRequest req, JwtProcessor jwtProcessor);

    /**
     * Translate credentials from an alternate authorization format
     *
     * <p>If the authorization header / cookie / metadata contains something other than a
     * JWT token, the auth provider is given a chance to translate it. This can happen
     * synchronously or asynchronously.</p>
     *
     * <p>If the auth provider can translate the data in authInfo directly using only
     * the information it has available, it should create and return and encoded JWT token.</p>
     *
     * <p>If an asynchronous workflow is required, the provider should make any
     * required requests and return null. The provider is also responsible for
     * responding to the client, e.g. with REDIRECT or UNAUTHORIZED.</p>
     *
     * <p>If authentication fails, the provider should write an error message
     * to the client and return null.</p>
     *
     * @param ctx Netty channel handler context
     * @param req Incoming HTTP/1 request to be authenticated
     * @param authInfo Contents of the authentication header / cookie / metadata field
     * @param jwtProcessor JWT signing and validation, encoding and decoding
     *
     * @return A JWT token, or null if the authentication workflow is deferred or failed
     */
    String translateAuth(ChannelHandlerContext ctx, HttpRequest req, String authInfo, JwtProcessor jwtProcessor);
}
