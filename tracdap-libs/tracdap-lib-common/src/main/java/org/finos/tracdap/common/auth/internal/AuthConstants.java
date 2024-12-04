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

package org.finos.tracdap.common.auth.internal;

import io.grpc.Context;
import io.grpc.Metadata;


public class AuthConstants {

    public static final String BEARER_PREFIX = "bearer ";

    public static final String TRAC_AUTH_TOKEN = "trac-auth-token";
    public static final String TRAC_AUTH_USER = "trac-auth-user";
    public static final String TRAC_DELEGATE = "trac-auth-delegate";

    public static final String TRAC_AUTH_TOKEN_HEADER = TRAC_AUTH_TOKEN;
    public static final String TRAC_AUTH_EXPIRY_HEADER = "trac-auth-expiry-utc";
    public static final String TRAC_AUTH_COOKIES_HEADER = "trac-auth-cookies";
    public static final String TRAC_USER_ID_HEADER = "trac-user-id";
    public static final String TRAC_USER_NAME_HEADER = "trac-user-name";

    // Restrict access to the gRPC context keys to the auth.internal package
    // Access is through AuthHelpers.currentUser() and AuthHelpers.currentSystemUser()

    static final Metadata.Key<String> TRAC_AUTH_TOKEN_KEY =
            Metadata.Key.of(TRAC_AUTH_TOKEN, Metadata.ASCII_STRING_MARSHALLER);

    static final Context.Key<UserInfo> TRAC_AUTH_USER_KEY =
            Context.key(TRAC_AUTH_USER);

    static final Context.Key<UserInfo> TRAC_DELEGATE_KEY =
            Context.key(TRAC_DELEGATE);
}
