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

import io.grpc.Context;
import io.grpc.Metadata;


public class AuthConstants {

    public static final String TRAC_AUTH_TOKEN = "trac_auth_token";
    public static final String TRAC_AUTH_USER = "trac_auth_user";
    public static final String TRAC_DELEGATE = "trac_delegate";

    // Restrict access to the gRPC context keys to the auth.internal package
    // Access is through AuthHelpers.currentUser() and AuthHelpers.currentSystemUser()

    static final Metadata.Key<String> TRAC_AUTH_TOKEN_KEY =
            Metadata.Key.of(TRAC_AUTH_TOKEN, Metadata.ASCII_STRING_MARSHALLER);

    static final Context.Key<UserInfo> TRAC_AUTH_USER_KEY =
            Context.key(TRAC_AUTH_USER);

    static final Context.Key<UserInfo> TRAC_DELEGATE_KEY =
            Context.key(TRAC_DELEGATE);
}
