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

import org.finos.tracdap.common.auth.internal.UserInfo;


public class AuthResult {

    private final AuthResultCode code;
    private final String message;
    private final UserInfo userInfo;

    public static AuthResult AUTHORIZED(UserInfo userInfo) {
        return new AuthResult(AuthResultCode.AUTHORIZED, userInfo);
    }

    public static AuthResult FAILED() {
        return new AuthResult(AuthResultCode.FAILED, (String) null);
    }

    public AuthResult(AuthResultCode code, UserInfo userInfo) {
        this.code = code;
        this.message = null;
        this.userInfo = userInfo;
    }

    public AuthResult(AuthResultCode code, String message) {
        this.code = code;
        this.message = message;
        this.userInfo = null;
    }

    public AuthResultCode getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public UserInfo getUserInfo() {
        return userInfo;
    }
}
