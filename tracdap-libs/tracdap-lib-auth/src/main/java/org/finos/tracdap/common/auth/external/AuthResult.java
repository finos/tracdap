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
    private final UserInfo userInfo;
    private final String message;
    private final AuthResponse otherResponse;

    public static AuthResult AUTHORIZED(UserInfo userInfo) {
        return new AuthResult(AuthResultCode.AUTHORIZED, userInfo);
    }

    public static AuthResult FAILED() {
        return new AuthResult(AuthResultCode.FAILED, (String) null);
    }

    public static AuthResult FAILED(String message) {
        return new AuthResult(AuthResultCode.FAILED, message);
    }

    public static AuthResult OTHER_RESPONSE(AuthResponse response) {
        return new AuthResult(AuthResultCode.OTHER_RESPONSE, response);
    }

    public static AuthResult NEED_CONTENT() {
        return new AuthResult(AuthResultCode.NEED_CONTENT, (UserInfo) null);
    }

    private AuthResult(AuthResultCode code, UserInfo userInfo) {
        this.code = code;
        this.userInfo = userInfo;
        this.message = null;
        this.otherResponse = null;
    }

    private AuthResult(AuthResultCode code, String message) {
        this.code = code;
        this.userInfo = null;
        this.message = message;
        this.otherResponse = null;
    }

    private AuthResult(AuthResultCode code, AuthResponse otherResponse) {
        this.code = code;
        this.userInfo = null;
        this.message = null;
        this.otherResponse = otherResponse;
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

    public AuthResponse getOtherResponse() {
        return otherResponse;
    }
}
