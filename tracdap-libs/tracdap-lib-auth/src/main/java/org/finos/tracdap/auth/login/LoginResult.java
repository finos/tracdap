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

package org.finos.tracdap.auth.login;

import org.finos.tracdap.common.auth.UserInfo;
import org.finos.tracdap.common.http.CommonHttpResponse;


public class LoginResult {

    private final LoginResultCode code;
    private final UserInfo userInfo;
    private final String message;
    private final CommonHttpResponse otherResponse;

    public static LoginResult AUTHORIZED(UserInfo userInfo) {
        return new LoginResult(LoginResultCode.AUTHORIZED, userInfo);
    }

    public static LoginResult FAILED() {
        return new LoginResult(LoginResultCode.FAILED, (String) null);
    }

    public static LoginResult FAILED(String message) {
        return new LoginResult(LoginResultCode.FAILED, message);
    }

    public static LoginResult OTHER_RESPONSE(CommonHttpResponse response) {
        return new LoginResult(LoginResultCode.OTHER_RESPONSE, response);
    }

    public static LoginResult NEED_CONTENT() {
        return new LoginResult(LoginResultCode.NEED_CONTENT, (UserInfo) null);
    }

    private LoginResult(LoginResultCode code, UserInfo userInfo) {
        this.code = code;
        this.userInfo = userInfo;
        this.message = null;
        this.otherResponse = null;
    }

    private LoginResult(LoginResultCode code, String message) {
        this.code = code;
        this.userInfo = null;
        this.message = message;
        this.otherResponse = null;
    }

    private LoginResult(LoginResultCode code, CommonHttpResponse otherResponse) {
        this.code = code;
        this.userInfo = null;
        this.message = null;
        this.otherResponse = otherResponse;
    }

    public LoginResultCode getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public UserInfo getUserInfo() {
        return userInfo;
    }

    public CommonHttpResponse getOtherResponse() {
        return otherResponse;
    }
}
