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

package org.finos.tracdap.common.auth.external.common;

import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.config.CryptoHelpers;
import org.finos.tracdap.common.config.ISecretLoader;
import org.slf4j.Logger;

public class LocalUsers {

    private static final String DISPLAY_NAME_ATTR = "displayName";

    static boolean checkPassword(ISecretLoader userDb, String user, String pass, Logger log) {

        if (!userDb.hasSecret(user)) {

            log.warn("AUTHENTICATION: Failed [{}] user not found", user);
            return false;
        }

        var passwordHash = userDb.loadPassword(user);
        var passwordOk = CryptoHelpers.validateSSHA512(passwordHash, pass);

        if (passwordOk)
            log.info("AUTHENTICATION: Succeeded [{}]", user);
        else
            log.warn("AUTHENTICATION: Failed [{}] wrong password", user);

        return passwordOk;
    }

    static UserInfo getUserInfo(ISecretLoader userDb, String user) {

        var displayName = userDb.hasAttr(user, DISPLAY_NAME_ATTR)
                ? userDb.loadAttr(user, DISPLAY_NAME_ATTR)
                : user;

        var userInfo = new UserInfo();
        userInfo.setUserId(user);
        userInfo.setDisplayName(displayName);

        return userInfo;
    }
}
