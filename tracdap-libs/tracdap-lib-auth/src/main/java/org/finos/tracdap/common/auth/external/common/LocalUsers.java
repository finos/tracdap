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

import org.finos.tracdap.common.auth.external.IUserDatabase;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.config.CryptoHelpers;
import org.finos.tracdap.common.exception.EAuthorization;
import org.slf4j.Logger;

public class LocalUsers {

    static boolean checkPassword(IUserDatabase userDb, String user, String pass, Logger log) {

        var userDbRecord = userDb.getUserDbRecord(user);

        if (userDbRecord == null) {

            log.warn("AUTHENTICATION: Failed [{}] user not found", user);
            return false;
        }

        var passwordOk = CryptoHelpers.validateSSHA512(userDbRecord.getPasswordHash(), pass);

        if (passwordOk)
            log.info("AUTHENTICATION: Succeeded [{}]", user);
        else
            log.warn("AUTHENTICATION: Failed [{}] wrong password", user);

        return passwordOk;
    }

    static UserInfo getUserInfo(IUserDatabase userDb, String user) {

        var userDbRecord = userDb.getUserDbRecord(user);

        if (userDbRecord == null)
            throw new EAuthorization(String.format("Unknown user [%s]", user));

        var userInfo = new UserInfo();
        userInfo.setUserId(userDbRecord.getUserId());
        userInfo.setDisplayName(userDbRecord.getUserName());

        return userInfo;
    }
}
