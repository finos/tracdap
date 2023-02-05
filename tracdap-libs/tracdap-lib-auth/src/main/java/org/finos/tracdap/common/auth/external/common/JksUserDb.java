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
import org.finos.tracdap.common.auth.external.UserDbRecord;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.ISecretLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JksUserDb implements IUserDatabase {

    private static final String DISPLAY_NAME_ATTR = "displayName";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ISecretLoader secretLoader;

    public JksUserDb(ConfigManager configManager) {

        log.info("Using JKS user database");

        this.secretLoader = configManager.getUserDb();
    }

    @Override
    public UserDbRecord getUserDbRecord(String userId) {

        if (log.isDebugEnabled())
            log.debug("Getting user DB record for {}", userId);

        if (!secretLoader.hasSecret(userId))
            return null;

        var passwordHash = secretLoader.loadPassword(userId);

        var userName = secretLoader.hasAttr(userId, DISPLAY_NAME_ATTR)
                ? secretLoader.loadAttr(userId, DISPLAY_NAME_ATTR)
                : userId;

        return new UserDbRecord(userId, userName, passwordHash);
    }
}
