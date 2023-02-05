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

package org.finos.tracdap.tools.secrets;

import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.CryptoHelpers;
import org.finos.tracdap.common.exception.ETracPublic;
import org.finos.tracdap.config.GatewayConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Map;

public class JksUserManager implements IUserManager {

    private static final String DISPLAY_NAME_ATTR = "displayName";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ConfigManager configManager;

    public JksUserManager(ConfigManager configManager) {
        this.configManager = configManager;
        this.configManager.prepareSecrets();
    }

    @Override
    public void initTracUsers() {

        var config = configManager.loadRootConfigObject(GatewayConfig.class);
        var userDbType = config.getConfigOrThrow(ConfigKeys.USER_DB_TYPE);
        var userDbUrl = config.getConfigOrThrow(ConfigKeys.USER_DB_URL);
        var userDbSecret = config.getConfigOrThrow(ConfigKeys.USER_DB_KEY);
        var userDbKey = configManager.loadPassword(userDbSecret);

        var userDbPath = Paths.get(configManager.resolveConfigFile(URI.create(userDbUrl)));
        var userDb = JksHelpers.loadKeystore(userDbType, userDbPath, userDbKey, /* createIfMissing = */ true);
        JksHelpers.saveKeystore(userDbPath, userDbKey, userDb);
    }

    @Override
    public void addUser(String userId, String userName, String passwordHash) {

        try {

            var attrs = Map.of(DISPLAY_NAME_ATTR, userName);

            var userDb = loadUserDb();

            var config = configManager.loadRootConfigObject(GatewayConfig.class);
            var userDbSecret = config.getConfigOrThrow(ConfigKeys.USER_DB_KEY);
            var userDbKey = configManager.loadPassword(userDbSecret);

            CryptoHelpers.writeTextEntry(userDb, userDbKey, userId, passwordHash, attrs);

            saveUserDb(userDb);
        }
        catch (Exception e) {
            throw new ETracPublic("Error adding user: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteUser(String userId) {

        try {

            var userDb = loadUserDb();
            CryptoHelpers.deleteEntry(userDb, userId);
            saveUserDb(userDb);
        }
        catch (Exception e) {
            throw new ETracPublic("Error deleting user: " + e.getMessage(), e);
        }
    }


    private KeyStore loadUserDb() {

        var config = configManager.loadRootConfigObject(GatewayConfig.class);
        var userDbType = config.getConfigOrThrow(ConfigKeys.USER_DB_TYPE);
        var userDbUrl = config.getConfigOrThrow(ConfigKeys.USER_DB_URL);
        var userDbSecret = config.getConfigOrThrow(ConfigKeys.USER_DB_KEY);

        var userDbPath = Paths.get(configManager.resolveConfigFile(URI.create(userDbUrl)));
        var userDbKey = configManager.loadPassword(userDbSecret);

        return JksHelpers.loadKeystore(userDbType, userDbPath, userDbKey, false);
    }

    private void saveUserDb(KeyStore userDb) {

        var config = configManager.loadRootConfigObject(GatewayConfig.class);
        var userDbUrl = config.getConfigOrThrow(ConfigKeys.USER_DB_URL);
        var userDbSecret = config.getConfigOrThrow(ConfigKeys.USER_DB_KEY);

        var userDbPath = Paths.get(configManager.resolveConfigFile(URI.create(userDbUrl)));
        var userDbKey = configManager.loadPassword(userDbSecret);

        JksHelpers.saveKeystore(userDbPath, userDbKey, userDb);
    }
}
