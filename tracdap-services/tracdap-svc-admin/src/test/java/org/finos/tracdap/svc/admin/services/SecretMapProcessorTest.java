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

package org.finos.tracdap.svc.admin.services;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.ISecretService;
import org.finos.tracdap.common.exception.EConfigParse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


public class SecretMapProcessorTest {

    private FakeSecretService secrets;

    @BeforeEach
    void setup() {
        secrets = new FakeSecretService();
    }

    @Test
    void newSecretIsStoredAndAliased() {

        var newSecrets = Map.of("clientSecret", "raw-value");
        var oldSecrets = Map.<String, String>of();

        var secretsUpdated = new SimpleResult<Boolean>();
        var result = SecretMapProcessor.processSecrets(newSecrets, oldSecrets, secrets, secretsUpdated);

        assertEquals("raw-value", secrets.stored.get("clientSecret"));
        assertEquals("clientSecret", result.get("clientSecret"));
        assertTrue(secretsUpdated.getResult());
        assertTrue(secrets.committed);
    }

    @Test
    void blankSecretCarriesOverExistingAlias() {

        var newSecrets = Map.of("clientSecret", "");
        var oldSecrets = Map.of("clientSecret", "clientSecret");

        secrets.stored.put("clientSecret", "existing-value");

        var secretsUpdated = new SimpleResult<Boolean>();
        var result = SecretMapProcessor.processSecrets(newSecrets, oldSecrets, secrets, secretsUpdated);

        assertEquals("clientSecret", result.get("clientSecret"));
        assertEquals("existing-value", secrets.stored.get("clientSecret"));
        assertFalse(secretsUpdated.getResult());
    }

    @Test
    void blankSecretWithNoPriorValueFails() {

        var newSecrets = Map.of("clientSecret", "");
        var oldSecrets = Map.<String, String>of();

        var secretsUpdated = new SimpleResult<Boolean>();

        assertThrows(EConfigParse.class, () ->
                SecretMapProcessor.processSecrets(newSecrets, oldSecrets, secrets, secretsUpdated));
    }

    @Test
    void removedSecretIsDeletedFromStore() {

        var newSecrets = Map.<String, String>of();

        // The stored alias differs from the map key, as it would for a real scoped secret service -
        // deleteSecret() must be called with the key, not the alias, or it deletes the wrong entry
        var oldSecrets = Map.of("clientSecret", "unrelated-alias-value");

        secrets.stored.put("clientSecret", "existing-value");

        var secretsUpdated = new SimpleResult<Boolean>();
        var result = SecretMapProcessor.processSecrets(newSecrets, oldSecrets, secrets, secretsUpdated);

        assertTrue(result.isEmpty());
        assertFalse(secrets.stored.containsKey("clientSecret"));
        assertTrue(secretsUpdated.getResult());
        assertTrue(secrets.committed);
    }

    @Test
    void noChangeLeavesSecretsUpdatedFalseAndSkipsCommit() {

        var newSecrets = Map.of("clientSecret", "");
        var oldSecrets = Map.of("clientSecret", "clientSecret");

        secrets.stored.put("clientSecret", "existing-value");

        var secretsUpdated = new SimpleResult<Boolean>();
        SecretMapProcessor.processSecrets(newSecrets, oldSecrets, secrets, secretsUpdated);

        assertFalse(secretsUpdated.getResult());
        assertFalse(secrets.committed);
    }

    private static class FakeSecretService implements ISecretService {

        final Map<String, String> stored = new HashMap<>();
        boolean committed = false;

        @Override
        public void init(ConfigManager configManager, boolean createIfMissing) {}

        @Override
        public ISecretService scope(String scope) { return this; }

        @Override
        public String storePassword(String secretName, String password) {
            stored.put(secretName, password);
            return secretName;
        }

        @Override
        public void deleteSecret(String secretName) { stored.remove(secretName); }

        @Override
        public void commit() { committed = true; }

        @Override
        public void init(ConfigManager configManager) {}

        @Override
        public void reload() {}

        @Override
        public boolean hasSecret(String secretName) { return stored.containsKey(secretName); }

        @Override
        public String loadPassword(String secretName) { return stored.get(secretName); }

        @Override
        public PublicKey loadPublicKey(String secretName) { throw new UnsupportedOperationException(); }

        @Override
        public PrivateKey loadPrivateKey(String secretName) { throw new UnsupportedOperationException(); }
    }
}
