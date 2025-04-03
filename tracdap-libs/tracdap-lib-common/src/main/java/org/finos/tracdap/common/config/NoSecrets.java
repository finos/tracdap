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

package org.finos.tracdap.common.config;

import org.finos.tracdap.common.exception.EConfig;

import java.security.PrivateKey;
import java.security.PublicKey;


class NoSecrets implements ISecretLoader, ISecretService {

    // Null implementation of secret loader / service interface
    // Intended for a dev / local configuration that is not using secrets
    // Any attempt to store / access secrets will result in an error

    public static final String NO_SECRETS = "No secret handling mechanism has been configured";

    @Override
    public void init(ConfigManager configManager) {
        // No-op
    }

    @Override
    public void init(ConfigManager configManager, boolean createIfMissing) {
        // No-op
    }

    @Override
    public void reload() {
        // No-op
    }

    @Override
    public ISecretService scope(String scope) {
        return this;
    }

    @Override
    public boolean hasSecret(String secretName) {
        return false;
    }

    @Override
    public boolean hasAttr(String secretName, String attrName) {
        return false;
    }

    @Override
    public String loadPassword(String secretName) {
        throw new EConfig(NO_SECRETS);
    }

    @Override
    public String loadAttr(String secretName, String attrName) {
        throw new EConfig(NO_SECRETS);
    }

    @Override
    public PublicKey loadPublicKey(String secretName) {
        throw new EConfig(NO_SECRETS);
    }

    @Override
    public PrivateKey loadPrivateKey(String secretName) {
        throw new EConfig(NO_SECRETS);
    }

    @Override
    public String storePassword(String secretName, String password) {
        throw new EConfig(NO_SECRETS);
    }

    @Override
    public void deleteSecret(String secretName) {
        throw new EConfig(NO_SECRETS);
    }

    @Override
    public void commit() {
        throw new EConfig(NO_SECRETS);
    }
}
