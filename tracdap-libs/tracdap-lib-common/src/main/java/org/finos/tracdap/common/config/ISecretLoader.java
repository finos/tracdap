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

package org.finos.tracdap.common.config;


import org.finos.tracdap.common.exception.EStartup;

/**
 * Tech stack abstraction interface for loading secrets, certificates and other sensitive configuration items.
 *
 * <p>Cloud platforms provide dedicated services for managing, holding and retrieving secrets.
 * In traditional setups, secrets are managed using encrypted key store files.
 * The secret loader interface is an abstraction that allows sensitive configuration to be accessed
 * using the appropriate mechanism for a given deployment target. ConfigManager will select the
 * correct secret loader depending on the supplied configuration and available plugins.</p>
 *
 * <p>A secret loader that uses JKS files is included in the common library. Implementations for the
 * major cloud platforms are included in the cloud platform plugins.</p>
 *
 * @see ConfigManager
 */
public interface ISecretLoader {

    String SECRET_TYPE_PROP = "secret.type";
    String SECRET_KEY_PROP = "secret.key";

    /**
     * Initialize the secret loader.
     *
     * <p>This method is called before any secrets are requested.
     * If necessary, the supplied ConfigManager can be used to load supporting configuration,
     * such as encrypted key stores or other secret files.</p>
     *
     * @param configManager A ConfigManager instance that can be used to load supporting configuration
     */
    void init(ConfigManager configManager);

    /**
     * Load a secret as a text string.
     *
     * @param secretName The unique name of the secret to load
     * @return The decoded value of the secret, as a string
     * @throws EStartup There was a problem loading the secret
     */
    String loadTextSecret(String secretName);

    /**
     * Load a secret as a byte array
     *
     * @param secretName The unique name of the secret to load
     * @return The decoded value of the secret, in bytes
     * @throws EStartup There was a problem loading the secret
     */
    byte[] loadBinarySecret(String secretName);
}
