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

import org.finos.tracdap.common.config.ISecretService;
import org.finos.tracdap.common.exception.EConfigParse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * Shared secret create/carry-over/cleanup logic for any config object's {@code secrets} map,
 * used by both tenant-scoped {@link ConfigService} (for {@code ResourceDefinition}) and
 * tenant-less {@link PlatformConfigService} (for {@code CredentialDefinition}).
 */
class SecretMapProcessor {

    private static final Logger log = LoggerFactory.getLogger(SecretMapProcessor.class);

    static Map<String, String> processSecrets(
            Map<String, String> newSecrets, Map<String, String> oldSecrets,
            ISecretService secrets, SimpleResult<Boolean> secretsUpdated) {

        var secureSecrets = new HashMap<String, String>();

        for (var secret : newSecrets.entrySet()) {

            var secretKey = secret.getKey();
            var secretValue = secret.getValue();

            if (secretValue != null && !secretValue.isEmpty()) {

                // Secret value is supplied - update the secret store
                var secretAlias = secrets.storePassword(secretKey, secretValue);
                secureSecrets.put(secretKey, secretAlias);

                if (!secretsUpdated.isDone())
                    secretsUpdated.setResult(true);
            }
            else if (oldSecrets.containsKey(secretKey)) {

                // If secret value is blank and a previous version exists, carry the secret over
                secureSecrets.put(secretKey, oldSecrets.get(secretKey));
            }
            else {

                // Do not allow setting blank secrets
                var message = String.format("No value supplied for config secret [%s]", secretKey);
                log.error(message);
                throw new EConfigParse(message);
            }
        }

        for (var secretKey : oldSecrets.keySet()) {
            if (!newSecrets.containsKey(secretKey)) {

                // Delete any secrets that have been removed since the prior version
                // Use the map key, not the stored alias - deleteSecret() re-scopes its argument,
                // and the stored alias is already scoped (from a prior storePassword() call)
                secrets.deleteSecret(secretKey);

                if (!secretsUpdated.isDone())
                    secretsUpdated.setResult(true);
            }
        }

        if (secretsUpdated.isDone())
            secrets.commit();
        else
            secretsUpdated.setResult(false);

        return secureSecrets;
    }
}
