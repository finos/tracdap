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

package org.finos.tracdap.common.config.local;

import org.finos.tracdap.common.config.CryptoHelpers;
import org.finos.tracdap.common.exception.EConfigLoad;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.config.ISecretService;
import org.finos.tracdap.common.config.ScopedSecretService;
import org.finos.tracdap.common.startup.StartupLog;
import org.slf4j.event.Level;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Properties;


public class JksSecretService extends JksSecretLoader implements ISecretService {

    public JksSecretService(Properties properties) {
        super(properties);
    }

    @Override
    public ISecretService scope(String scope) {
        return ScopedSecretService.rootScope(this).scope(scope);
    }

    @Override
    public String storePassword(String secretName, String password) {

        try {
            System.out.println("Saving secret: " + secretName);
            CryptoHelpers.writeTextEntry(keystore, secretKey, secretName, password);
            return secretName;
        }
        catch (EConfigLoad e) {
            var message = String.format("Password could not be saved to the key store: [%s] %s", secretName, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EConfigLoad(message, e);
        }
    }

    @Override
    public void commit() {

        try {

            var keystorePath = configManager.resolveConfigFile(URI.create(keystoreUrl));

            try (var stream = new FileOutputStream(keystorePath.getPath())) {
                keystore.store(stream, keystoreKey.toCharArray());
            }
        }
        catch (IOException e) {
            // Inner error is more meaningful if keystore cannot be read
            var error = e.getCause() != null ? e.getCause() : e;
            var errorDetail = error.getMessage() + " (this normally means the secret key is wrong)";
            var message = String.format("Failed to save keystore [%s]: %s", keystoreUrl, errorDetail);
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }
        catch (NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            var message = String.format("Failed to save keystore [%s]: %s", keystoreUrl, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }
    }
}
