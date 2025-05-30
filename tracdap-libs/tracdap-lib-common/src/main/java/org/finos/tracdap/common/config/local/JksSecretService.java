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

import org.finos.tracdap.common.config.*;
import org.finos.tracdap.common.exception.EConfigLoad;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.startup.StartupLog;

import org.slf4j.event.Level;

import java.io.IOException;
import java.net.URI;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Properties;
import java.util.Random;


public class JksSecretService extends JksSecretLoader implements ISecretService {

    private final Random random;

    public JksSecretService(Properties properties) {
        super(properties);
        this.random = new Random(System.currentTimeMillis());
    }

    @Override
    public void init(ConfigManager configManager, boolean createIfMissing) {

        if (ready) {
            StartupLog.log(this, Level.ERROR, "JKS secret service initialized twice");
            throw new EStartup("JKS secret service initialized twice");
        }

        if (configManager.hasConfig(keystoreUrl) || !createIfMissing) {

            init(configManager);
            return;
        }

        try {

            this.configManager = configManager;

            this.keystore.load(null, keystoreKey.toCharArray());
            this.commit();

            this.ready = true;
        }
        catch (IOException e) {
            // Inner error is more meaningful if keystore cannot be read
            var error = e.getCause() != null ? e.getCause() : e;
            var message = String.format("Failed to create keystore [%s]: %s", keystoreUrl, error.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }
        catch (NoSuchAlgorithmException | CertificateException e) {
            var message = String.format("Failed to open keystore [%s]: %s", keystoreUrl, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }
    }

    @Override
    public ISecretService scope(String scope) {
        return ScopedSecretService.rootScope(this).scope(scope);
    }

    @Override
    public String storePassword(String secretName, String password) {

        try {
            JksHelpers.writeTextEntry(keystore, keystoreKey, secretName, password);
            return secretName;
        }
        catch (EConfigLoad e) {
            var message = String.format("Password could not be saved to the key store: [%s] %s", secretName, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EConfigLoad(message, e);
        }
    }

    @Override
    public void deleteSecret(String secretName) {

        try {
            JksHelpers.deleteEntry(keystore, secretName);
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

            // Avoid conflicts on temporary files used for updates
            var tempSuffix = "~upd." + Math.abs(random.nextLong());
            var tempPath = keystorePath.getPath() + tempSuffix;

            try (var stream = LocalConfigLock.exclusiveWriteStream(tempPath)) {
                keystore.store(stream, keystoreKey.toCharArray());
            }

            // Exclusive move locks on the target file
            LocalConfigLock.exclusiveMove(tempPath, keystorePath.getPath());
        }
        catch (IOException e) {
            // Inner error is more meaningful if keystore cannot be read
            var error = e.getCause() != null ? e.getCause() : e;
            var errorDetail = error.getMessage() + " (this normally means the secret key is wrong)";
            var message = String.format("Failed to save keystore [%s]: %s", keystoreUrl, errorDetail);
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message, e);
        }
        catch (NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            var message = String.format("Failed to save keystore [%s]: %s", keystoreUrl, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message, e);
        }
    }
}
