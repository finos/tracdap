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
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.startup.StartupLog;
import org.slf4j.event.Level;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Properties;


public class JksSecretLoader implements ISecretLoader {

    public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

    protected final Properties properties;
    protected final String keystoreType;
    protected final String keystoreUrl;
    protected final String keystoreKey;
    protected final KeyStore keystore;

    protected ConfigManager configManager;
    protected boolean ready;

    public JksSecretLoader(Properties properties) {

        this.properties = properties;
        this.keystoreType = properties.getProperty(ConfigKeys.SECRET_TYPE_KEY, DEFAULT_KEYSTORE_TYPE);
        this.keystoreUrl = properties.getProperty(ConfigKeys.SECRET_URL_KEY);
        this.keystoreKey = properties.getProperty(ConfigKeys.SECRET_KEY_KEY);

        StartupLog.log(this, Level.INFO, "Initializing JKS secret loader...");

        if (keystoreUrl == null || keystoreUrl.isBlank()) {
            var message = String.format("JKS secrets need %s in the main config file", ConfigKeys.SECRET_URL_KEY);
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }

        if (keystoreKey == null || keystoreKey.isBlank()) {
            var template = "JKS secrets need a secret key, use --secret-key or set %s in the environment";
            var message = String.format(template, ConfigKeys.SECRET_KEY_ENV);
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }

        try {
            this.keystore = KeyStore.getInstance(keystoreType);
        }
        catch (KeyStoreException e) {
            var message = String.format("Keystore type is not supported: [%s]", keystoreType);
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }

        this.configManager = null;
        this.ready = false;
    }

    @Override
    public void init(ConfigManager configManager) {

        if (ready) {
            StartupLog.log(this, Level.ERROR, "JKS secret loader initialized twice");
            throw new EStartup("JKS secret loader initialized twice");
        }

        this.configManager = configManager;

        reload();
    }

    @Override
    public void reload() {

        try {

            var keystoreBytes = configManager.loadBinaryConfig(keystoreUrl);

            try (var stream = new ByteArrayInputStream(keystoreBytes)) {

                keystore.load(stream, keystoreKey.toCharArray());
                ready = true;
            }
        }
        catch (IOException e) {
            // Inner error is more meaningful if keystore cannot be read
            var error = e.getCause() != null ? e.getCause() : e;
            var errorDetail = error.getMessage() + " (this normally means the secret key is wrong)";
            var message = String.format("Failed to open keystore [%s]: %s", keystoreUrl, errorDetail);
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
    public ISecretLoader scope(String scope) {
        return ScopedSecretLoader.rootScope(this).scope(scope);
    }

    @Override
    public boolean hasSecret(String secretName) {

        try {
            return CryptoHelpers.containsEntry(keystore, secretName);
        }
        catch (EConfigLoad e) {
            var message = String.format("Secret could not be found in the key store: [%s] %s", secretName, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EConfigLoad(message);
        }
    }

    @Override
    public String loadPassword(String secretName) {

        try {
            return CryptoHelpers.readTextEntry(keystore, keystoreKey, secretName);
        }
        catch (EConfigLoad e) {
            var message = String.format("Password could not be retrieved from the key store: [%s] %s", secretName, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EConfigLoad(message, e);
        }
    }

    @Override
    public PublicKey loadPublicKey(String secretName) {

        try {
            var base64 = CryptoHelpers.readTextEntry(keystore, keystoreKey, secretName);
            return CryptoHelpers.decodePublicKey(base64, false);
        }
        catch (EConfigLoad e) {
            var message = String.format("Public key could not be retrieved from the key store: [%s] %s", secretName, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EConfigLoad(message, e);
        }
    }

    @Override
    public PrivateKey loadPrivateKey(String secretName) {

        try {
            var base64 = CryptoHelpers.readTextEntry(keystore, keystoreKey, secretName);
            return CryptoHelpers.decodePrivateKey(base64, false);
        }
        catch (EConfigLoad e) {
            var message = String.format("Private key could not be retrieved from the key store: [%s] %s", secretName, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EConfigLoad(message, e);
        }
    }
}
