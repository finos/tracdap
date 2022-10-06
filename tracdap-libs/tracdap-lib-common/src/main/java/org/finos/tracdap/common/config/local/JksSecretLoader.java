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

package org.finos.tracdap.common.config.local;

import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.ISecretLoader;
import org.finos.tracdap.common.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Properties;


public class JksSecretLoader implements ISecretLoader {

    public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Properties properties;
    private KeyStore keystore;
    private boolean ready;
    String secretKey;

    public JksSecretLoader(Properties properties) {

        this.properties = properties;
        this.keystore = null;
        this.ready = false;
    }

    @Override
    public void init(ConfigManager configManager) {

        if (ready) {
            log.error("JKS secret loader initialized twice");
            throw new EStartup("JKS secret loader initialized twice");
        }

        var keystoreType = properties.getProperty(ConfigKeys.SECRET_TYPE_KEY, DEFAULT_KEYSTORE_TYPE);
        var keystoreUrl = properties.getProperty(ConfigKeys.SECRET_URL_KEY);
        var keystoreKey = properties.getProperty(ConfigKeys.SECRET_KEY_KEY);

        try {

            log.info("Initializing JKS secret loader...");

            if (keystoreUrl == null || keystoreUrl.isBlank()) {
                var message = String.format("JKS secrets need %s in the main config file", ConfigKeys.SECRET_URL_KEY);
                log.error(message);
                throw new EStartup(message);
            }

            if (keystoreKey == null || keystoreKey.isBlank()) {
                var template = "JKS secrets need a secret key, use --secret-key or set %s in the environment";
                var message = String.format(template, ConfigKeys.SECRET_KEY_ENV);
                log.error(message);
                throw new EStartup(message);
            }

            this.keystore = KeyStore.getInstance(keystoreType);
            var keystoreBytes = configManager.loadBinaryConfig(keystoreUrl);

            try (var stream = new ByteArrayInputStream(keystoreBytes)) {

                keystore.load(stream, keystoreKey.toCharArray());
                ready = true;
                this.secretKey = keystoreKey;
            }
        }
        catch (KeyStoreException e) {
            var message = String.format("Keystore type is not supported: [%s]", keystoreType);
            log.error(message);
            throw new EStartup(message);
        }
        catch (IOException e) {
            // Inner error is more meaningful if keystore cannot be read
            var error = e.getCause() != null ? e.getCause() : e;
            var errorDetail = error.getMessage() + " (this normally means the secret key is wrong)";
            var message = String.format("Failed to open keystore [%s]: %s", keystoreUrl, errorDetail);
            log.error(message);
            throw new EStartup(message);
        }
        catch (NoSuchAlgorithmException | CertificateException e) {
            var message = String.format("Failed to open keystore [%s]: %s", keystoreUrl, e.getMessage());
            log.error(message);
            throw new EStartup(message);
        }
    }

    @Override
    public String loadPassword(String secretName) {

        try {

            if (!ready) {
                log.error("JKS Secret loader has not been initialized");
                throw new ETracInternal("JKS Secret loader has not been initialized");
            }

            var entry = keystore.getEntry(secretName, new KeyStore.PasswordProtection(secretKey.toCharArray()));  // KeyStore.ProtectionParameter() /* protection = */);

            if (entry == null) {
                var message = String.format("Secret is not present in the store: [%s]", secretName);
                log.error(message);
                throw new EConfigLoad(message);
            }

            if (!(entry instanceof KeyStore.SecretKeyEntry)) {
                var message = String.format("Secret is not a secret key: [%s] is %s", secretName, entry.getClass().getSimpleName());
                log.error(message);
                throw new EConfigLoad(message);
            }

            var secret = (KeyStore.SecretKeyEntry) entry;
            var algorithm = secret.getSecretKey().getAlgorithm();
            var factory = SecretKeyFactory.getInstance(algorithm);

            // Decode using based encryption
            var keySpecType = PBEKeySpec.class;
            var keySpec = (PBEKeySpec) factory.getKeySpec(secret.getSecretKey(), keySpecType);

            var password = keySpec.getPassword();

            return new String(password);
        }
        // TODO: Errors
        catch (UnrecoverableEntryException e) {
            throw new RuntimeException(e);
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        catch (KeyStoreException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }
}
