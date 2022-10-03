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

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.ISecretLoader;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.util.Properties;


public class JksSecretLoader implements ISecretLoader {

    public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Properties keystoreProps;
    private final KeyStore keystore;
    private boolean ready;

    public JksSecretLoader(Properties properties) {

        var keystoreType = properties.getProperty(SECRET_TYPE_PROP, DEFAULT_KEYSTORE_TYPE);

        try {

            this.keystoreProps = properties;
            this.keystore = KeyStore.getInstance(keystoreType);
            this.ready = false;
        }
        catch (KeyStoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void init(ConfigManager configManager) {

        if (ready) {
            log.error("JKS Secret loader initialized twice");
            throw new EStartup("JKS Secret loader initialized twice");
        }

        log.info("Initializing JKS Secret loader...");

        var keystoreUrl = "";
        var keystoreKey = this.keystoreProps.getProperty(SECRET_KEY_PROP);

        var keystoreBytes = configManager.loadBinaryConfig(keystoreUrl);

        try (var stream = new ByteArrayInputStream(keystoreBytes)) {

            keystore.load(stream, keystoreKey.toCharArray());

            ready = true;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (NoSuchAlgorithmException | CertificateException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public String loadTextSecret(String secretName) {

        try {

            if (!ready) {
                log.error("JKS Secret loader has not been initialized");
                throw new ETracInternal("JKS Secret loader has not been initialized");
            }

            var secret = keystore.getEntry(secretName, /* protection = */ null);

            if (secret == null) {
                // todo
            }

            if (!(secret instanceof KeyStore.SecretKeyEntry)) {
                throw new EUnexpected();  // todo
            }

//        ((KeyStore.SecretKeyEntry) secret).getSecretKey().
//
//        Keystore.En

            return null;
        }
        catch (UnrecoverableEntryException e) {
            throw new RuntimeException(e);
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        catch (KeyStoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] loadBinarySecret(String secretName) {

        if (!ready) {
            log.error("JKS Secret loader has not been initialized");
            throw new ETracInternal("JKS Secret loader has not been initialized");
        }

        return new byte[0];
    }


}
