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
import org.finos.tracdap.common.config.CryptoHelpers;
import org.finos.tracdap.common.exception.EStartup;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.UnrecoverableEntryException;


public class JksHelpers {

    public static KeyStore loadKeystore(
            String keystoreType, Path keystorePath, String keystoreKey,
            boolean createIfMissing) {

        try {

            var keystore = KeyStore.getInstance(keystoreType);

            if (!Files.exists(keystorePath) && createIfMissing) {
                keystore.load(null, keystoreKey.toCharArray());
            } else {
                try (var in = new FileInputStream(keystorePath.toFile())) {
                    keystore.load(in, keystoreKey.toCharArray());
                }
            }

            return keystore;
        }
        catch (Exception e) {

            var message = String.format("There was a problem loading the key store: %s", e.getMessage());
            throw new EStartup(message, e);
        }
    }

    public static void saveKeystore(Path keystorePath, String keystoreKey, KeyStore keystore) {

        try {

            var keystoreBackup = keystorePath.getParent().resolve((keystorePath.getFileSystem() + ".upd~"));

            if (Files.exists(keystorePath))
                Files.move(keystorePath, keystoreBackup);

            try (var out = new FileOutputStream(keystorePath.toFile())) {
                keystore.store(out, keystoreKey.toCharArray());
            }

            if (Files.exists(keystoreBackup))
                Files.delete(keystoreBackup);
        }
        catch (Exception e) {

            var message = String.format("There was a problem saving the key store: %s", e.getMessage());
            throw new EStartup(message, e);
        }
    }

    public static void writeKeysToKeystore(KeyStore keystore, String keystoreKey, KeyPair keyPair) {

        try {

            var publicEncoded = CryptoHelpers.encodePublicKey(keyPair.getPublic(), false);
            var privateEncoded = CryptoHelpers.encodePrivateKey(keyPair.getPrivate(), false);

            CryptoHelpers.writeTextEntry(keystore, keystoreKey, ConfigKeys.TRAC_AUTH_PUBLIC_KEY, publicEncoded);
            CryptoHelpers.writeTextEntry(keystore, keystoreKey, ConfigKeys.TRAC_AUTH_PRIVATE_KEY, privateEncoded);
        }
        catch (Exception e) {

            var innerError = (e.getCause() instanceof UnrecoverableEntryException)
                    ? e.getCause() : e;

            var message = String.format("There was a problem writing the keys: %s", innerError.getMessage());
            throw new EStartup(message, innerError);
        }
    }
}
