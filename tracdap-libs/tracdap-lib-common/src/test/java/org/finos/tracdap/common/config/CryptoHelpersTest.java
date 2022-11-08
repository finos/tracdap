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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;

public class CryptoHelpersTest {

    @Test
    void roundTrip_password() throws Exception {

        var secretKey = "qdierj-ejcuw-ejcude";
        var keystore = KeyStore.getInstance("PKCS12");
        keystore.load(null, secretKey.toCharArray());

        var payload = "A bilge rat and a parrot";

        CryptoHelpers.writeTextEntry(keystore, "my_secret", payload, secretKey);
        var rtPayload = CryptoHelpers.readTextEntry(keystore, "my_secret", secretKey);

        Assertions.assertEquals(payload, rtPayload);
    }

    @Test
    void roundTrip_publicKey() throws Exception {

        var keySize = 256;
        var keyGen = KeyPairGenerator.getInstance("EC");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        var keyPair = keyGen.generateKeyPair();

        var publicKey = keyPair.getPublic();
        var encoded = CryptoHelpers.encodePublicKey(publicKey, false);
        var rtPublicKey = CryptoHelpers.decodePublicKey(encoded, false);

        Assertions.assertEquals(publicKey, rtPublicKey);
    }

    @Test
    void roundTrip_privateKey() throws Exception {

        var keySize = 256;
        var keyGen = KeyPairGenerator.getInstance("EC");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        var keyPair = keyGen.generateKeyPair();

        var privateKey = keyPair.getPrivate();
        var encoded = CryptoHelpers.encodePrivateKey(privateKey, false);
        var rtPrivateKey = CryptoHelpers.decodePrivateKey(encoded, false);

        Assertions.assertEquals(privateKey, rtPrivateKey);
    }

    @Test
    void roundTrip_publicKey_RsaMime() throws Exception {

        var keySize = 2048;
        var keyGen = KeyPairGenerator.getInstance("RSA");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        var keyPair = keyGen.generateKeyPair();

        var publicKey = keyPair.getPublic();
        var encoded = CryptoHelpers.encodePublicKey(publicKey, true);
        var rtPublicKey = CryptoHelpers.decodePublicKey(encoded, true);

        Assertions.assertEquals(publicKey, rtPublicKey);
    }

    @Test
    void roundTrip_privateKey_RsaMime() throws Exception {

        var keySize = 2048;
        var keyGen = KeyPairGenerator.getInstance("RSA");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        var keyPair = keyGen.generateKeyPair();

        var privateKey = keyPair.getPrivate();
        var encoded = CryptoHelpers.encodePrivateKey(privateKey, true);
        var rtPrivateKey = CryptoHelpers.decodePrivateKey(encoded,  true);

        Assertions.assertEquals(privateKey, rtPrivateKey);
    }

    @Test
    void roundTrip_keyPairFullJourney() throws Exception {

        var secretKey = "qdierj-ejcuw-ejcude";
        var keystore = KeyStore.getInstance("PKCS12");
        keystore.load(null, secretKey.toCharArray());

        var keySize = 256;
        var keyGen = KeyPairGenerator.getInstance("EC");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        var keyPair = keyGen.generateKeyPair();

        var publicKey = keyPair.getPublic();
        var privateKey = keyPair.getPrivate();
        var encodedPublic = CryptoHelpers.encodePublicKey(publicKey, false);
        var encodedPrivate = CryptoHelpers.encodePrivateKey(privateKey, false);

        CryptoHelpers.writeTextEntry(keystore, "public_key", encodedPublic, secretKey);
        CryptoHelpers.writeTextEntry(keystore, "private_key", encodedPrivate, secretKey);

        var rtEncodedPublic = CryptoHelpers.readTextEntry(keystore, "public_key", secretKey);
        var rtEncodedPrivate = CryptoHelpers.readTextEntry(keystore, "private_key", secretKey);

        var rtPublicKey = CryptoHelpers.decodePublicKey(rtEncodedPublic, false);
        var rtPrivateKey = CryptoHelpers.decodePrivateKey(rtEncodedPrivate, false);

        Assertions.assertEquals(publicKey, rtPublicKey);
        Assertions.assertEquals(privateKey, rtPrivateKey);
    }
}
