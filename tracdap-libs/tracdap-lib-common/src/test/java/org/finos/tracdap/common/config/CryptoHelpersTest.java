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

import org.finos.tracdap.common.exception.EConfigLoad;
import org.finos.tracdap.common.exception.ETracInternal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;

public class CryptoHelpersTest {

    @Test
    void passwordHashing() {

        var password = "aKpeLiBuh-as£ASDF";

        var random = new SecureRandom();
        var salt = new byte[16];
        random.nextBytes(salt);

        var hash = CryptoHelpers.encodeSSHA512(password, salt);

        Assertions.assertTrue(CryptoHelpers.validateSSHA512(hash, password));
        Assertions.assertFalse(CryptoHelpers.validateSSHA512(hash, "wrong_password"));
    }

    @Test
    void passwordHashing_invalid() {

        Assertions.assertThrows(ETracInternal.class, () ->
                CryptoHelpers.validateSSHA512("invalid_hash", "any_password"));
    }

    @Test
    void roundTrip_password() throws Exception {

        var secretKey = "qdierj-ejcuw-ejcude";
        var keystore = KeyStore.getInstance("PKCS12");
        keystore.load(null, secretKey.toCharArray());

        var payload = "A bilge rat and a parrot";

        CryptoHelpers.writeTextEntry(keystore, secretKey, "my_secret", payload);
        var rtPayload = CryptoHelpers.readTextEntry(keystore, secretKey, "my_secret");

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

        CryptoHelpers.writeTextEntry(keystore, secretKey, "public_key", encodedPublic);
        CryptoHelpers.writeTextEntry(keystore, secretKey, "private_key", encodedPrivate);

        var rtEncodedPublic = CryptoHelpers.readTextEntry(keystore, secretKey, "public_key");
        var rtEncodedPrivate = CryptoHelpers.readTextEntry(keystore, secretKey, "private_key");

        var rtPublicKey = CryptoHelpers.decodePublicKey(rtEncodedPublic, false);
        var rtPrivateKey = CryptoHelpers.decodePrivateKey(rtEncodedPrivate, false);

        Assertions.assertEquals(publicKey, rtPublicKey);
        Assertions.assertEquals(privateKey, rtPrivateKey);
    }


    // Error cases

    @Test
    void missingSecret() throws Exception {

        var secretKey = "qdierj-ejcuw-ejcude";
        var keystore = KeyStore.getInstance("PKCS12");
        keystore.load(null, secretKey.toCharArray());

        var payload = "A bilge rat and a parrot";

        CryptoHelpers.writeTextEntry(keystore, secretKey, "my_secret", payload);

        Assertions.assertThrows(EConfigLoad.class, () ->
                CryptoHelpers.readTextEntry(keystore, secretKey, "different_secret"));
    }

    @Test
    void decodeGarbled() {

        Assertions.assertThrows(EConfigLoad.class, () ->  CryptoHelpers.decodePublicKey("asdfpasdfasef", false));
        Assertions.assertThrows(EConfigLoad.class, () -> CryptoHelpers.decodePrivateKey("asdfasdfasdf", false));

        Assertions.assertThrows(EConfigLoad.class, () ->  CryptoHelpers.decodePublicKey("asdfpasdfasef", true));
        Assertions.assertThrows(EConfigLoad.class, () -> CryptoHelpers.decodePrivateKey("asdfasdfasdf", true));
    }

    @Test
    void decodeWrongKeyType() throws Exception {

        var keySize = 256;
        var keyGen = KeyPairGenerator.getInstance("EC");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        var keyPair = keyGen.generateKeyPair();

        var publicKey = keyPair.getPublic();
        var publicEncoded = CryptoHelpers.encodePublicKey(publicKey, false);

        var privateKey = keyPair.getPrivate();
        var privateEncoded = CryptoHelpers.encodePrivateKey(privateKey, false);

        Assertions.assertThrows(EConfigLoad.class, () ->  CryptoHelpers.decodePublicKey(privateEncoded, false));
        Assertions.assertThrows(EConfigLoad.class, () -> CryptoHelpers.decodePrivateKey(publicEncoded, false));
    }

    @Test
    void decodeWrongEncoding() throws Exception {

        var keySize = 256;
        var keyGen = KeyPairGenerator.getInstance("EC");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        var keyPair = keyGen.generateKeyPair();

        var publicKey = keyPair.getPublic();
        var publicEncoded = CryptoHelpers.encodePublicKey(publicKey, true);

        var privateKey = keyPair.getPrivate();
        var privateEncoded = CryptoHelpers.encodePrivateKey(privateKey, true);

        Assertions.assertThrows(EConfigLoad.class, () ->  CryptoHelpers.decodePublicKey(publicEncoded, false));
        Assertions.assertThrows(EConfigLoad.class, () -> CryptoHelpers.decodePrivateKey(privateEncoded, false));
    }
}
