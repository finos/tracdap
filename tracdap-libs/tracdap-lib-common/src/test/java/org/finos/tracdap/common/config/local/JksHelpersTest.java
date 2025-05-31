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

import org.finos.tracdap.common.exception.EConfigLoad;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;

public class JksHelpersTest {

    @Test
    void roundTrip_password() throws Exception {

        var secretKey = "qdierj-ejcuw-ejcude";
        var keystore = KeyStore.getInstance("PKCS12");
        keystore.load(null, secretKey.toCharArray());

        var payload = "A bilge rat and a parrot";

        JksHelpers.writeTextEntry(keystore, secretKey, "my_secret", payload);
        var rtPayload = JksHelpers.readTextEntry(keystore, secretKey, "my_secret");

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
        var encoded = JksHelpers.encodePublicKey(publicKey, false);
        var rtPublicKey = JksHelpers.decodePublicKey(encoded, false);

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
        var encoded = JksHelpers.encodePrivateKey(privateKey, false);
        var rtPrivateKey = JksHelpers.decodePrivateKey(encoded, false);

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
        var encoded = JksHelpers.encodePublicKey(publicKey, true);
        var rtPublicKey = JksHelpers.decodePublicKey(encoded, true);

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
        var encoded = JksHelpers.encodePrivateKey(privateKey, true);
        var rtPrivateKey = JksHelpers.decodePrivateKey(encoded,  true);

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
        var encodedPublic = JksHelpers.encodePublicKey(publicKey, false);
        var encodedPrivate = JksHelpers.encodePrivateKey(privateKey, false);

        JksHelpers.writeTextEntry(keystore, secretKey, "public_key", encodedPublic);
        JksHelpers.writeTextEntry(keystore, secretKey, "private_key", encodedPrivate);

        var rtEncodedPublic = JksHelpers.readTextEntry(keystore, secretKey, "public_key");
        var rtEncodedPrivate = JksHelpers.readTextEntry(keystore, secretKey, "private_key");

        var rtPublicKey = JksHelpers.decodePublicKey(rtEncodedPublic, false);
        var rtPrivateKey = JksHelpers.decodePrivateKey(rtEncodedPrivate, false);

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

        JksHelpers.writeTextEntry(keystore, secretKey, "my_secret", payload);

        Assertions.assertThrows(EConfigLoad.class, () ->
                JksHelpers.readTextEntry(keystore, secretKey, "different_secret"));
    }

    @Test
    void decodeGarbled() {

        Assertions.assertThrows(EConfigLoad.class, () ->  JksHelpers.decodePublicKey("asdfpasdfasef", false));
        Assertions.assertThrows(EConfigLoad.class, () -> JksHelpers.decodePrivateKey("asdfasdfasdf", false));

        Assertions.assertThrows(EConfigLoad.class, () ->  JksHelpers.decodePublicKey("asdfpasdfasef", true));
        Assertions.assertThrows(EConfigLoad.class, () -> JksHelpers.decodePrivateKey("asdfasdfasdf", true));
    }

    @Test
    void decodeWrongKeyType() throws Exception {

        var keySize = 256;
        var keyGen = KeyPairGenerator.getInstance("EC");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        var keyPair = keyGen.generateKeyPair();

        var publicKey = keyPair.getPublic();
        var publicEncoded = JksHelpers.encodePublicKey(publicKey, false);

        var privateKey = keyPair.getPrivate();
        var privateEncoded = JksHelpers.encodePrivateKey(privateKey, false);

        Assertions.assertThrows(EConfigLoad.class, () ->  JksHelpers.decodePublicKey(privateEncoded, false));
        Assertions.assertThrows(EConfigLoad.class, () -> JksHelpers.decodePrivateKey(publicEncoded, false));
    }

    @Test
    void decodeWrongEncoding() throws Exception {

        var keySize = 256;
        var keyGen = KeyPairGenerator.getInstance("EC");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        var keyPair = keyGen.generateKeyPair();

        var publicKey = keyPair.getPublic();
        var publicEncoded = JksHelpers.encodePublicKey(publicKey, true);

        var privateKey = keyPair.getPrivate();
        var privateEncoded = JksHelpers.encodePrivateKey(privateKey, true);

        Assertions.assertThrows(EConfigLoad.class, () ->  JksHelpers.decodePublicKey(publicEncoded, false));
        Assertions.assertThrows(EConfigLoad.class, () -> JksHelpers.decodePrivateKey(privateEncoded, false));
    }
}
