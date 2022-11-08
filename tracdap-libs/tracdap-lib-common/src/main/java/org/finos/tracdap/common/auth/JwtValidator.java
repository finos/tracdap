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

package org.finos.tracdap.common.auth;

import com.auth0.jwt.JWT;
import com.auth0.jwt.RegisteredClaims;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTDecodeException;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EUnexpected;

import java.security.KeyPair;
import java.security.PublicKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;


public class JwtValidator {

    protected static final String JWT_NAME_CLAIM = "name";

    protected final Algorithm algorithm;

    public static JwtValidator usePublicKey(PublicKey publicKey) {

        var algorithm = chooseAlgorithm(publicKey);
        return new JwtValidator(algorithm);
    }

    JwtValidator(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public SessionInfo decodeAndValidate(String token) {

        try {

            var jwt = JWT.decode(token);
            var userId = jwt.getClaim(RegisteredClaims.SUBJECT);
            var displayName = jwt.getClaim(JWT_NAME_CLAIM);
            var issueTimeStr = jwt.getClaim(RegisteredClaims.ISSUED_AT);
            var expiryTimeStr = jwt.getClaim(RegisteredClaims.EXPIRES_AT);

            if (userId == null || issueTimeStr == null || expiryTimeStr == null) {
                var sessionInfo = new SessionInfo();
                sessionInfo.setValid(false);
                sessionInfo.setErrorMessage("Missing required authentication details");
                return sessionInfo;
            }

            // Note: calling "toString()" on claims will include JSON quoting in the output
            // Use the asString / asLong / asType methods to avoid this behavior

            var issueTime = Instant.ofEpochSecond(issueTimeStr.asLong());
            var expiryTime = Instant.ofEpochSecond(expiryTimeStr.asLong());

            var sessionInfo = new SessionInfo();
            sessionInfo.setValid(true);
            sessionInfo.setIssueTime(issueTime);
            sessionInfo.setExpiryTime(expiryTime);

            var userInfo = new UserInfo();
            userInfo.setUserId(userId.asString());
            userInfo.setDisplayName(displayName != null ? displayName.asString() : userId.asString());
            sessionInfo.setUserInfo(userInfo);

            return sessionInfo;
        }
        catch (JWTDecodeException | NumberFormatException e) {

            var sessionInfo = new SessionInfo();
            sessionInfo.setValid(false);
            sessionInfo.setErrorMessage(e.getMessage());

            return sessionInfo;
        }
    }

    private static Algorithm chooseAlgorithm(PublicKey publicKey) {

        // Should already be checked
        if (publicKey == null)
            throw new EUnexpected();

        var keyPair = new KeyPair(publicKey, null);

        return chooseAlgorithm(keyPair);
    }

    protected static Algorithm chooseAlgorithm(KeyPair keyPair) {

        // Should already be checked
        if (keyPair == null)
            throw new EUnexpected();

        var keyAlgo = keyPair.getPublic().getAlgorithm();
        var keySize = keyPair.getPublic().getEncoded().length * 8;

        if (keyAlgo.equals("EC")) {

            if (keySize >= 512)
                return Algorithm.ECDSA512((ECPublicKey) keyPair.getPublic(), (ECPrivateKey)  keyPair.getPrivate());

            if (keySize >= 384)
                return Algorithm.ECDSA384((ECPublicKey) keyPair.getPublic(), (ECPrivateKey)  keyPair.getPrivate());

            if (keySize >= 256)
                return Algorithm.ECDSA256((ECPublicKey) keyPair.getPublic(), (ECPrivateKey)  keyPair.getPrivate());
        }

        if (keyAlgo.equals("RSA")) {

            if (keySize >= 512)
                return Algorithm.RSA512((RSAPublicKey)  keyPair.getPublic(), (RSAPrivateKey) keyPair.getPrivate());

            if (keySize >= 384)
                return Algorithm.RSA384((RSAPublicKey)  keyPair.getPublic(), (RSAPrivateKey) keyPair.getPrivate());

            if (keySize >= 256)
                return Algorithm.RSA256((RSAPublicKey)  keyPair.getPublic(), (RSAPrivateKey) keyPair.getPrivate());
        }

        var message = String.format("" +
                "Root authentication keys are not available, no JWT singing / validation algorithm for [algorithM: %s, key size: %s]",
                keyAlgo, keySize);

        throw new EStartup(message);
    }
}
