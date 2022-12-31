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

package org.finos.tracdap.common.auth.internal;

import org.finos.tracdap.common.config.ConfigDefaults;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EUnexpected;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.RegisteredClaims;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.config.PlatformInfo;

import java.security.KeyPair;
import java.security.PublicKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;


public class JwtValidator {

    protected static final String JWT_NAME_CLAIM = "name";
    protected static final String JWT_LIMIT_CLAIM = "limit";

    protected final Algorithm algorithm;
    protected final String issuer;
    protected final int expiry;

    private final JWTVerifier verifier;

    public static JwtValidator configure(AuthenticationConfig authConfig, PlatformInfo platformInfo, PublicKey publicKey) {

        // Allow disabling signing in non-prod environments only
        if (authConfig.getDisableSigning()) {

            if (platformInfo.getProduction()) {

                var message = String.format(
                        "Token signing must be enabled in production environment [%s]",
                        platformInfo.getEnvironment());

                throw new EStartup(message);
            }

            return new JwtProcessor(authConfig, Algorithm.none());
        }

        // If the key pair is missing but signing is not disabled, this is an error
        if (publicKey == null) {
            throw new EStartup("Root authentication key is not available (do you need to run auth-tool)?");
        }

        var algorithm = chooseAlgorithm(publicKey);
        return new JwtValidator(authConfig, algorithm);
    }

    JwtValidator(AuthenticationConfig authConfig, Algorithm algorithm) {

        this.algorithm = algorithm;

        this.issuer = authConfig.getJwtIssuer();
        this.expiry = ConfigDefaults.readOrDefault(authConfig.getJwtExpiry(), ConfigDefaults.DEFAULT_JWT_EXPIRY);

        this.verifier = JWT
                .require(algorithm)
                .withIssuer(issuer)
                .build();
    }

    public SessionInfo decodeAndValidate(String token) {

        try {

            var jwt = verifier.verify(token);

            var userId = jwt.getClaim(RegisteredClaims.SUBJECT);
            var displayName = jwt.getClaim(JWT_NAME_CLAIM);
            var issueTimeStr = jwt.getClaim(RegisteredClaims.ISSUED_AT);
            var expiryTimeStr = jwt.getClaim(RegisteredClaims.EXPIRES_AT);
            var expiryLimitStr = jwt.getClaim(JWT_LIMIT_CLAIM);

            if (userId == null || issueTimeStr == null || expiryTimeStr == null || expiryLimitStr == null) {
                var sessionInfo = new SessionInfo();
                sessionInfo.setValid(false);
                sessionInfo.setErrorMessage("Authentication failed: Missing required details");
                return sessionInfo;
            }

            // Note: calling "toString()" on claims will include JSON quoting in the output
            // Use the asString / asLong / asType methods to avoid this behavior

            var userInfo = new UserInfo();
            userInfo.setUserId(userId.asString());
            userInfo.setDisplayName(displayName != null ? displayName.asString() : userId.asString());

            var issueTime = Instant.ofEpochSecond(issueTimeStr.asLong());
            var expiryTime = Instant.ofEpochSecond(expiryTimeStr.asLong());
            var expiryLimit = Instant.ofEpochSecond(expiryLimitStr.asLong());

            var sessionInfo = new SessionInfo();
            sessionInfo.setUserInfo(userInfo);
            sessionInfo.setIssueTime(issueTime);
            sessionInfo.setExpiryTime(expiryTime);
            sessionInfo.setExpiryLimit(expiryLimit);
            sessionInfo.setValid(true);

            return sessionInfo;
        }
        catch (JWTVerificationException | NumberFormatException e) {

            var message = String.format("Authentication failed: %s", e.getMessage());

            var sessionInfo = new SessionInfo();
            sessionInfo.setValid(false);
            sessionInfo.setErrorMessage(message);

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

            if (keySize >= 3072)
                return Algorithm.RSA512((RSAPublicKey)  keyPair.getPublic(), (RSAPrivateKey) keyPair.getPrivate());

            if (keySize >= 2048)
                return Algorithm.RSA384((RSAPublicKey)  keyPair.getPublic(), (RSAPrivateKey) keyPair.getPrivate());

            if (keySize >= 1024)
                return Algorithm.RSA256((RSAPublicKey)  keyPair.getPublic(), (RSAPrivateKey) keyPair.getPrivate());
        }

        var message = String.format("" +
                        "Root authentication keys are not available, no JWT singing / validation algorithm for [algorithM: %s, key size: %s]",
                keyAlgo, keySize);

        throw new EStartup(message);
    }
}