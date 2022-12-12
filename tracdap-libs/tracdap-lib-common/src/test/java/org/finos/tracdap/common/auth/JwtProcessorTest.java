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

import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.config.AuthenticationConfig;

import com.auth0.jwt.HeaderParams;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;

import org.finos.tracdap.config.PlatformInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Map;


public class JwtProcessorTest {

    static AuthenticationConfig authConfig;
    static PlatformInfo platformInfo;
    static KeyPair keyPair;

    JwtProcessor jwt;

    @BeforeAll
    static void createKeyPair() throws Exception {

        authConfig = AuthenticationConfig.newBuilder()
                .setJwtIssuer("https://trac.example.com/trac/platform")
                .setJwtExpiry(7200)
                .build();

        platformInfo = PlatformInfo.newBuilder()
                .setEnvironment("TEST_PRODUCTION")
                .setProduction(true)
                .build();

        var keySize = 1024;
        var keyGen = KeyPairGenerator.getInstance("RSA");
        var random = SecureRandom.getInstance("SHA1PRNG");

        keyGen.initialize(keySize, random);

        keyPair = keyGen.generateKeyPair();
    }

    @BeforeEach
    void setupJwt() {

        jwt = JwtProcessor.configure(authConfig, platformInfo, keyPair);
    }

    @Test
    void jwtRoundTrip() {

        var userInfo = new UserInfo();
        userInfo.setUserId("fb2876");
        userInfo.setDisplayName("Fred Blogs Jnr.");

        var token = jwt.encodeToken(userInfo);
        var rtSession = jwt.decodeAndValidate(token);
        var rtUserInfo = rtSession.getUserInfo();

        Assertions.assertEquals(userInfo.getUserId(), rtUserInfo.getUserId());
        Assertions.assertEquals(userInfo.getDisplayName(), rtUserInfo.getDisplayName());
    }

    @Test
    void signatureMissing() {

        var algorithm = Algorithm.none();

        var issuer = "trac_gateway";
        var issueTime = Instant.now();
        var expiryTime = issueTime.plusSeconds(60);

        var header = Map.of(
                HeaderParams.TYPE, "jwt",
                HeaderParams.ALGORITHM, algorithm);

        var tokenBuilder = JWT.create()
                .withHeader(header)
                .withIssuer(issuer)
                .withIssuedAt(issueTime)
                .withExpiresAt(expiryTime)
                .withSubject("cookie_monster")
                .withClaim("name", "The Monster who LOOOVES cookies!");

        // Signing with algorithm NONE creates a token with no signature
        var token = tokenBuilder.sign(algorithm).trim();

        // Decoding an unsigned token should be an auth error
        var result = jwt.decodeAndValidate(token);
        Assertions.assertFalse(result.isValid());
    }

    @Test
    void signatureNotTrusted() throws Exception {

        // Tokens created with different key pairs should fail to decode and validated

        var keySize = 1024;
        var keyGen = KeyPairGenerator.getInstance("RSA");
        var random = SecureRandom.getInstance("SHA1PRNG");
        keyGen.initialize(keySize, random);

        var altKeyPair = keyGen.generateKeyPair();

        var altJwt = JwtProcessor.configure(authConfig, platformInfo, altKeyPair);

        var userInfo = new UserInfo();
        userInfo.setUserId("fb2876");
        userInfo.setDisplayName("Fred Blogs Jnr.");

        var token = jwt.encodeToken(userInfo);
        var altToken = altJwt.encodeToken(userInfo);

        var result = altJwt.decodeAndValidate(token);
        var altResult =  jwt.decodeAndValidate(altToken);

        Assertions.assertFalse(result.isValid());
        Assertions.assertFalse(altResult.isValid());
    }

    @Test
    void differentIssuer() {

        // Tokens created with different issuers should fail to decode and validated

        var altAuthConfig = AuthenticationConfig.newBuilder()
                .setJwtIssuer("https://alt.trac.example.com/trac/platform")
                .setJwtExpiry(7200)
                .build();

        var altJwt = JwtProcessor.configure(altAuthConfig, platformInfo, keyPair);

        var userInfo = new UserInfo();
        userInfo.setUserId("fb2876");
        userInfo.setDisplayName("Fred Blogs Jnr.");

        var token = jwt.encodeToken(userInfo);
        var altToken = altJwt.encodeToken(userInfo);

        var result = altJwt.decodeAndValidate(token);
        var altResult =  jwt.decodeAndValidate(altToken);

        Assertions.assertFalse(result.isValid());
        Assertions.assertFalse(altResult.isValid());
    }

    @Test
    void sessionExpired() throws Exception {

        // Attempting to decode and validate after a token expires is an auth error

        var altAuthConfig = AuthenticationConfig.newBuilder()
                .setJwtIssuer("https://trac.example.com/trac/platform")
                .setJwtExpiry(1)
                .build();

        var altJwt = JwtProcessor.configure(altAuthConfig, platformInfo, keyPair);

        var userInfo = new UserInfo();
        userInfo.setUserId("fb2876");
        userInfo.setDisplayName("Fred Blogs Jnr.");

        var token = altJwt.encodeToken(userInfo);

        Thread.sleep(2000);

        var result = altJwt.decodeAndValidate(token);
        Assertions.assertFalse(result.isValid());
    }

    @Test
    void disableSigningNonProd() {

        var altAuthConfig = AuthenticationConfig.newBuilder()
                .setDisableSigning(true)
                .build();

        var altEnvironment = PlatformInfo.newBuilder()
                .setEnvironment("TEST_NON_PROD")
                .setProduction(false)
                .build();

        var altJwt = JwtProcessor.configure(altAuthConfig, altEnvironment, keyPair);

        var userInfo = new UserInfo();
        userInfo.setUserId("fb2876");
        userInfo.setDisplayName("Fred Blogs Jnr.");

        var token = altJwt.encodeToken(userInfo);
        var rtSession = altJwt.decodeAndValidate(token);
        var rtUserInfo = rtSession.getUserInfo();

        Assertions.assertEquals(userInfo.getUserId(), rtUserInfo.getUserId());
        Assertions.assertEquals(userInfo.getDisplayName(), rtUserInfo.getDisplayName());
    }

    @Test
    void disableSigningInProduction() {

        var altAuthConfig = AuthenticationConfig.newBuilder()
                .setDisableSigning(true)
                .build();

        Assertions.assertThrows(EStartup.class, () -> JwtProcessor.configure(altAuthConfig, platformInfo, keyPair));
    }
}
