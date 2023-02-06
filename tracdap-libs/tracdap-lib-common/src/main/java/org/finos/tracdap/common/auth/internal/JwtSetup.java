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

package org.finos.tracdap.common.auth.internal;

import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.PlatformInfo;

import com.auth0.jwt.algorithms.Algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.PublicKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;


public class JwtSetup {

    private static final Logger log = LoggerFactory.getLogger(JwtSetup.class);

    public static JwtProcessor createProcessor(AuthenticationConfig authConfig, PlatformInfo platformInfo, ConfigManager configManager) {

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

        if (!configManager.hasSecret(ConfigKeys.TRAC_AUTH_PUBLIC_KEY) ||
            !configManager.hasSecret(ConfigKeys.TRAC_AUTH_PRIVATE_KEY)) {

            // Allowing the service to run without validating authentication is hugely risky
            // Especially because claims can still be added to JWT
            // The auth-tool utility makes it really easy to set up auth keys on local JKS

            var error = "Root authentication keys are not available, the service will not start";
            log.error(error);
            throw new EStartup(error);
        }

        var publicKey = configManager.loadPublicKey(ConfigKeys.TRAC_AUTH_PUBLIC_KEY);
        var privateKey = configManager.loadPrivateKey(ConfigKeys.TRAC_AUTH_PRIVATE_KEY);
        var keyPair = new KeyPair(publicKey, privateKey);

        var algorithm = chooseAlgorithm(keyPair);
        return new JwtProcessor(authConfig, algorithm);
    }

    public static JwtValidator createValidator(
            PlatformConfig platformConfig,
            ConfigManager configManager) {

        return createValidator(
                platformConfig.getAuthentication(),
                platformConfig.getPlatformInfo(),
                configManager);
    }

    public static JwtValidator createValidator(
            AuthenticationConfig authConfig,
            PlatformInfo platformInfo,
            ConfigManager configManager) {

        // Do not allow turning off the authentication mechanism in production!
        if (platformInfo.getProduction()) {

            if (authConfig.getDisableAuth() || authConfig.getDisableSigning()) {

                var message = String.format(
                        "Authentication and token signing must be enabled in production environment [%s]",
                        platformInfo.getEnvironment());

                log.error(message);
                throw new EStartup(message);
            }
        }

        if (authConfig.getDisableAuth()) {

            log.warn("!!!!! AUTHENTICATION IS DISABLED (do not use this setting in production)");

            return null;
        }
        else if (authConfig.getDisableSigning()) {

            log.warn("!!!!! SIGNATURE VALIDATION IS DISABLED (do not use this setting in production)");

            return createValidator(authConfig, platformInfo, (PublicKey) null);
        }
        else if (configManager.hasSecret(ConfigKeys.TRAC_AUTH_PUBLIC_KEY)) {

            var publicKey = configManager.loadPublicKey(ConfigKeys.TRAC_AUTH_PUBLIC_KEY);
            return createValidator(authConfig, platformInfo, publicKey);
        }
        else {

            // Allowing the service to run without validating authentication is hugely risky
            // Especially because claims can still be added to JWT
            // The auth-tool utility makes it really easy to set up auth keys on local JKS

            var error = "Root authentication keys are not available, the service will not start";
            log.error(error);
            throw new EStartup(error);
        }
    }

    private static JwtValidator createValidator(
            AuthenticationConfig authConfig,
            PlatformInfo platformInfo,
            PublicKey publicKey) {

        // Allow disabling signing in non-prod environments only
        if (authConfig.getDisableSigning()) {

            if (platformInfo.getProduction()) {

                var message = String.format(
                        "Token signing must be enabled in production environment [%s]",
                        platformInfo.getEnvironment());

                throw new EStartup(message);
            }

            return new JwtValidator(authConfig, Algorithm.none());
        }

        // If the key pair is missing but signing is not disabled, this is an error
        if (publicKey == null) {
            throw new EStartup("Root authentication key is not available (do you need to run auth-tool)?");
        }

        var algorithm = chooseAlgorithm(publicKey);
        return new JwtValidator(authConfig, algorithm);
    }

    private static Algorithm chooseAlgorithm(PublicKey publicKey) {

        // Should already be checked
        if (publicKey == null)
            throw new EUnexpected();

        var keyPair = new KeyPair(publicKey, null);

        return chooseAlgorithm(keyPair);
    }

    private static Algorithm chooseAlgorithm(KeyPair keyPair) {

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
