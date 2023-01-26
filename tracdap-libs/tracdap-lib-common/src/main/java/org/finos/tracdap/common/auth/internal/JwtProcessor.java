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

import com.auth0.jwt.HeaderParams;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.config.PlatformInfo;

import java.security.KeyPair;
import java.util.Map;


public class JwtProcessor extends JwtValidator {

    public static JwtProcessor configure(AuthenticationConfig authConfig, PlatformInfo platformInfo, KeyPair keyPair) {

        // TODO: Move this method to AuthSetup

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
        if (keyPair == null) {
            throw new EStartup("Root authentication key is not available (do you need to run auth-tool)?");
        }

        var algorithm = chooseAlgorithm(keyPair);
        return new JwtProcessor(authConfig, algorithm);
    }

    JwtProcessor(AuthenticationConfig authConfig, Algorithm algorithm) {
        super(authConfig, algorithm);
    }

    public String encodeToken(SessionInfo session) {

        var header = Map.of(
                HeaderParams.TYPE, "jwt",
                HeaderParams.ALGORITHM, algorithm);

        var jwt = JWT.create()
                .withHeader(header)
                .withSubject(session.getUserInfo().getUserId())
                .withIssuer(issuer)
                .withIssuedAt(session.getIssueTime())
                .withExpiresAt(session.getExpiryTime())
                .withClaim(JWT_LIMIT_CLAIM, session.getExpiryLimit())
                .withClaim(JWT_NAME_CLAIM, session.getUserInfo().getDisplayName());

        return jwt.sign(algorithm).trim();
    }

}