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

import com.auth0.jwt.HeaderParams;
import com.auth0.jwt.JWT;
import com.auth0.jwt.RegisteredClaims;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTDecodeException;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;


public class JwtHelpers {

    private static final String JWT_NAME_CLAIM = "name";

    public static String encodeToken(UserInfo userInfo, Duration expiry) {

        // TODO: Real issuer and signing

        var issuer = "trac_gateway";
        var issueTime = Instant.now();
        var expiryTime = issueTime.plus(expiry);

        var algorithm = Algorithm.none();

        var header = Map.of(
                HeaderParams.TYPE, "jwt",
                HeaderParams.ALGORITHM, algorithm);

        var jwt = JWT.create()
                .withHeader(header)
                .withIssuer(issuer)
                .withIssuedAt(issueTime)
                .withExpiresAt(expiryTime)
                .withSubject(userInfo.getUserId())
                .withClaim(JWT_NAME_CLAIM, userInfo.getDisplayName());

        return jwt.sign(algorithm).trim();
    }

    public static SessionInfo decodeAndValidate(String token) {

        try {

            // TODO: Validation

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

            var issueTime = Instant.ofEpochSecond(Long.parseLong(issueTimeStr.toString()));
            var expiryTime = Instant.ofEpochSecond(Long.parseLong(expiryTimeStr.toString()));

            var sessionInfo = new SessionInfo();
            sessionInfo.setValid(true);
            sessionInfo.setIssueTime(issueTime);
            sessionInfo.setExpiryTime(expiryTime);

            var userInfo = new UserInfo();
            userInfo.setUserId(userId.toString());
            userInfo.setDisplayName(displayName != null ? displayName.toString() : userId.toString());
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
}
