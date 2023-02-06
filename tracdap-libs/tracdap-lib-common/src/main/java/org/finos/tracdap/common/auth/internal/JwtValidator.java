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

import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.common.config.ConfigDefaults;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.RegisteredClaims;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;

import java.time.Instant;


public class JwtValidator {

    protected static final String JWT_NAME_CLAIM = "name";
    protected static final String JWT_LIMIT_CLAIM = "limit";

    protected static final String JWT_DELEGATE_ID_CLAIM = "delegate";
    protected static final String JWT_DELEGATE_NAME_CLAIM = "delegateName";

    protected final Algorithm algorithm;
    protected final String issuer;
    protected final int expiry;

    private final JWTVerifier verifier;

    public JwtValidator(AuthenticationConfig authConfig, Algorithm algorithm) {

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

            if (userId.isMissing() || issueTimeStr.isMissing() || expiryTimeStr.isMissing() || expiryLimitStr.isMissing()) {
                var sessionInfo = new SessionInfo();
                sessionInfo.setValid(false);
                sessionInfo.setErrorMessage("Authentication failed: Missing required details");
                return sessionInfo;
            }

            // Note: calling "toString()" on claims will include JSON quoting in the output
            // Use the asString / asLong / asType methods to avoid this behavior

            var userInfo = new UserInfo();
            userInfo.setUserId(userId.asString());
            userInfo.setDisplayName(displayName.isMissing() ? userId.asString() : displayName.asString());

            var issueTime = Instant.ofEpochSecond(issueTimeStr.asLong());
            var expiryTime = Instant.ofEpochSecond(expiryTimeStr.asLong());
            var expiryLimit = Instant.ofEpochSecond(expiryLimitStr.asLong());

            var sessionInfo = new SessionInfo();
            sessionInfo.setUserInfo(userInfo);
            sessionInfo.setIssueTime(issueTime);
            sessionInfo.setExpiryTime(expiryTime);
            sessionInfo.setExpiryLimit(expiryLimit);
            sessionInfo.setValid(true);

            var delegateIdClaim = jwt.getClaim(JWT_DELEGATE_ID_CLAIM);
            var delegateNameClaim = jwt.getClaim(JWT_DELEGATE_NAME_CLAIM);

            if (!delegateIdClaim.isMissing()) {
                var delegate = new UserInfo();
                delegate.setUserId(delegateIdClaim.asString());
                delegate.setDisplayName(delegateNameClaim.asString());
                sessionInfo.setDelegate(delegate);
            }

            return sessionInfo;
        }
        catch (JWTVerificationException | NumberFormatException e) {

            var message = String.format("Session is not valid: %s", e.getMessage());

            var sessionInfo = new SessionInfo();
            sessionInfo.setValid(false);
            sessionInfo.setErrorMessage(message);

            return sessionInfo;
        }
    }
}