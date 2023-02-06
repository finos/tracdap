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
import org.finos.tracdap.common.auth.SessionInfo;
import org.finos.tracdap.config.AuthenticationConfig;

import java.util.Map;


public class JwtProcessor extends JwtValidator {

    public JwtProcessor(AuthenticationConfig authConfig, Algorithm algorithm) {
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

        if (session.getDelegate() != null) {
            jwt.withClaim(JWT_DELEGATE_ID_CLAIM, session.getDelegate().getUserId());
            jwt.withClaim(JWT_DELEGATE_NAME_CLAIM, session.getDelegate().getDisplayName());
        }

        return jwt.sign(algorithm).trim();
    }
}