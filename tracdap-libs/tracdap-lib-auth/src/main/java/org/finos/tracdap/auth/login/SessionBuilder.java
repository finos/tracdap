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

package org.finos.tracdap.auth.login;

import org.finos.tracdap.common.auth.internal.SessionInfo;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.config.ConfigDefaults;
import org.finos.tracdap.config.AuthenticationConfig;

import java.time.Instant;


public class SessionBuilder {

    public static SessionInfo newSession(UserInfo userInfo, AuthenticationConfig authConfig) {

        var configExpiry = ConfigDefaults.readOrDefault(authConfig.getJwtExpiry(), ConfigDefaults.DEFAULT_JWT_EXPIRY);
        var configLimit = ConfigDefaults.readOrDefault(authConfig.getJwtLimit(), ConfigDefaults.DEFAULT_JWT_LIMIT);

        var issue = Instant.now();
        var expiry = issue.plusSeconds(configExpiry);
        var limit = issue.plusSeconds(configLimit);

        var session = new SessionInfo();
        session.setUserInfo(userInfo);
        session.setIssueTime(issue);
        session.setExpiryTime(expiry);
        session.setExpiryLimit(limit);
        session.setValid(true);

        return session;
    }

    public static SessionInfo refreshSession(SessionInfo session, AuthenticationConfig authConfig) {

        var latestIssue = session.getIssueTime();
        var originalLimit = session.getExpiryLimit();

        var configRefresh = ConfigDefaults.readOrDefault(authConfig.getJwtRefresh(), ConfigDefaults.DEFAULT_JWT_REFRESH);
        var configExpiry = ConfigDefaults.readOrDefault(authConfig.getJwtExpiry(), ConfigDefaults.DEFAULT_JWT_EXPIRY);

        // If the refresh time hasn't elapsed yet, return the original session without modification
        if (latestIssue.plusSeconds(configRefresh).isAfter(Instant.now()))
            return session;

        var newIssue = Instant.now();
        var newExpiry = newIssue.plusSeconds(configExpiry);
        var limitedExpiry = newExpiry.isBefore(originalLimit) ? newExpiry : originalLimit;

        var newSession = new SessionInfo();
        newSession.setUserInfo(session.getUserInfo());
        newSession.setIssueTime(newIssue);
        newSession.setExpiryTime(limitedExpiry);
        newSession.setExpiryLimit(originalLimit);

        // Session remains valid until time ticks past the original limit time, i.e. issue < limit
        newSession.setValid(newIssue.isBefore(originalLimit));

        return newSession;
    }
}
