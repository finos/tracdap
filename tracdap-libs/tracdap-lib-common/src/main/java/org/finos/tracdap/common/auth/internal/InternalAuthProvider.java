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

import io.grpc.CallCredentials;
import org.finos.tracdap.config.AuthenticationConfig;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;


public class InternalAuthProvider {

    private static final String DEFAULT_SYSTEM_USER_ID = "#trac";
    private static final String DEFAULT_SYSTEM_USER_NAME = "TRAC System";
    private static final Duration DEFAULT_SYSTEM_TICKET_DURATION = Duration.of(5, ChronoUnit.MINUTES);
    private static final Duration DEFAULT_SYSTEM_TICKET_REFRESH = Duration.of(1, ChronoUnit.MINUTES);

    private final JwtProcessor tokenProcessor;
    private final UserInfo systemUser;
    private final Duration systemTicketDuration;
    private final Duration systemTicketRefresh;

    public InternalAuthProvider(JwtProcessor tokenProcessor, AuthenticationConfig authConfig) {

        var systemUserId = !authConfig.getSystemUserId().isBlank()
                ? authConfig.getSystemUserId()
                : DEFAULT_SYSTEM_USER_ID;

        var systemUserName = !authConfig.getSystemUserName().isBlank()
                ? authConfig.getSystemUserName()
                : DEFAULT_SYSTEM_USER_NAME;

        var systemTicketDuration = authConfig.getSystemTicketDuration() > 0
                ? Duration.of(authConfig.getSystemTicketDuration(), ChronoUnit.SECONDS)
                : DEFAULT_SYSTEM_TICKET_DURATION;

        var systemTicketRefresh = authConfig.getSystemTicketRefresh() > 0
                ? Duration.of(authConfig.getSystemTicketRefresh(), ChronoUnit.SECONDS)
                : DEFAULT_SYSTEM_TICKET_REFRESH;

        var systemUser = new UserInfo();
        systemUser.setUserId(systemUserId);
        systemUser.setDisplayName(systemUserName);

        this.tokenProcessor = tokenProcessor;
        this.systemUser = systemUser;
        this.systemTicketDuration = systemTicketDuration;
        this.systemTicketRefresh = systemTicketRefresh;
    }

    public CallCredentials createDelegateSession(UserInfo delegate, Duration sessionTimeout) {

        var issue = Instant.now();
        var expiry = issue.plus(systemTicketDuration);
        var limit = issue.plus(sessionTimeout);

        var session = new SessionInfo();
        session.setUserInfo(systemUser);
        session.setDelegate(delegate);
        session.setIssueTime(issue);
        session.setExpiryTime(expiry);
        session.setExpiryLimit(limit);

        return new InternalCallCredentials(
                systemTicketDuration,
                systemTicketRefresh,
                session, tokenProcessor);
    }
}
