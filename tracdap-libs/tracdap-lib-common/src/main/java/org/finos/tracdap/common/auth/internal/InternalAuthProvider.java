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
import io.grpc.Metadata;
import io.grpc.Status;
import org.finos.tracdap.config.AuthenticationConfig;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executor;


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
        var limit = issue.plus(sessionTimeout);
        return new DelegateSession(delegate, issue, limit);
    }

    private class DelegateSession extends CallCredentials {

        private final UserInfo delegate;
        private final Instant limit;

        private String token;
        private Instant refresh;

        private DelegateSession(UserInfo delegate, Instant issue, Instant limit) {

            this.delegate = delegate;
            this.limit = limit;

            this.token = regenerateToken(issue);
            this.refresh = issue.plus(systemTicketRefresh);
        }

        @Override
        public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {

            var now = Instant.now();

            if (now.isAfter(limit)) {
                applier.fail(Status.UNAUTHENTICATED.withDescription("Session timed out"));
                return;
            }

            if (now.isAfter(refresh))  {
                token = regenerateToken(now);
                refresh = now.plus(systemTicketRefresh);
            }

            var authHeaders = new Metadata();
            authHeaders.put(AuthConstants.TRAC_AUTH_TOKEN_KEY, token);
            applier.apply(authHeaders);
        }

        @Override
        public void thisUsesUnstableApi() {

        }

        private String regenerateToken(Instant refreshTime) {

            var expiryUnlimited = refreshTime.plus(systemTicketDuration);
            var expiryTime = expiryUnlimited.isBefore(limit) ? expiryUnlimited : limit;

            var session = new SessionInfo();
            session.setUserInfo(systemUser);
            session.setDelegate(delegate);
            session.setIssueTime(refreshTime);
            session.setExpiryTime(expiryTime);
            session.setExpiryLimit(limit);

            return tokenProcessor.encodeToken(session);
        }
    }
}
