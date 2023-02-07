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

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executor;


public class InternalCallCredentials extends CallCredentials implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Duration systemTicketDuration;
    private final Duration systemTicketRefresh;
    private final SessionInfo session;

    private String token;
    private Instant refresh;

    private transient JwtProcessor tokenProcessor;


    InternalCallCredentials(
            Duration systemTicketDuration,
            Duration systemTicketRefresh,
            SessionInfo session,
            JwtProcessor tokenProcessor) {

        this.systemTicketDuration = systemTicketDuration;
        this.systemTicketRefresh = systemTicketRefresh;
        this.session = session;

        this.tokenProcessor = tokenProcessor;
    }

    void setTokenProcessor(JwtProcessor tokenProcessor) {
        this.tokenProcessor = tokenProcessor;
    }

    @Override
    public void applyRequestMetadata(CallCredentials.RequestInfo requestInfo, Executor appExecutor, CallCredentials.MetadataApplier applier) {

        var now = Instant.now();

        if (now.isAfter(session.getExpiryLimit())) {
            applier.fail(Status.UNAUTHENTICATED.withDescription("Session timed out"));
            return;
        }

        if (token == null || now.isAfter(refresh)) {
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

        session.setIssueTime(refreshTime);

        var expiryUnlimited = refreshTime.plus(systemTicketDuration);

        if (expiryUnlimited.isAfter(session.getExpiryLimit()))
            session.setExpiryTime(expiryUnlimited);
        else
            session.setExpiryTime(session.getExpiryLimit());

        return tokenProcessor.encodeToken(session);
    }
}
