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

package org.finos.tracdap.svc.orch.cache;

import java.time.Duration;
import java.time.Instant;

public class Ticket implements AutoCloseable {

    private final IJobCache<?> cache;

    private final String key;
    private final int revision;
    private final Instant grantTime;
    private final Instant expiry;
    private final boolean superseded;
    private final boolean missing;

    public static Ticket missingEntryTicket(String key, int revision, Instant grantTime) {

        return new Ticket(null, key, revision, grantTime, grantTime, true, true);
    }

    public static Ticket supersededTicket(String key, int revision, Instant grantTime) {

        return new Ticket(null, key, revision, grantTime, grantTime, true, false);
    }

    public static Ticket forDuration(
            IJobCache<?> cache,
            String key, int revision,
            Instant grantTime, Duration grantDuration) {

        return new Ticket(cache, key, revision, grantTime, grantTime.plus(grantDuration), false, false);
    }

    protected Ticket(
            IJobCache<?> cache,
            String key, int iteration,
            Instant grantTime, Instant expiry,
            boolean superseded, boolean missing) {

        this.cache = cache;

        this.key = key;
        this.revision = iteration;
        this.grantTime = grantTime;
        this.expiry = expiry;
        this.superseded = superseded;
        this.missing = missing;
    }

    public String key() {
        return key;
    }

    public int revision() {
        return revision;
    }

    public Instant grantTime() {
        return grantTime;
    }

    public Instant expiry() {
        return expiry;
    }

    public boolean superseded() {
        return superseded || missing;
    }

    public boolean missing() {
        return missing;
    }

    @Override
    public void close() {

        if (cache != null)
            cache.closeTicket(this);
    }
}
