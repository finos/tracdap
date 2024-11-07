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

package org.finos.tracdap.common.cache;

import java.time.Duration;
import java.time.Instant;


public final class CacheTicket implements AutoCloseable {

    private static final Instant NO_EXPIRY = Instant.MAX;

    private final IJobCache<?> cache;

    private final String key;
    private final int revision;
    private final Instant grantTime;
    private final Instant expiry;
    private final boolean superseded;
    private final boolean missing;

    public static CacheTicket missingEntryTicket(String key, int revision, Instant grantTime) {

        return new CacheTicket(null, key, revision, grantTime, NO_EXPIRY, false, true);
    }

    public static CacheTicket supersededTicket(String key, int revision, Instant grantTime) {

        return new CacheTicket(null, key, revision, grantTime, NO_EXPIRY, true, false);
    }

    public static CacheTicket forDuration(
            IJobCache<?> cache,
            String key, int revision,
            Instant grantTime, Duration grantDuration) {

        return new CacheTicket(cache, key, revision, grantTime, grantTime.plus(grantDuration), false, false);
    }

    private CacheTicket(
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
        return superseded || expiry.isBefore(Instant.now());
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
