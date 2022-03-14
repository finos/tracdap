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

package org.finos.tracdap.svc.orch.cache;

import java.time.Duration;
import java.time.Instant;

public class Ticket {

    private final String key;
    private final Instant grantTime;
    private final Instant expiry;
    private final boolean superseded;

    Ticket(String key, Instant grantTime, Instant expiry, boolean superseded) {
        this.key = key;
        this.grantTime = grantTime;
        this.expiry = expiry;
        this.superseded = superseded;
    }

    public static Ticket forDuration(String key, Instant grantTime, Duration duration) {
        var expiry = grantTime.plus(duration);
        return new Ticket(key, grantTime, expiry, false);
    }

    public static Ticket superseded(String key, Instant operationTime) {
        return new Ticket(key, operationTime, operationTime, true);
    }

    public String key() {
        return key;
    }

    public Instant grantTime() {
        return grantTime;
    }

    public Instant expiry() {
        return expiry;
    }

    public boolean superseded() {
        return superseded;
    }
}
