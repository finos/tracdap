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

package com.accenture.trac.svc.orch.cache.local;

import com.accenture.trac.svc.orch.cache.JobState;
import com.accenture.trac.svc.orch.cache.Ticket;

import java.time.Instant;

class LocalJobCacheEntry implements Cloneable {

    int revision;
    Instant lastActivity;
    Instant lastPoll;
    Ticket ticket;
    JobState jobState;

    @Override
    public LocalJobCacheEntry clone() {

        try {
            return (LocalJobCacheEntry) super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
