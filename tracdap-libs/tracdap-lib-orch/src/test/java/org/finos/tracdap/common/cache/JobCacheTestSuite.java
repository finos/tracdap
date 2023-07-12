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

package org.finos.tracdap.common.cache;

import org.finos.tracdap.common.cache.local.LocalJobCache;
import org.finos.tracdap.metadata.TagHeader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.UUID;


public abstract class JobCacheTestSuite {

    static class UnitTest extends JobCacheTestSuite {}

    static class DummyState implements Serializable {

        int intVar;
        String stringVar;
        byte[] blobVar;

        transient String transientVar;

        TagHeader objectId;
        Throwable exception;
    }

    private final IJobCache<DummyState> cache = new LocalJobCache<>();

    @Test
    void lifecycle_createReadDelete() {

        var key = UUID.randomUUID().toString();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key)) {
            revision = cache.addEntry(ticket, status, value);
        }

        CacheEntry<DummyState> cacheEntry;

        try (var ticket = cache.openTicket(key, revision)) {
            cacheEntry = cache.getEntry(ticket);
        }

        Assertions.assertEquals(key, cacheEntry.key());
        Assertions.assertEquals(revision, cacheEntry.revision());
        Assertions.assertEquals(status, cacheEntry.status());
        Assertions.assertEquals(value, cacheEntry.value());

        try (var ticket = cache.openTicket(key, revision)) {
            cache.removeEntry(ticket);
        }

        var removedVEntry = cache.lookupKey(key);

        Assertions.assertNull(removedVEntry);
    }

    @Test
    void lifecycle_createAndUpdate() {

        var key = UUID.randomUUID().toString();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key)) {
            revision = cache.addEntry(ticket, status, value);
        }

        var status2 = "status2";
        int revision2;
        DummyState value2;

        try (var ticket = cache.openTicket(key, revision)) {

            var cacheEntry = cache.getEntry(ticket);
            var cacheValue = cacheEntry.value();
            cacheValue.intVar += 1;
            cacheValue.stringVar += ", these are not";

            revision2 = cache.updateEntry(ticket, status2, cacheValue);
            value2 = cacheValue;
        }

        CacheEntry<DummyState> cacheEntry;

        try (var ticket = cache.openTicket(key, revision2)) {
            cacheEntry = cache.getEntry(ticket);
        }

        Assertions.assertEquals(key, cacheEntry.key());
        Assertions.assertEquals(revision2, cacheEntry.revision());
        Assertions.assertEquals(status2, cacheEntry.status());
        Assertions.assertEquals(value2, cacheEntry.value());

        try (var ticket = cache.openTicket(key, revision2)) {
            cache.removeEntry(ticket);
        }

        var removedVEntry = cache.lookupKey(key);

        Assertions.assertNull(removedVEntry);
    }


}
