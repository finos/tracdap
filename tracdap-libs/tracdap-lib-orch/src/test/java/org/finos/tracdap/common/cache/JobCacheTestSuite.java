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

import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagHeader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;


public abstract class JobCacheTestSuite {

    public static final Duration TICKET_TIMEOUT = Duration.ofSeconds(5);

    // To use the test suite, create the cache instance in a @BeforeAll method
    protected static IJobCache<DummyState> cache;


    // -----------------------------------------------------------------------------------------------------------------
    // LIFECYCLE TESTS (high level smoke testing)
    // -----------------------------------------------------------------------------------------------------------------


    @Test
    void lifecycle_createReadDelete() {

        var key = UUID.randomUUID().toString();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        CacheEntry<DummyState> cacheEntry;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cacheEntry = cache.readEntry(ticket);
        }

        Assertions.assertEquals(key, cacheEntry.key());
        Assertions.assertEquals(revision, cacheEntry.revision());
        Assertions.assertEquals(status, cacheEntry.status());
        Assertions.assertEquals(value, cacheEntry.value());

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cache.deleteEntry(ticket);
        }

        var removedVEntry = cache.queryKey(key);

        Assertions.assertTrue(removedVEntry.isEmpty());
    }

    @Test
    void lifecycle_createAndUpdate() {

        var key = UUID.randomUUID().toString();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var status2 = "status2";
        int revision2;
        DummyState value2;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            var cacheEntry = cache.readEntry(ticket);
            var cacheValue = cacheEntry.value();
            cacheValue.intVar += 1;
            cacheValue.stringVar += ", these are not";

            revision2 = cache.updateEntry(ticket, status2, cacheValue);
            value2 = cacheValue;
        }

        CacheEntry<DummyState> cacheEntry;

        try (var ticket = cache.openTicket(key, revision2, TICKET_TIMEOUT)) {
            cacheEntry = cache.readEntry(ticket);
        }

        Assertions.assertEquals(key, cacheEntry.key());
        Assertions.assertEquals(revision2, cacheEntry.revision());
        Assertions.assertEquals(status2, cacheEntry.status());
        Assertions.assertEquals(value2, cacheEntry.value());

        try (var ticket = cache.openTicket(key, revision2, TICKET_TIMEOUT)) {
            cache.deleteEntry(ticket);
        }

        var removedVEntry = cache.queryKey(key);

        Assertions.assertTrue(removedVEntry.isEmpty());
    }

    @Test
    void lifecycle_dataTypes() {

        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";
        value.blobVar = new byte[1234];
        new Random().nextBytes(value.blobVar);
        value.objectId = TagHeader.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(Instant.now()))
                .setTagVersion(2)
                .setTagTimestamp(MetadataCodec.encodeDatetime(Instant.now()))
                .build();
        value.exception = new CompletionException(new RuntimeException("out msg", new RuntimeException("inner msg")));

        var key = UUID.randomUUID().toString();
        var status = "status1";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        CacheEntry<DummyState> cacheEntry;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cacheEntry = cache.readEntry(ticket);
        }

        Assertions.assertEquals(key, cacheEntry.key());
        Assertions.assertEquals(revision, cacheEntry.revision());
        Assertions.assertEquals(status, cacheEntry.status());

        // Check all the members of value

        Assertions.assertEquals(value.intVar, cacheEntry.value().intVar);
        Assertions.assertEquals(value.stringVar, cacheEntry.value().stringVar);
        Assertions.assertArrayEquals(value.blobVar, cacheEntry.value().blobVar);
        Assertions.assertEquals(value.objectId, cacheEntry.value().objectId);
        // Can't compare equality of exceptions directly, so check message and stack trace
        Assertions.assertEquals(value.exception.getMessage(), cacheEntry.value().exception.getMessage());
        Assertions.assertArrayEquals(value.exception.getStackTrace(), cacheEntry.value().exception.getStackTrace());
    }

    @Test
    void lifecycle_transientValues() {

        var key = UUID.randomUUID().toString();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";
        value.transientVar = "ephemeral content";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        CacheEntry<DummyState> cacheEntry;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cacheEntry = cache.readEntry(ticket);
        }

        // Regular vars are persisted, transient vars are not

        Assertions.assertEquals(42, cacheEntry.value().intVar);
        Assertions.assertEquals("the droids you're looking for", cacheEntry.value().stringVar);
        Assertions.assertNull(cacheEntry.value().transientVar);

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            var updateEntry = cache.readEntry(ticket);
            var updateValue = updateEntry.value();
            updateValue.intVar += 1;
            updateValue.transientVar = "more ephemeral data";

            cache.updateEntry(ticket, "status2", updateValue);
        }

        var modifiedEntry = cache.queryKey(key);

        Assertions.assertTrue(modifiedEntry.isPresent());
        Assertions.assertTrue(modifiedEntry.get().revision() > revision);
        Assertions.assertEquals("status2", modifiedEntry.get().status());

        // The intVar member should be updated
        // transientVar should still be null

        Assertions.assertEquals(43, modifiedEntry.get().value().intVar);
        Assertions.assertNull(modifiedEntry.get().value().transientVar);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // TICKETS
    // -----------------------------------------------------------------------------------------------------------------


    @Test
    void openNewTicket_ok() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        // Basic use of openNewTicket

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            Thread.sleep(1);

            Assertions.assertEquals(key, ticket.key());
            Assertions.assertEquals(0, ticket.revision());
            Assertions.assertFalse(ticket.missing());
            Assertions.assertFalse(ticket.superseded());
            Assertions.assertTrue(ticket.grantTime().isBefore(Instant.now()));
            Assertions.assertTrue(ticket.expiry().isAfter(Instant.now()));

            revision = cache.createEntry(ticket, status, value);
        }

        // Check result is as expected

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(key, entry.get().key());
        Assertions.assertEquals(revision, entry.get().revision());
        Assertions.assertEquals(value, entry.get().value());

        // First revision should be numbered 1
        Assertions.assertEquals(1, entry.get().revision());
    }

    @Test
    void openNewTicket_badKey() {

        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, null, TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, "", TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, "_reserved", TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, "trac_reserved", TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, "$%#3&--!", TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, "你好", TICKET_TIMEOUT));
    }

    @Test
    void openNewTicket_badDuration() {

        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, newKey(), null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, newKey(), Duration.ZERO));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, newKey(), Duration.ofSeconds(-1)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openNewTicket, newKey(), Duration.ofNanos(-1)));
    }

    @Test
    void openNewTicket_duplicate() {

        var key = newKey();

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            Assertions.assertFalse(ticket.superseded());
            Assertions.assertFalse(ticket.missing());

            // Ticket is superseded because another ticket exists to add this key
            // The missing flag is not set for openNewTicket()
            try (var ticket2 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
                Assertions.assertTrue(ticket2.superseded());
                Assertions.assertFalse(ticket2.missing());
            }

            // Ticket is superseded because another ticket is open for this key
            // Missing flag is not set, because a ticket is open even though the entry isn't added yet
            try (var ticket3 = cache.openTicket(key, ticket.revision(), TICKET_TIMEOUT)) {
                Assertions.assertTrue(ticket3.superseded());
                Assertions.assertFalse(ticket3.missing());
            }
        }
    }

    @Test
    void openNewTicket_unused() {

        // Ticket closed without being used, key is available to retry the add operation

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket.superseded());
            Assertions.assertFalse(ticket.missing());
        }

        try (var ticket2 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket2.superseded());
            Assertions.assertFalse(ticket2.missing());
            cache.createEntry(ticket2, status, value);
        }

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(key, entry.get().key());
        Assertions.assertEquals(1, entry.get().revision());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void openNewTicket_durationTooLong() {

        // One day ticket duration should exceed the max grant

        // TODO: What is the right behavior here?

        Assertions.assertThrows(ECacheTicket.class, () -> failToOpen(cache::openNewTicket, newKey(), Duration.ofDays(1)));
    }

    @Test
    void openNewTicket_entryExists() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            cache.createEntry(ticket, status, value);
        }

        // Check entry was created

        var entry = cache.queryKey(key);
        Assertions.assertNotNull(entry);

        // Opening a new ticket for an existing key should create a superseded ticket
        // Trying to use a superseded ticket should result in ECacheTicket

        try (var ticket2 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket2.superseded());
            Assertions.assertThrows(ECacheTicket.class, () -> cache.createEntry(ticket2, status, value));
            Assertions.assertThrows(ECacheTicket.class, () -> cache.deleteEntry(ticket2));
        }
    }

    @Test
    void openNewTicket_entryDeleted() {

        // After an entry is removed, it should be possible to get a ticket to create it again
        // The cache doesn't know anything about entries after they are deleted

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cache.deleteEntry(ticket2);
        }

        try (var ticket3 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket3.missing());
            Assertions.assertFalse(ticket3.superseded());
        }
    }

    @Test
    @SuppressWarnings("resource")
    void openNewTicket_timeoutNoAction() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        // Open a ticket and do nothing with it
        // The ticket should expire even if it is not explicitly closed

        var ticket1 = cache.openNewTicket(key, Duration.ofMillis(50));
        Thread.sleep(100);
        Assertions.assertTrue(ticket1.expiry().isBefore(Instant.now()));

        // The entry was not created, so getting a regular ticket should not be possible

        try (var ticket2 = cache.openTicket(key, 1, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket2.missing());
            Assertions.assertFalse(ticket2.superseded());
        }

        // After the timeout expires, get a new ticket to create the same key

        try (var ticket3 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            cache.createEntry(ticket3, status, value);
        }

        // First revision number should not be affected by the expired ticket

        var entry = cache.queryKey(key);
        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(1, entry.get().revision());
    }

    @Test
    void openNewTicket_timeoutAfterCreate() throws Exception {

        // Tickets are not transactions, updates take effect even if the ticket is not closed
        // The close() just releases the ticket so other operations can happen without waiting for timeout

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        // Open a ticket and do nothing with it
        // The ticket should expire even if it is not explicitly closed

        var ticket1 = cache.openNewTicket(key, Duration.ofMillis(250));
        cache.createEntry(ticket1, status, value);

        Thread.sleep(300);

        var entry = cache.queryKey(key);
        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(1, entry.get().revision());

        Assertions.assertTrue(ticket1.expiry().isBefore(Instant.now()));

        // After the timeout, openNewTicket() should give a superseded result because the key exists

        try (var ticket2 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket2.superseded());
        }

        // After the first ticket expires, it should be possible to get a regular ticket for the same key

        try (var ticket2 = cache.openTicket(key, entry.get().revision(), TICKET_TIMEOUT)) {
            var t2Entry = cache.readEntry(ticket2);
            Assertions.assertEquals(value, t2Entry.value());
        }
    }

    @Test
    void openTicket_ok() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            cache.createEntry(ticket, status, value);
        }

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        int revision;

        // Basic use of openTicket()

        try (var ticket = cache.openTicket(key, 1, TICKET_TIMEOUT)) {

            Thread.sleep(1);

            Assertions.assertEquals(key, ticket.key());
            Assertions.assertEquals(1, ticket.revision());
            Assertions.assertFalse(ticket.missing());
            Assertions.assertFalse(ticket.superseded());
            Assertions.assertTrue(ticket.grantTime().isBefore(Instant.now()));
            Assertions.assertTrue(ticket.expiry().isAfter(Instant.now()));

            revision = cache.updateEntry(ticket, status2, value2);
        }

        // Check result is as expected

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(key, entry.get().key());
        Assertions.assertEquals(revision, entry.get().revision());
        Assertions.assertEquals(value2, entry.get().value());

        // First revision should be numbered 1
        Assertions.assertEquals(2, entry.get().revision());
    }

    @Test
    void openTicket_badKey() {

        // Since these are bad keys, it's not possible to prepare a valid initial entry

        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, null, 1, TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, "", 1, TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, "_reserved", 1, TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, "trac_reserved", 1, TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, "$%#3&--!", 1, TICKET_TIMEOUT));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, "你好", 1, TICKET_TIMEOUT));
    }

    @Test
    void openTicket_badRevision() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            cache.createEntry(ticket, status, value);
        }

        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, key, -1, TICKET_TIMEOUT));
    }

    @Test
    void openTicket_badDuration() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();

        int rev;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            rev = cache.createEntry(ticket, status, value);
        }

        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, key, rev, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, key, rev, Duration.ZERO));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, key, rev, Duration.ofSeconds(-1)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> failToOpen(cache::openTicket, key, rev, Duration.ofNanos(-1)));
    }

    @Test
    void openTicket_duplicate() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();

        int rev;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            rev = cache.createEntry(ticket, status, value);
        }

        try (var ticket = cache.openTicket(key, rev, TICKET_TIMEOUT)) {

            Assertions.assertFalse(ticket.superseded());
            Assertions.assertFalse(ticket.missing());

            try (var ticket2 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
                Assertions.assertTrue(ticket2.superseded());
                Assertions.assertFalse(ticket2.missing());
            }

            try (var ticket3 = cache.openTicket(key, rev, TICKET_TIMEOUT)) {
                Assertions.assertTrue(ticket3.superseded());
                Assertions.assertFalse(ticket3.missing());
            }
        }
    }

    @Test
    void openTicket_unused() {

        // Ticket closed without being used, key is available to retry the add operation

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int rev;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            rev = cache.createEntry(ticket, status, value);
        }

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        int rev2;

        try (var ticket = cache.openTicket(key, rev, TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket.superseded());
            Assertions.assertFalse(ticket.missing());
        }

        // The original revision can be re-used, since the ticket was closed without an update

        try (var ticket2 = cache.openTicket(key, rev, TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket2.superseded());
            Assertions.assertFalse(ticket2.missing());
            rev2 = cache.updateEntry(ticket2, status2, value2);
        }

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(key, entry.get().key());
        Assertions.assertEquals(rev2, entry.get().revision());
        Assertions.assertEquals(value2, entry.get().value());
    }

    @Test
    void openTicket_durationTooLong() {

        // One day ticket duration should exceed the max grant

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int rev;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            rev = cache.createEntry(ticket, status, value);
        }

        // TODO: What is the right behavior here?

        Assertions.assertThrows(ECacheTicket.class, () -> failToOpen(cache::openTicket, key, rev, Duration.ofDays(1)));
    }

    @Test
    void openTicket_missingEntry() {

        var key = newKey();
        var rev = 1;

        try (var ticket = cache.openTicket(key, rev, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket.missing());
            Assertions.assertFalse(ticket.superseded());
        }
    }

    @Test
    void openTicket_missingRevision() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int rev;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            rev = cache.createEntry(ticket, status, value);
        }

        try (var ticket = cache.openTicket(key, rev + 1, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket.missing());
            Assertions.assertFalse(ticket.superseded());
        }
    }

    @Test
    void openTicket_entryDeleted() {

        // After an entry is removed, it is not possible to open a regular ticket for read/update
        // To recreate the entry, it is necessary to use openNewTicket() and addEntry()
        // The cache doesn't know anything about entries after they are deleted

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cache.deleteEntry(ticket2);
        }

        try (var ticket3 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket3.missing());
            Assertions.assertFalse(ticket3.superseded());
        }
    }

    @Test
    @SuppressWarnings("resource")
    void openTicket_timeoutNoAction() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        // Open a ticket on the new entry and let it time out

        var unusedTicket = cache.openTicket(key, revision, Duration.ofMillis(50));
        Thread.sleep(100);
        Assertions.assertTrue(unusedTicket.expiry().isBefore(Instant.now()));

        // Now perform a regular update - it should be fine to grant a new ticket now

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        int revision2;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            Thread.sleep(1);

            Assertions.assertEquals(key, ticket.key());
            Assertions.assertEquals(1, ticket.revision());
            Assertions.assertFalse(ticket.missing());
            Assertions.assertFalse(ticket.superseded());
            Assertions.assertTrue(ticket.grantTime().isBefore(Instant.now()));
            Assertions.assertTrue(ticket.expiry().isAfter(Instant.now()));

            revision2 = cache.updateEntry(ticket, status2, value2);
        }

        // Check result is as expected

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(key, entry.get().key());
        Assertions.assertEquals(revision2, entry.get().revision());
        Assertions.assertEquals(value2, entry.get().value());

        // First revision should be numbered 1
        Assertions.assertEquals(2, entry.get().revision());
    }

    @Test
    void openTicket_timeoutAfterUpdate() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        // Open a ticket that will not be closed

        var unclosedTicket = cache.openTicket(key, revision, Duration.ofMillis(250));

        // Now perform a regular update - it should be fine to grant a new ticket now

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        cache.updateEntry(unclosedTicket, status2, value2);

        // Now let the ticket expire

        Thread.sleep(300);

        // Looking up the entry, the new revision should be available

        var entry = cache.queryKey(key);
        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(2, entry.get().revision());
        Assertions.assertEquals(status2, entry.get().status());
        Assertions.assertEquals(value2, entry.get().value());

        // The original revision should be superseded

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket2.missing());
            Assertions.assertTrue(ticket2.superseded());
        }

        // But it should be possible to get a ticket on the latest revision

        try (var ticket3 = cache.openTicket(key, entry.get().revision(), TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket3.missing());
            Assertions.assertFalse(ticket3.superseded());
        }

        // Getting a ticket for a new entry should also not be possible

        try (var ticket3 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket3.missing());
            Assertions.assertTrue(ticket3.superseded());
        }
    }

    @Test
    void closeTicket_ok() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        // Create a ticket it and use it to read an entry

        var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT);

        var entry = cache.readEntry(ticket);
        Assertions.assertTrue(entry.cacheOk());
        Assertions.assertEquals(entry.value(), value);

        // After closing the ticket, any attempt to use it should throw ECacheTicket

        cache.closeTicket(ticket);

        Assertions.assertThrows(ECacheTicket.class, () -> cache.readEntry(ticket));
    }

    @Test
    void closeTicket_badTicket() {

        Assertions.assertThrows(IllegalArgumentException.class, () -> cache.closeTicket(null));

        // Create an invalid ticket - closing it should have no effect
        // In practice invalid tickets should never be created

        try (var ticket1 = CacheTicket.forDuration(cache, "", -1, Instant.now().minusSeconds(1), Duration.ofSeconds(-1))) {
            Assertions.assertDoesNotThrow(() -> cache.closeTicket(ticket1));
        }
    }

    @Test
    void closeTicket_unknownTicket() throws Exception {

        // Create a ticket that could be valid, but doesn't have a real entry in the cache
        // Closing tickets for missing entries is a no-op, the ticket didn't grant any rights anyway

        var key = newKey();
        var revision = 1;

        try (var ticket1 = CacheTicket.forDuration(cache, key, revision, Instant.now(), Duration.ofMinutes(1))) {
            Thread.sleep(100);
            Assertions.assertDoesNotThrow(() -> cache.closeTicket(ticket1));
        }
    }

    @Test
    void closeTicket_noAction() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        // Open a ticket it and close it without doing anything

        var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT);
        cache.closeTicket(ticket);

        Assertions.assertThrows(ECacheTicket.class, () -> cache.readEntry(ticket));

        // It should be possible to open a new ticket for the same key / revision

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            var entry = cache.readEntry(ticket2);
            Assertions.assertTrue(entry.cacheOk());
            Assertions.assertEquals(entry.value(), value);
        }
    }

    @Test
    @SuppressWarnings("resource")
    void closeTicket_afterTimeout() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        // Open a ticket it and let it time out without doing anything

        var ticket = cache.openTicket(key, revision, Duration.ofMillis(50));
        Thread.sleep(100);

        // It should not be possible to use the ticket

        Assertions.assertThrows(ECacheTicket.class, () -> cache.readEntry(ticket));

        // It should be possible to open a new ticket for the same key / revision

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            var entry = cache.readEntry(ticket2);
            Assertions.assertTrue(entry.cacheOk());
            Assertions.assertEquals(entry.value(), value);
        }

        // Closing the original ticket should not raise an error (this is a no-op)

        Assertions.assertDoesNotThrow(() -> cache.closeTicket(ticket));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // CACHE ENTRIES (CRUD OPERATIONS)
    // -----------------------------------------------------------------------------------------------------------------


    @Test
    void createEntry_ok() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision, entry.get().revision());
        Assertions.assertEquals(status, entry.get().status());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void createEntry_alreadyExists() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            revision = cache.createEntry(ticket, status, value);

            // Add the same entry a second time using the same ticket
            Assertions.assertThrows(ECacheDuplicate.class, () -> cache.createEntry(ticket, status, value));
        }

        // Make sure the entry exists
        var entry = cache.queryKey(key);
        Assertions.assertTrue(entry.isPresent());

        // Not possible to add again using a second new item ticket, because the ticket will be superseded
        try (var ticket2 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket2.superseded());
        }

        // Try using a regular ticket instead
        try (var ticket3 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            // Check ticket2 should be a valid ticket
            Assertions.assertFalse(ticket3.superseded());
            Assertions.assertFalse(ticket3.missing());

            // Add the same entry a second time using a new valid ticket
            Assertions.assertThrows(ECacheDuplicate.class, () -> cache.createEntry(ticket3, status, value));
        }
    }

    @Test
    void createEntry_afterRemoval() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        Assertions.assertTrue(cache.queryKey(key).isPresent());

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cache.deleteEntry(ticket2);
        }

        Assertions.assertFalse(cache.queryKey(key).isPresent());

        try (var ticket3 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            cache.createEntry(ticket3, "status2", value);
        }

        var entry = cache.queryKey(key);
        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals("status2", entry.get().status());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void createEntry_afterUnusedTicket() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            Assertions.assertFalse(ticket.superseded());
            Assertions.assertFalse(ticket.missing());

            // Do not use the ticket
        }

        // No entry created, it should be possible to open a fresh ticket and create

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            Assertions.assertFalse(ticket.superseded());
            Assertions.assertFalse(ticket.missing());

            revision = cache.createEntry(ticket, status, value);
        }

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision, entry.get().revision());
        Assertions.assertEquals(status, entry.get().status());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void createEntry_ticketUnknown() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        // Create a ticket without going through the cache implementation

        try (var ticket = CacheTicket.forDuration(cache, key, 0, Instant.now(), TICKET_TIMEOUT)) {
            Assertions.assertThrows(ECacheTicket.class, () -> cache.createEntry(ticket, status, value));
        }

        // TODO check this test
    }

    @Test
    void createEntry_ticketSuperseded() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            Assertions.assertFalse(ticket.superseded());

            try (var ticket2 = cache.openNewTicket(key, TICKET_TIMEOUT)) {

                Assertions.assertTrue(ticket2.superseded());
                Assertions.assertThrows(ECacheTicket.class, () -> cache.createEntry(ticket2, status, value));
            }
        }

        var entry = cache.queryKey(key);
        Assertions.assertFalse(entry.isPresent());
    }

    @Test
    void createEntry_ticketExpired() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        try (var ticket = cache.openNewTicket(key, Duration.ofMillis(50))) {

            // Wait for ticket to expire
            Thread.sleep(51);

            Assertions.assertThrows(ECacheTicket.class, () -> cache.createEntry(ticket, status, value));
        }

        var entry = cache.queryKey(key);
        Assertions.assertFalse(entry.isPresent());
    }

    @Test
    void createEntry_ticketNeverClosed() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        var ticket = cache.openNewTicket(key, TICKET_TIMEOUT);
        var revision = cache.createEntry(ticket, status, value);

        // Query the entry without closing the ticket - should still be fine

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision, entry.get().revision());
        Assertions.assertEquals(status, entry.get().status());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void createEntry_statusInvalid() {

        var key = newKey();
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            Assertions.assertThrows(ECacheOperation.class, () -> cache.createEntry(ticket, "", value));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.createEntry(ticket, " ", value));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.createEntry(ticket, "-", value));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.createEntry(ticket, " leading_space", value));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.createEntry(ticket, "trailing_space ", value));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.createEntry(ticket, "@%&^*&£", value));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.createEntry(ticket, "你好", value));
        }

        var entry = cache.queryKey(key);

        Assertions.assertFalse(entry.isPresent());
    }

    @Test
    void createEntry_statusReserved() {

        var key = newKey();
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            Assertions.assertThrows(ECacheOperation.class, () -> cache.createEntry(ticket, "_reserved_status", value));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.createEntry(ticket, "trac_reserved_status", value));
        }

        var entry = cache.queryKey(key);

        Assertions.assertFalse(entry.isPresent());
    }

    @Test
    void createEntry_nulls() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            Assertions.assertThrows(ETracInternal.class, () -> cache.createEntry(null, status, value));
            Assertions.assertThrows(ETracInternal.class, () -> cache.createEntry(ticket, null, value));
            Assertions.assertThrows(ETracInternal.class, () -> cache.createEntry(ticket, status, null));
        }

        var entry = cache.queryKey(key);

        Assertions.assertFalse(entry.isPresent());
    }

    @Test
    void readEntry_ok() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            revision = cache.createEntry(ticket, status, value);

            // Read inside original ticket

            var entry = cache.readEntry(ticket);
            Assertions.assertTrue(entry.cacheOk());
            Assertions.assertEquals(revision, entry.revision());
            Assertions.assertEquals(status, entry.status());
            Assertions.assertEquals(value, entry.value());
        }

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            // Read in subsequent ticket

            var entry = cache.readEntry(ticket);
            Assertions.assertTrue(entry.cacheOk());
            Assertions.assertEquals(revision, entry.revision());
            Assertions.assertEquals(status, entry.status());
            Assertions.assertEquals(value, entry.value());
        }
    }

    @Test
    void readEntry_afterUpdate() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {

            revision = cache.createEntry(ticket, status, value);
        }

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        int revision2;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            revision2 = cache.updateEntry(ticket, status2, value2);
        }

        try (var ticket = cache.openTicket(key, revision2, TICKET_TIMEOUT)) {

            var entry = cache.readEntry(ticket);
            Assertions.assertTrue(entry.cacheOk());
            Assertions.assertEquals(revision2, entry.revision());
            Assertions.assertEquals(status2, entry.status());
            Assertions.assertEquals(value2, entry.value());
        }
    }

    @Test
    void readEntry_afterRemoval() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var entry = cache.queryKey(key);
        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision, entry.get().revision());
        Assertions.assertEquals(status, entry.get().status());
        Assertions.assertEquals(value, entry.get().value());

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            // Ticket is valid but entry removed, error should be ECacheNotFound
            cache.deleteEntry(ticket);
            Assertions.assertThrows(ECacheNotFound.class, () -> cache.readEntry(ticket));
        }

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            // Ticket is in missing status, error should be ECacheTicket
            Assertions.assertTrue(ticket.missing());
            Assertions.assertThrows(ECacheTicket.class, () -> cache.readEntry(ticket));
        }

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            // New ticket is not in missing status, error should be ECacheNotFound
            Assertions.assertFalse(ticket.missing());
            Assertions.assertThrows(ECacheNotFound.class, () -> cache.readEntry(ticket));
        }
    }

    @Test
    void readEntry_afterUnusedTicket() {

        var key = newKey();
        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = ticket.revision();
        }

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            // Ticket will be in missing status, error should be ECacheTicket
            Assertions.assertTrue(ticket.missing());
            Assertions.assertThrows(ECacheTicket.class, () -> cache.readEntry(ticket));
        }

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            // New ticket is not in missing status, error should be ECacheNotFound
            Assertions.assertFalse(ticket.missing());
            Assertions.assertThrows(ECacheNotFound.class, () -> cache.readEntry(ticket));
        }
    }

    @Test
    void readEntry_missing() {

        var key = newKey();

        try (var ticket = cache.openTicket(key, 1, TICKET_TIMEOUT)) {
            // Ticket will be in missing status, error should be ECacheTicket
            Assertions.assertTrue(ticket.missing());
            Assertions.assertThrows(ECacheTicket.class, () -> cache.readEntry(ticket));
        }

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            // New ticket is not in missing status, error should be ECacheNotFound
            Assertions.assertFalse(ticket.missing());
            Assertions.assertThrows(ECacheNotFound.class, () -> cache.readEntry(ticket));
        }
    }

    @Test
    void readEntry_ticketUnknown() {

        // Add a valid entry

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        // Try to read the entry using a ticket that was not created by the cache implementation

        try (var ticket = CacheTicket.forDuration(cache, key, revision, Instant.now(), TICKET_TIMEOUT)) {
            Assertions.assertThrows(ECacheTicket.class, () -> cache.readEntry(ticket));
        }
    }

    @Test
    void readEntry_ticketSuperseded() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            Assertions.assertFalse(ticket.superseded());

            try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

                Assertions.assertTrue(ticket2.superseded());
                Assertions.assertThrows(ECacheTicket.class, () -> cache.readEntry(ticket2));
            }
        }
    }

    @Test
    void readEntry_ticketExpired() throws InterruptedException {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        try (var ticket = cache.openTicket(key, revision, Duration.ofMillis(50))) {

            // Wait for ticket to expire
            Thread.sleep(51);

            Assertions.assertThrows(ECacheTicket.class, () -> cache.readEntry(ticket));
        }
    }

    @Test
    void readEntry_ticketNeverClosed() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        var ticketDuration = Duration.ofMillis(250);
        var ticket = cache.openNewTicket(key, ticketDuration);

        var revision = cache.createEntry(ticket, status, value);

        Thread.sleep(ticketDuration.toMillis() + 1);

        // Query the entry without closing the ticket - should still be fine

        CacheEntry<DummyState> entry;

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            entry = cache.readEntry(ticket2);
        }

        Assertions.assertTrue(entry.cacheOk());
        Assertions.assertEquals(revision, entry.revision());
        Assertions.assertEquals(status, entry.status());
        Assertions.assertEquals(value, entry.value());
    }

    @Test
    void readEntry_nulls() {

        Assertions.assertThrows(ETracInternal.class, () -> cache.readEntry(null));
    }

    @Test
    void updateEntry_ok() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        int revision2;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            revision2 = cache.updateEntry(ticket, status2, value2);
        }

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision2, entry.get().revision());
        Assertions.assertEquals(status2, entry.get().status());
        Assertions.assertEquals(value2, entry.get().value());
    }

    @Test
    void updateEntry_noChanges() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        int revision2;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            revision2 = cache.updateEntry(ticket, status, value);
        }

        // Revision should still increase
        // Job cache does not try to compare between versions
        Assertions.assertTrue(revision2 > revision);

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision2, entry.get().revision());
        Assertions.assertEquals(status, entry.get().status());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void updateEntry_sameStatus() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var status2 = "status2";

        int revision2;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            revision2 = cache.updateEntry(ticket, status2, value);
        }

        Assertions.assertTrue(revision2 > revision);

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision2, entry.get().revision());
        Assertions.assertEquals(status2, entry.get().status());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void updateEntry_sameValue() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        int revision2;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            revision2 = cache.updateEntry(ticket, status, value2);
        }

        Assertions.assertTrue(revision2 > revision);

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision2, entry.get().revision());
        Assertions.assertEquals(status, entry.get().status());
        Assertions.assertEquals(value2, entry.get().value());
    }

    @Test
    void updateEntry_afterRemoval() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            // Ticket is valid but entry is already deleted, error should be ECacheNoteFound
            cache.deleteEntry(ticket);
            Assertions.assertThrows(ECacheNotFound.class, () -> cache.updateEntry(ticket, status2, value2));
        }

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            // When a new ticket is opened, ticket status is missing so error is ECacheTicket
            Assertions.assertTrue(ticket.missing());
            Assertions.assertThrows(ECacheTicket.class, () -> cache.updateEntry(ticket, status2, value2));
        }
    }

    @Test
    void updateEntry_afterUnusedTicket() {

        var key = newKey();

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket.missing() || ticket.superseded());
        }

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        try (var ticket = cache.openTicket(key, 0, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket.missing());
            Assertions.assertThrows(ECacheTicket.class, ()-> cache.updateEntry(ticket, status2, value2));
        }
    }

    @Test
    void updateEntry_missing() {

        var key = newKey();

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        try (var ticket = cache.openTicket(key, 0, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket.missing());
            Assertions.assertThrows(ECacheTicket.class, ()-> cache.updateEntry(ticket, status2, value2));
        }
    }

    @Test
    void updateEntry_ticketUnknown() {

        // Add a valid entry

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        // Try to update the entry using a ticket that was not created by the cache implementation

        var status2 = "status1";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        try (var ticket = CacheTicket.forDuration(cache, key, revision, Instant.now(), TICKET_TIMEOUT)) {
            Assertions.assertThrows(ECacheTicket.class, () -> cache.createEntry(ticket, status2, value2));
        }
    }

    @Test
    void updateEntry_ticketSuperseded() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var status2 = "status1";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            Assertions.assertFalse(ticket.superseded());

            try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

                Assertions.assertTrue(ticket2.superseded());
                Assertions.assertThrows(ECacheTicket.class, () -> cache.updateEntry(ticket2, status2, value2));
            }
        }
    }

    @Test
    void updateEntry_ticketExpired() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var status2 = "status1";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        try (var ticket = cache.openTicket(key, revision, Duration.ofMillis(50))) {

            // Wait for ticket to expire
            Thread.sleep(51);

            Assertions.assertThrows(ECacheTicket.class, () -> cache.updateEntry(ticket, status2, value2));
        }
    }

    @Test
    void updateEntry_ticketNeverClosed() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var ticketDuration = Duration.ofMillis(250);
        var ticket = cache.openTicket(key, revision, ticketDuration);

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "not the droids you're looking for";

        var revision2 = cache.updateEntry(ticket, status2, value2);

        Thread.sleep(ticketDuration.toMillis() + 1);

        // Query the entry without closing the ticket - should still be fine

        CacheEntry<DummyState> entry;

        try (var ticket2 = cache.openTicket(key, revision2, TICKET_TIMEOUT)) {
            entry = cache.readEntry(ticket2);
        }

        Assertions.assertTrue(entry.cacheOk());
        Assertions.assertEquals(revision2, entry.revision());
        Assertions.assertEquals(status2, entry.status());
        Assertions.assertEquals(value2, entry.value());
    }

    @Test
    void updateEntry_statusInvalid() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            Assertions.assertThrows(ECacheOperation.class, () -> cache.updateEntry(ticket, "", value2));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.updateEntry(ticket, " ", value2));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.updateEntry(ticket, "-", value2));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.updateEntry(ticket, " leading_space", value2));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.updateEntry(ticket, "trailing_space ", value2));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.updateEntry(ticket, "@%&^*&£", value2));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.updateEntry(ticket, "你好", value2));
        }

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision, entry.get().revision());
        Assertions.assertEquals(status, entry.get().status());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void updateEntry_statusReserved() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            Assertions.assertThrows(ECacheOperation.class, () -> cache.updateEntry(ticket, "_reserved_status", value2));
            Assertions.assertThrows(ECacheOperation.class, () -> cache.updateEntry(ticket, "trac_reserved_status", value2));
        }

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision, entry.get().revision());
        Assertions.assertEquals(status, entry.get().status());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void updateEntry_nulls() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var status2 = "status2";
        var value2 = new DummyState();
        value2.intVar = 43;
        value2.stringVar = "move along";

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            Assertions.assertThrows(ETracInternal.class, () -> cache.updateEntry(null, status2, value2));
            Assertions.assertThrows(ETracInternal.class, () -> cache.updateEntry(ticket, null, value2));
            Assertions.assertThrows(ETracInternal.class, () -> cache.updateEntry(ticket, status2, null));
        }
    }

    @Test
    void deleteEntry_ok() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cache.deleteEntry(ticket);
        }

        var entry = cache.queryKey(key);

        Assertions.assertFalse(entry.isPresent());
    }

    @Test
    void deleteEntry_missing() {

        var key = newKey();

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertThrows(ECacheNotFound.class, () -> cache.deleteEntry(ticket));
        }

        try (var ticket = cache.openTicket(key, 0, TICKET_TIMEOUT)) {
            Assertions.assertThrows(ECacheTicket.class, () -> cache.deleteEntry(ticket));
        }

        var entry = cache.queryKey(key);

        Assertions.assertFalse(entry.isPresent());
    }

    @Test
    void deleteEntry_afterRemoval() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            // Ticket is valid but entry is already removed, error should be ECacheNotFound
            cache.deleteEntry(ticket);
            Assertions.assertThrows(ECacheNotFound.class, () -> cache.deleteEntry(ticket));
        }

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            // Ticket is not valid, error should be ECacheTicket
            Assertions.assertThrows(ECacheTicket.class, () -> cache.deleteEntry(ticket));
        }

        var entry = cache.queryKey(key);

        Assertions.assertFalse(entry.isPresent());
    }

    @Test
    void deleteEntry_ticketUnknown() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        // Try to delete the entry using a ticket that was not created by the cache implementation

        try (var ticket = CacheTicket.forDuration(cache, key, revision, Instant.now(), TICKET_TIMEOUT)) {
            Assertions.assertThrows(ECacheTicket.class, () -> cache.deleteEntry(ticket));
        }

        // Entry should not have been affected

        var entry = cache.queryKey(key);

        Assertions.assertTrue(entry.isPresent());
        Assertions.assertEquals(revision, entry.get().revision());
        Assertions.assertEquals(status, entry.get().status());
        Assertions.assertEquals(value, entry.get().value());
    }

    @Test
    void deleteEntry_ticketSuperseded() {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            Assertions.assertFalse(ticket.superseded());

            try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

                Assertions.assertTrue(ticket2.superseded());
                Assertions.assertThrows(ECacheTicket.class, () -> cache.deleteEntry(ticket2));
            }
        }
    }

    @Test
    void deleteEntry_ticketExpired() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        try (var ticket = cache.openTicket(key, revision, Duration.ofMillis(50))) {

            // Wait for ticket to expire
            Thread.sleep(51);

            Assertions.assertThrows(ECacheTicket.class, () -> cache.deleteEntry(ticket));
        }
    }

    @Test
    void deleteEntry_ticketNeverClosed() throws Exception {

        var key = newKey();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.createEntry(ticket, status, value);
        }

        var ticketDuration = Duration.ofMillis(250);
        var ticket = cache.openTicket(key, revision, ticketDuration);

        cache.deleteEntry(ticket);

        Thread.sleep(ticketDuration.toMillis() + 1);

        // Query the entry without closing the ticket - should still be fine

        var entry = cache.queryKey(key);

        Assertions.assertFalse(entry.isPresent());
    }

    @Test
    void deleteEntry_nulls() {

        Assertions.assertThrows(ETracInternal.class, () -> cache.deleteEntry(null));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS AND EXTRAS
    // -----------------------------------------------------------------------------------------------------------------


    private String newKey() {
        return UUID.randomUUID().toString();
    }

    private <T, U> void failToOpen(BiFunction<T, U, AutoCloseable> openFunc, T param1, U param2) throws Exception {

        try (var closable = openFunc.apply(param1, param2)) {
            Assertions.fail("unexpectedly succeeded creating [" + closable.getClass().getSimpleName() + "]");
        }
    }

    private <T, U, V> void failToOpen(TriFunction<T, U, V, AutoCloseable> openFunc, T param1, U param2, V param3) throws Exception {

        try (var closable = openFunc.apply(param1, param2, param3)) {
            Assertions.fail("unexpectedly succeeded creating [" + closable.getClass().getSimpleName() + "]");
        }
    }

    protected static class DummyState implements Serializable {

        int intVar;
        String stringVar;
        byte[] blobVar;

        transient String transientVar;

        TagHeader objectId;
        Throwable exception;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DummyState that = (DummyState) o;

            if (intVar != that.intVar) return false;
            if (!Objects.equals(stringVar, that.stringVar)) return false;
            if (!Arrays.equals(blobVar, that.blobVar)) return false;
            if (!Objects.equals(objectId, that.objectId)) return false;
            return Objects.equals(exception, that.exception);
        }

        @Override
        public int hashCode() {
            int result = intVar;
            result = 31 * result + (stringVar != null ? stringVar.hashCode() : 0);
            result = 31 * result + Arrays.hashCode(blobVar);
            result = 31 * result + (objectId != null ? objectId.hashCode() : 0);
            result = 31 * result + (exception != null ? exception.hashCode() : 0);
            return result;
        }
    }

    @FunctionalInterface
    private interface TriFunction <T, U, V, X> {

        X apply(T param1, U param2, V param3);
    }
}
