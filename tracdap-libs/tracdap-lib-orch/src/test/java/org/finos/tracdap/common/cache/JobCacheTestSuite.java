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

import org.finos.tracdap.common.exception.ECacheTicket;
import org.finos.tracdap.metadata.TagHeader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.BiFunction;


public abstract class JobCacheTestSuite {

    public static final Duration TICKET_TIMEOUT = Duration.ofSeconds(5);

    // To use the test suite, create the cache instance in a @BeforeAll method
    protected static IJobCache<DummyState> cache;

    @Test
    void lifecycle_createReadDelete() {

        var key = UUID.randomUUID().toString();
        var status = "status1";
        var value = new DummyState();
        value.intVar = 42;
        value.stringVar = "the droids you're looking for";

        int revision;

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.addEntry(ticket, status, value);
        }

        CacheEntry<DummyState> cacheEntry;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cacheEntry = cache.getEntry(ticket);
        }

        Assertions.assertEquals(key, cacheEntry.key());
        Assertions.assertEquals(revision, cacheEntry.revision());
        Assertions.assertEquals(status, cacheEntry.status());
        Assertions.assertEquals(value, cacheEntry.value());

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
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

        try (var ticket = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            revision = cache.addEntry(ticket, status, value);
        }

        var status2 = "status2";
        int revision2;
        DummyState value2;

        try (var ticket = cache.openTicket(key, revision, TICKET_TIMEOUT)) {

            var cacheEntry = cache.getEntry(ticket);
            var cacheValue = cacheEntry.value();
            cacheValue.intVar += 1;
            cacheValue.stringVar += ", these are not";

            revision2 = cache.updateEntry(ticket, status2, cacheValue);
            value2 = cacheValue;
        }

        CacheEntry<DummyState> cacheEntry;

        try (var ticket = cache.openTicket(key, revision2, TICKET_TIMEOUT)) {
            cacheEntry = cache.getEntry(ticket);
        }

        Assertions.assertEquals(key, cacheEntry.key());
        Assertions.assertEquals(revision2, cacheEntry.revision());
        Assertions.assertEquals(status2, cacheEntry.status());
        Assertions.assertEquals(value2, cacheEntry.value());

        try (var ticket = cache.openTicket(key, revision2, TICKET_TIMEOUT)) {
            cache.removeEntry(ticket);
        }

        var removedVEntry = cache.lookupKey(key);

        Assertions.assertNull(removedVEntry);
    }

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

            revision = cache.addEntry(ticket, status, value);
        }

        // Check result is as expected

        var entry = cache.lookupKey(key);

        Assertions.assertNotNull(entry);
        Assertions.assertEquals(key, entry.key());
        Assertions.assertEquals(revision, entry.revision());
        Assertions.assertEquals(value, entry.value());

        // First revision should be numbered 1
        Assertions.assertEquals(1, entry.revision());
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
            // It is also missing, because no entry exists yet
            try (var ticket3 = cache.openTicket(key, ticket.revision(), TICKET_TIMEOUT)) {
                Assertions.assertTrue(ticket3.superseded());
                Assertions.assertTrue(ticket3.missing());
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
            cache.addEntry(ticket2, status, value);
        }

        var entry = cache.lookupKey(key);

        Assertions.assertNotNull(entry);
        Assertions.assertEquals(key, entry.key());
        Assertions.assertEquals(1, entry.revision());
        Assertions.assertEquals(value, entry.value());
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
            cache.addEntry(ticket, status, value);
        }

        // Check entry was created

        var entry = cache.lookupKey(key);
        Assertions.assertNotNull(entry);

        // Opening a new ticket for an existing key should create a superseded ticket
        // Trying to use a superseded ticket should result in ECacheTicket

        try (var ticket2 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket2.superseded());
            Assertions.assertThrows(ECacheTicket.class, () -> cache.addEntry(ticket2, status, value));
            Assertions.assertThrows(ECacheTicket.class, () -> cache.removeEntry(ticket2));
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
            revision = cache.addEntry(ticket, status, value);
        }

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cache.removeEntry(ticket2);
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
        // It should expire even if it is not explicitly closed

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
            cache.addEntry(ticket3, status, value);
        }

        var entry = cache.lookupKey(key);
        Assertions.assertNotNull(entry);

        // First revision number should not be affected by the expired ticket
        Assertions.assertEquals(1, entry.revision());
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
        // It should expire even if it is not explicitly closed

        var ticket1 = cache.openNewTicket(key, Duration.ofMillis(250));
        cache.addEntry(ticket1, status, value);

        Thread.sleep(300);

        var entry = cache.lookupKey(key);
        Assertions.assertNotNull(entry);
        Assertions.assertEquals(1, entry.revision());

        Assertions.assertTrue(ticket1.expiry().isBefore(Instant.now()));

        // After the timeout, openNewTicket() should give a superseded result because the key exists

        try (var ticket2 = cache.openNewTicket(key, TICKET_TIMEOUT)) {
            Assertions.assertTrue(ticket2.superseded());
        }

        // After the first ticket expires, it should be possible to get a regular ticket for the same key

        try (var ticket2 = cache.openTicket(key, entry.revision(), TICKET_TIMEOUT)) {
            var t2Entry = cache.getEntry(ticket2);
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
            cache.addEntry(ticket, status, value);
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

        var entry = cache.lookupKey(key);

        Assertions.assertNotNull(entry);
        Assertions.assertEquals(key, entry.key());
        Assertions.assertEquals(revision, entry.revision());
        Assertions.assertEquals(value2, entry.value());

        // First revision should be numbered 1
        Assertions.assertEquals(2, entry.revision());
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
            cache.addEntry(ticket, status, value);
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
            rev = cache.addEntry(ticket, status, value);
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
            rev = cache.addEntry(ticket, status, value);
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
            rev = cache.addEntry(ticket, status, value);
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

        var entry = cache.lookupKey(key);

        Assertions.assertNotNull(entry);
        Assertions.assertEquals(key, entry.key());
        Assertions.assertEquals(rev2, entry.revision());
        Assertions.assertEquals(value2, entry.value());
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
            rev = cache.addEntry(ticket, status, value);
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
            rev = cache.addEntry(ticket, status, value);
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
            revision = cache.addEntry(ticket, status, value);
        }

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            cache.removeEntry(ticket2);
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
            revision = cache.addEntry(ticket, status, value);
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

        var entry = cache.lookupKey(key);

        Assertions.assertNotNull(entry);
        Assertions.assertEquals(key, entry.key());
        Assertions.assertEquals(revision2, entry.revision());
        Assertions.assertEquals(value2, entry.value());

        // First revision should be numbered 1
        Assertions.assertEquals(2, entry.revision());
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
            revision = cache.addEntry(ticket, status, value);
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

        var entry = cache.lookupKey(key);
        Assertions.assertNotNull(entry);
        Assertions.assertEquals(2, entry.revision());
        Assertions.assertEquals(status2, entry.status());
        Assertions.assertEquals(value2, entry.value());

        // The original revision should be superseded

        try (var ticket2 = cache.openTicket(key, revision, TICKET_TIMEOUT)) {
            Assertions.assertFalse(ticket2.missing());
            Assertions.assertTrue(ticket2.superseded());
        }

        // But it should be possible to get a ticket on the latest revision

        try (var ticket3 = cache.openTicket(key, entry.revision(), TICKET_TIMEOUT)) {
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
        Assertions.fail();
    }

    @Test
    void closeTicket_badTicket() {
        Assertions.fail();
    }

    @Test
    void closeTicket_unknownTicket() {
        Assertions.fail();
    }

    @Test
    void closeTicket_noAction() {
        Assertions.fail();
    }

    @Test
    void closeTicket_afterTimeout() {
        Assertions.fail();
    }

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

    static class DummyState implements Serializable {

        int intVar;
        String stringVar;
        byte[] blobVar;

        transient String transientVar;

        TagHeader objectId;
        Throwable exception;
    }

    @FunctionalInterface
    private interface TriFunction <T, U, V, X> {

        X apply(T param1, U param2, V param3);
    }
}
