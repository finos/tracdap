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

package org.finos.tracdap.common.cache.local;

import org.finos.tracdap.common.cache.CacheEntry;
import org.finos.tracdap.common.cache.CacheTicket;
import org.finos.tracdap.common.cache.IJobCache;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.metadata.MetadataConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class LocalJobCache<TValue extends Serializable> implements IJobCache<TValue> {

    public static final Duration DEFAULT_TICKET_DURATION = Duration.of(30, ChronoUnit.SECONDS);
    public static final Duration MAX_TICKET_DURATION = Duration.of(5, ChronoUnit.MINUTES);

    private static final int FIRST_REVISION = 0;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConcurrentMap<String, LocalJobCacheEntry> _cache;

    // Package-local constructor
    // Instances must be created by LocalJobCacheManager

    LocalJobCache() {

        this._cache = new ConcurrentHashMap<>();
    }

    @Override
    public CacheTicket openNewTicket(String key, Duration duration) {

        checkKey(key);
        checkDuration(duration);
        checkMaxDuration(key, duration);

        var grantTime = Instant.now();
        var ticket = CacheTicket.forDuration(this, key, FIRST_REVISION, grantTime, duration);

        var cacheEntry = _cache.compute(key, (_key, priorEntry) -> {

            if (priorEntry != null) {

                if (priorEntry.encodedValue != null)
                    return priorEntry;

                if (priorEntry.ticket != null && priorEntry.ticket.expiry().isAfter(grantTime))
                    return priorEntry;
            }

            var newEntry = new LocalJobCacheEntry();
            newEntry.ticket = ticket;

            return newEntry;
        });

        if (cacheEntry.ticket != ticket)
            return CacheTicket.supersededTicket(key, FIRST_REVISION, grantTime);

        return ticket;
    }

    @Override
    public CacheTicket openTicket(String key, int revision, Duration duration) {

        checkKey(key);
        checkRevision(revision);
        checkDuration(duration);
        checkMaxDuration(key, duration);

        var grantTime = Instant.now();
        var ticket = CacheTicket.forDuration(this, key, revision, grantTime, duration);

        var cacheEntry = _cache.computeIfPresent(key, (_key, priorEntry) -> {

            if (priorEntry.revision != revision)
                return priorEntry;

            if (priorEntry.ticket != null && priorEntry.ticket.expiry().isAfter(grantTime))
                return priorEntry;

            var newEntry = priorEntry.clone();
            newEntry.ticket = ticket;

            return newEntry;
        });

        if (cacheEntry == null)
            return CacheTicket.missingEntryTicket(key, revision, grantTime);

        if (cacheEntry.ticket != ticket) {
            if (cacheEntry.revision >= revision)
                return CacheTicket.supersededTicket(key, revision, grantTime);
            else
                return CacheTicket.missingEntryTicket(key, revision, grantTime);
        }

        return ticket;
    }

    @Override
    public void closeTicket(CacheTicket ticket) {

        checkTicket(ticket);

        _cache.computeIfPresent(ticket.key(), (_key, priorEntry) -> {

            if (priorEntry.ticket != ticket)
                return priorEntry;

            if (priorEntry.encodedValue == null)
                return null;

            var newEntry = priorEntry.clone();
            newEntry.ticket = null;

            return newEntry;
        });
    }

    @Override
    public int createEntry(CacheTicket ticket, String status, TValue value) {

        var commitTime = Instant.now();

        checkValidTicket(ticket, "create", commitTime);

        var added = _cache.compute(ticket.key(), (_key, _entry) -> {

            checkEntryMatchesTicket(_entry, ticket, "create");

            if (_entry != null && _entry.encodedValue != null) {
                var message = String.format("Cannot create [%s], item is already in the cache", ticket.key());
                log.error(message);
                throw new ECacheDuplicate(message);
            }

            var newEntry = new LocalJobCacheEntry();
            newEntry.ticket = ticket;
            newEntry.revision = ticket.revision() + 1;
            newEntry.lastActivity = commitTime;
            newEntry.status = status;
            newEntry.encodedValue = encodeValue(value);

            return newEntry;
        });

        return added.revision;
    }

    @Override
    public int updateEntry(CacheTicket ticket, String stateKey, TValue value) {

        var commitTime = Instant.now();

        checkValidTicket(ticket, "update", commitTime);

        var updated = _cache.compute(ticket.key(), (_key, _entry) -> {

            checkEntryMatchesTicket(_entry, ticket, "update");

            if (_entry == null || _entry.encodedValue == null) {
                var message = String.format("Cannot update [%s], item is not in the cache", ticket.key());
                log.error(message);
                throw new ECacheNotFound(message);
            }

            var newEntry = _entry.clone();
            newEntry.revision += 1;
            newEntry.lastActivity = commitTime;
            newEntry.status = stateKey;
            newEntry.encodedValue = encodeValue(value);

            return newEntry;
        });

        return updated.revision;
    }

    @Override
    public void deleteEntry(CacheTicket ticket) {

        var commitTime = Instant.now();

        checkValidTicket(ticket, "remove", commitTime);

        _cache.compute(ticket.key(), (_key, _entry) -> {

            checkEntryMatchesTicket(_entry, ticket, "remove");

            if (_entry == null || _entry.encodedValue == null) {
                var message = String.format("Cannot remove [%s], item is not in the cache", ticket.key());
                log.error(message);
                throw new ECacheNotFound(message);
            }

            var newEntry = _entry.clone();
            newEntry.encodedValue = null;

            return newEntry;
        });
    }

    @Override
    public CacheEntry<TValue> readEntry(CacheTicket ticket) {

        var accessTime = Instant.now();

        checkValidTicket(ticket, "get", accessTime);

        var entry = _cache.get(ticket.key());

        checkEntryMatchesTicket(entry, ticket, "get");

        if (entry == null || entry.encodedValue == null) {
            var message = String.format("Cannot get [%s], item is not in the cache", ticket.key());
            log.error(message);
            throw new ECacheNotFound(message);
        }

        var cacheValue = decodeValue(entry.encodedValue);

        return CacheEntry.forValue(ticket.key(), entry.revision, entry.status, cacheValue);
    }

    @Override
    public Optional<CacheEntry<TValue>> queryKey(String key) {

        var entry = _cache.get(key);

        if (entry == null || entry.encodedValue == null)
            return Optional.empty();

        try {
            var cacheValue = decodeValue(entry.encodedValue);
            return Optional.of(CacheEntry.forValue(key, entry.revision, entry.status, cacheValue));
        }
        catch (ECacheCorruption cacheError) {
            return Optional.of(CacheEntry.error(key, entry.revision, entry.status, cacheError));
        }
    }

    @Override
    public List<CacheEntry<TValue>> queryStatus(List<String> statuses) {

        return queryStatus(statuses, false);
    }

    @Override
    public List<CacheEntry<TValue>> queryStatus(List<String> statuses, boolean includeOpenTickets) {

        var queryTime = Instant.now();

        var results = new ArrayList<CacheEntry<TValue>>();

        _cache.forEach((_key, _entry) -> {

            if (!statuses.contains(_entry.status))
                return;

            if (_entry.ticket != null && _entry.ticket.expiry().isAfter(queryTime))
                if (!includeOpenTickets)
                    return;

            // Errors decoding individual cache values must not invalidate the whole query
            // For any values with a decoding error, add an explicit error entry to the results
            // Client code can decide how to handle these errors, bad values can still be updated or removed

            try {
                var cacheValue = decodeValue(_entry.encodedValue);
                var result = CacheEntry.forValue(_key, _entry.revision, _entry.status, cacheValue);
                results.add(result);
            }
            catch (ECacheCorruption cacheError) {
                var result = CacheEntry.<TValue>error(_key, _entry.revision, _entry.status, cacheError);
                results.add(result);
            }
        });

        return results;
    }

    private void checkValidTicket(CacheTicket ticket, String operation, Instant operationTime) {

        if (ticket.missing()) {
            var message = String.format("Cannot %s [%s], item is not in the cache", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }

        if (ticket.superseded()) {
            var message = String.format("Cannot %s [%s], ticket has been superseded", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }

        if (operationTime.isAfter(ticket.expiry())) {
            var message = String.format("Failed to %s [%s], ticket has expired", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }
    }

    private void checkEntryMatchesTicket(LocalJobCacheEntry entry, CacheTicket ticket, String operation) {

        if (entry == null || entry.ticket != ticket) {
            var message = String.format("Cannot %s [%s], ticket is not recognized", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }
    }

    private void checkKey(String key) {

        if (key == null)
            throw new IllegalArgumentException(new NullPointerException());

        if (!VALID_KEY.matcher(key).matches())
            throw new IllegalArgumentException();

        if (MetadataConstants.TRAC_RESERVED_IDENTIFIER.matcher(key).matches())
            throw new IllegalArgumentException();
    }

    private void checkRevision(int revision) {

        if (revision < 0)
            throw new IllegalArgumentException();
    }

    private void checkDuration(Duration duration) {

        if (duration == null)
            throw new IllegalArgumentException(new NullPointerException());

        if (duration.isZero() || duration.isNegative())
            throw new IllegalArgumentException();
    }

    private void checkTicket(CacheTicket ticket) {

        if (ticket == null)
            throw new IllegalArgumentException(new NullPointerException());
    }

    private void checkMaxDuration(String key, Duration duration) {

        if (duration.compareTo(MAX_TICKET_DURATION) > 0) {

            var message = String.format(
                    "Requested ticket duration of [%s] for [%s] exceeds the maximum grant time",
                    duration, key);

            throw new ECacheTicket(message);
        }
    }

    private byte[] encodeValue(TValue value) {

        try (var stream = new ZCByteArrayOutputStream();
             var serialize = new ObjectOutputStream(stream)) {

            serialize.writeObject(value);
            serialize.flush();

            return stream.toByteArray();
        }
        catch (IOException e) {
            throw new EUnexpected(e);
        }
    }

    @SuppressWarnings("unchecked")
    private TValue decodeValue(byte[] encodedValue) throws ECacheCorruption {

        try (var stream = new ByteArrayInputStream(encodedValue);
             var deserialize = new ObjectInputStream(stream)) {

            var object = deserialize.readObject();

            return (TValue) object;
        }
        catch (InvalidClassException e) {
            var message = "The job cache references an old code version for class [" + e.classname + "]";
            throw new ECacheCorruption(message, e);
        }
        catch (ClassNotFoundException e) {
            var message = "The job cache references code for an unknown class [" + e.getMessage() + "]";
            throw new ECacheCorruption(message, e);
        }
        catch (IOException e) {
            var message = "The job cache contains a corrupt entry that cannot be decoded";
            throw new ECacheCorruption(message, e);
        }
    }

    private static class ZCByteArrayOutputStream extends ByteArrayOutputStream {
        @Override
        public byte[] toByteArray() {
            return this.buf;
        }
    }
}
