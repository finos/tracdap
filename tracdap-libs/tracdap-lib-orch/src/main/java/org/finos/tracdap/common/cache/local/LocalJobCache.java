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

package org.finos.tracdap.common.cache.local;

import org.finos.tracdap.common.cache.CacheEntry;
import org.finos.tracdap.common.cache.CacheHelpers;
import org.finos.tracdap.common.cache.CacheTicket;
import org.finos.tracdap.common.cache.IJobCache;
import org.finos.tracdap.common.exception.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class LocalJobCache<TValue extends Serializable> implements IJobCache<TValue> {

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

        CacheHelpers.checkKey(key);
        CacheHelpers.checkDuration(duration);
        CacheHelpers.checkMaxDuration(key, duration);

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

        CacheHelpers.checkKey(key);
        CacheHelpers.checkRevision(revision);
        CacheHelpers.checkDuration(duration);
        CacheHelpers.checkMaxDuration(key, duration);

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

        CacheHelpers.checkTicket(ticket);

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

        CacheHelpers.checkValidTicket(ticket, "create", commitTime);
        CacheHelpers.checkValidStatus(ticket, status);
        CacheHelpers.checkValidValue(ticket, value);

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
            newEntry.encodedValue = CacheHelpers.encodeValue(value);

            return newEntry;
        });

        return added.revision;
    }

    @Override
    public int updateEntry(CacheTicket ticket, String status, TValue value) {

        var commitTime = Instant.now();

        CacheHelpers.checkValidTicket(ticket, "update", commitTime);
        CacheHelpers. checkValidStatus(ticket, status);
        CacheHelpers.checkValidValue(ticket, value);

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
            newEntry.status = status;
            newEntry.encodedValue = CacheHelpers.encodeValue(value);

            return newEntry;
        });

        return updated.revision;
    }

    @Override
    public void deleteEntry(CacheTicket ticket) {

        var commitTime = Instant.now();

        CacheHelpers.checkValidTicket(ticket, "remove", commitTime);

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

        CacheHelpers.checkValidTicket(ticket, "get", accessTime);

        var entry = _cache.get(ticket.key());

        checkEntryMatchesTicket(entry, ticket, "get");

        if (entry == null || entry.encodedValue == null) {
            var message = String.format("Cannot get [%s], item is not in the cache", ticket.key());
            log.error(message);
            throw new ECacheNotFound(message);
        }

        var cacheValue = CacheHelpers.<TValue>decodeValue(entry.encodedValue);

        return CacheEntry.forValue(ticket.key(), entry.revision, entry.status, cacheValue);
    }

    @Override
    public Optional<CacheEntry<TValue>> queryKey(String key) {

        var entry = _cache.get(key);

        if (entry == null || entry.encodedValue == null)
            return Optional.empty();

        try {
            var cacheValue = CacheHelpers.<TValue>decodeValue(entry.encodedValue);
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
                var cacheValue = CacheHelpers.<TValue>decodeValue(_entry.encodedValue);
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

    private void checkEntryMatchesTicket(LocalJobCacheEntry entry, CacheTicket ticket, String operation) {

        if (entry == null || entry.ticket != ticket) {
            var message = String.format("Cannot %s [%s], ticket is not recognized", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }
    }
}
