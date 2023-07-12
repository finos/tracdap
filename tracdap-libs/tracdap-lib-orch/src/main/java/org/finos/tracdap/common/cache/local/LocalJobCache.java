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

import org.finos.tracdap.common.cache.CacheQueryResult;
import org.finos.tracdap.common.cache.IJobCache;
import org.finos.tracdap.common.cache.Ticket;
import org.finos.tracdap.common.exception.ECacheNotFound;
import org.finos.tracdap.common.exception.ECacheTicket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class LocalJobCache<TValue> implements IJobCache<TValue> {

    public static final Duration DEFAULT_TICKET_DURATION = Duration.of(30, ChronoUnit.SECONDS);
    public static final Duration MAX_TICKET_DURATION = Duration.of(5, ChronoUnit.MINUTES);

    private static final int FIRST_REVISION = 0;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConcurrentMap<String, LocalJobCacheEntry<TValue>> _cache;

    public LocalJobCache() {

        this._cache = new ConcurrentHashMap<>();
    }

    @Override
    public Ticket openNewTicket(String key) {

        return openNewTicket(key, DEFAULT_TICKET_DURATION);
    }

    @Override
    public Ticket openNewTicket(String key, Duration duration) {

        var grantTime = Instant.now();
        var grantDuration = duration.compareTo(MAX_TICKET_DURATION) < 0 ? duration : MAX_TICKET_DURATION;

        if (_cache.containsKey(key))
            return Ticket.supersededTicket(key, FIRST_REVISION, grantTime);
        else
            return Ticket.forDuration(this, key, FIRST_REVISION, grantTime, grantDuration);
    }

    @Override
    public Ticket openTicket(String key, int revision) {

        return openTicket(key, revision, DEFAULT_TICKET_DURATION);
    }

    @Override
    public Ticket openTicket(String key, int revision, Duration duration) {

        var grantTime = Instant.now();
        var grantDuration = duration.compareTo(MAX_TICKET_DURATION) < 0 ? duration : MAX_TICKET_DURATION;

        var ticket = Ticket.forDuration(this, key, revision, grantTime, grantDuration);

        var cacheEntry = _cache.computeIfPresent(key, (_key, priorEntry) -> {

            if (priorEntry.revision != revision)
                return priorEntry;

            if (priorEntry.ticket != null && priorEntry.ticket.expiry().isAfter(grantTime))
                return priorEntry;

            var newEntry = priorEntry.clone();
            newEntry.ticket = ticket;
            newEntry.revision += 1;

            return newEntry;
        });

        if (cacheEntry == null)
            return Ticket.missingEntryTicket(key, revision, grantTime);

        if (cacheEntry.ticket != ticket)
            return Ticket.supersededTicket(key, revision, grantTime);

        return ticket;
    }

    @Override
    public void closeTicket(Ticket ticket) {

        if (ticket.superseded())
            return;

        _cache.computeIfPresent(ticket.key(), (_key, priorEntry) -> {

            if (priorEntry.ticket != ticket)
                return priorEntry;

            var newEntry = priorEntry.clone();
            newEntry.ticket = null;

            return newEntry;
        });
    }

    @Override
    public int addEntry(Ticket ticket, String status, TValue value) {

        var commitTime = Instant.now();

        checkValidTicket(ticket, "create", commitTime);

        var added = _cache.compute(ticket.key(), (_key, _entry) -> {

            if (_entry != null) {
                var message = String.format("Cannot create [%s], item is already in the cache", ticket.key());
                log.error(message);
                throw new ECacheTicket(message);
            }

            var newEntry = new LocalJobCacheEntry<TValue>();
            newEntry.ticket = ticket;
            newEntry.revision = ticket.revision() + 1;
            newEntry.lastActivity = commitTime;
            newEntry.stateKey = status;
            newEntry.value = value;

            return newEntry;
        });

        return added.revision;
    }

    @Override
    public int updateEntry(Ticket ticket, String stateKey, TValue value) {

        var commitTime = Instant.now();

        checkValidTicket(ticket, "update", commitTime);

        var updated = _cache.compute(ticket.key(), (_key, _entry) -> {

            if (_entry == null) {
                var message = String.format("Cannot update [%s], item is not in the cache", ticket.key());
                log.error(message);
                throw new ECacheNotFound(message);
            }

            checkEntryMatchesTicket(_entry, ticket, "update");

            var newEntry = _entry.clone();
            newEntry.revision += 1;
            newEntry.lastActivity = commitTime;
            newEntry.stateKey = stateKey;
            newEntry.value = value;

            return newEntry;
        });

        return updated.revision;
    }

    @Override
    public void removeEntry(Ticket ticket) {

        var commitTime = Instant.now();

        checkValidTicket(ticket, "remove", commitTime);

        _cache.compute(ticket.key(), (_key, _entry) -> {

            if (_entry == null) {
                var message = String.format("Cannot remove [%s], item is not in the cache", ticket.key());
                log.error(message);
                throw new ECacheNotFound(message);
            }

            checkEntryMatchesTicket(_entry, ticket, "remove");

            return null;
        });
    }

    @Override
    public CacheQueryResult<TValue> getEntry(Ticket ticket) {

        var entry = _cache.get(ticket.key());

        if (entry == null) {
            var message = String.format("Entry for [%s] is not in the cache", ticket.key());
            log.error(message);
            throw new ECacheNotFound(message);
        }

        if (entry.ticket != ticket) {
            var message = String.format("Entry for [%s] does not match the expected ticket", ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }

        return new CacheQueryResult<>(ticket.key(), entry.revision, entry.stateKey, entry.value);
    }

    @Override
    public CacheQueryResult<TValue> getEntry(String key, int revision) {

        var entry = _cache.get(key);

        if (entry == null) {
            var message = String.format("Entry for [%s] is not in the cache", key);
            log.error(message);
            throw new ECacheNotFound(message);
        }

        if (entry.revision != revision) {
            var message = String.format("Entry for [%s] does not match the expected revision", key);
            log.error(message);
            throw new ECacheTicket(message);
        }

        return new CacheQueryResult<>(key, entry.revision, entry.stateKey, entry.value);
    }

    @Override
    public CacheQueryResult<TValue> getLatestEntry(String key) {

        var entry = _cache.get(key);

        if (entry == null) {
            var message = String.format("Entry for [%s] is not in the cache", key);
            log.error(message);
            throw new ECacheNotFound(message);
        }

        return new CacheQueryResult<>(key, entry.revision, entry.stateKey, entry.value);
    }

    @Override
    public List<CacheQueryResult<TValue>> queryState(List<String> states) {

        return queryState(states, false);
    }

    @Override
    public List<CacheQueryResult<TValue>> queryState(List<String> states, boolean includeOpenTickets) {

        var queryTime = Instant.now();

        var results = new ArrayList<CacheQueryResult<TValue>>();

        _cache.forEach((_key, _entry) -> {

            if (!states.contains(_entry.stateKey))
                return;

            if (_entry.ticket != null && _entry.ticket.expiry().isAfter(queryTime))
                if (!includeOpenTickets)
                    return;

            var result = new CacheQueryResult<>(_key, _entry.revision, _entry.stateKey, _entry.value);
            results.add(result);
        });

        return results;
    }

    private void checkValidTicket(Ticket ticket, String operation, Instant operationTime) {

        if (ticket.missing()) {
            var message = String.format("Cannot %s [%s], item is not in the cache", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }

        if (ticket.superseded()) {
            var message = String.format("Ticket to %s [%s] has been superseded", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }

        if (operationTime.isAfter(ticket.expiry())) {
            var message = String.format("Ticket to %s [%s] has expired", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }
    }

    private void checkEntryMatchesTicket(LocalJobCacheEntry<TValue> entry, Ticket ticket, String operation) {

        if (entry.ticket == null){
            var message = String.format("Cannot %s [%s], ticket is no longer valid", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }

        if (entry.ticket != ticket){
            var message = String.format("Cannot %s [%s], another operation is in progress", operation, ticket.key());
            log.error(message);
            throw new ECacheTicket(message);
        }
    }

}
