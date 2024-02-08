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

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;


public interface IJobCache<TValue> {

    Pattern VALID_KEY = Pattern.compile("\\A[\\w\\-]+\\Z");

    Ticket openNewTicket(String key, Duration duration);
    Ticket openTicket(String key, int revision, Duration duration);
    void closeTicket(Ticket ticket);

    int addEntry(Ticket ticket, String status, TValue value);
    int updateEntry(Ticket ticket, String status, TValue value);
    void removeEntry(Ticket ticket);
    CacheEntry<TValue> getEntry(Ticket ticket);

    Optional<CacheEntry<TValue>> queryKey(String key);
    List<CacheEntry<TValue>> queryStatus(List<String> statuses);
    List<CacheEntry<TValue>> queryStatus(List<String> statuses, boolean includeOpenTickets);
}
