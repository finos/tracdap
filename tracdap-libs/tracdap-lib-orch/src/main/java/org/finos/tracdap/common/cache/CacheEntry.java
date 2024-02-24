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

import org.finos.tracdap.common.exception.ECache;


public final class CacheEntry<TValue> {

    private final String key;
    private final int revision;
    private final String status;
    private final TValue value;
    private final ECache cacheError;

    public static <TValue>
    CacheEntry<TValue> forValue(String key, int revision, String status, TValue value) {

        return new CacheEntry<>(key, revision, status, value, null);
    }

    public static <TValue>
    CacheEntry<TValue> error(String key, int revision, String status, ECache cacheError) {

        return new CacheEntry<>(key, revision, status, null, cacheError);
    }

    private CacheEntry(String key, int revision, String status, TValue value, ECache cacheError) {

        this.key = key;
        this.revision = revision;
        this.status = status;
        this.value = value;
        this.cacheError = cacheError;
    }

    public String key() {
        return key;
    }

    public int revision() {
        return revision;
    }

    public String status() {
        return status;
    }

    public TValue value() {

        // Attempting to access the value for an invalid entry - throw the error
        if (cacheError != null)
            throw cacheError;

        return value;
    }

    public boolean cacheOk() {
        return cacheError == null;
    }

    public ECache cacheError() {
        return cacheError;
    }
}
