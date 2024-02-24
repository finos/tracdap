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

import org.finos.tracdap.common.cache.IJobCache;
import org.finos.tracdap.common.cache.IJobCacheManager;
import org.finos.tracdap.common.exception.ETracInternal;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class LocalJobCacheManager implements IJobCacheManager {

    private final Map<String, StronglyTypedCache<?>> caches;

    public LocalJobCacheManager() {
        this.caches = new ConcurrentHashMap<>();
    }

    @Override
    public <TValue extends Serializable> IJobCache<TValue> getCache(String cacheName, Class<TValue> cacheType) {

        var existingCache = caches.get(cacheName);

        if (existingCache != null)
            return checkCacheType(existingCache, cacheName, cacheType);

        var newCacheImpl = new LocalJobCache<TValue>();
        var newCache = new StronglyTypedCache<>(cacheType, newCacheImpl);

        var uniqueCache = caches.putIfAbsent(cacheName, newCache);

        if (uniqueCache != null)
            return checkCacheType(uniqueCache, cacheName, cacheType);
        else
            return newCache.cache;
    }

    @SuppressWarnings("unchecked")
    private <TValue extends Serializable>
    IJobCache<TValue> checkCacheType(StronglyTypedCache<?> cache, String cacheName, Class<TValue> cacheType) {

        if (cache.cacheType.equals(cacheType))
            return (IJobCache<TValue>) cache.cache;

        var message = String.format(
                "The cache for [%s] has the wrong type (expected [%s], got [%s])",
                cacheName, cacheType.getName(), cache.cacheType.getName());

        throw new ETracInternal(message);
    }

    private static class StronglyTypedCache<TValue extends Serializable> {

        private final Class<TValue> cacheType;
        private final IJobCache<TValue> cache;

        public StronglyTypedCache(Class<TValue> cacheType, IJobCache<TValue> cache) {
            this.cacheType = cacheType;
            this.cache = cache;
        }
    }
}
