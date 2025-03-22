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

package org.finos.tracdap.common.config;

import org.finos.tracdap.common.exception.EResourceNotFound;

import com.google.protobuf.Message;
import org.finos.tracdap.metadata.ResourceDefinition;
import org.finos.tracdap.metadata.ResourceType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


public class DynamicConfig<T extends Message> implements IDynamicConfig<T> {

    protected final Map<String, T> configMap;

    public DynamicConfig() {

        configMap = new ConcurrentHashMap<>();
    }

    public void addEntry(String configKey, T entry) {

        configMap.put(configKey, entry);
    }

    public void updateEntry(String configKey, T entry) {

        configMap.put(configKey, entry);
    }

    public void removeEntry(String configKey) {

        configMap.remove(configKey);
    }

    public T getEntry(String configKey) {

        return configMap.get(configKey);
    }

    @Override
    public T getStrictEntry(String configKey) {

        return getStrictEntry(configKey, (x) -> true, null);
    }

    @Override
    public T getStrictEntry(String configKey, Condition<T> condition) {

        return getStrictEntry(configKey, condition, null);
    }

    @Override
    public T getStrictEntry(String configKey, Condition<T> condition, String message) {

        var entry = configMap.get(configKey);

        if (entry == null)
            throw new EResourceNotFound("Config entry not found: " + configKey);

        if (!condition.checkCondition(entry)) {
            var suffix = message != null && ! message.isEmpty() ? " (" + message + ")" : "";
            throw new EResourceNotFound("Config entry is not usable: " + configKey + suffix);
        }

        return entry;
    }

    @Override
    public Map<String, T> getAllEntries() {

        return Map.copyOf(configMap);
    }

    @Override
    public Map<String, T> getMatchingEntries(Condition<T> condition) {

        return configMap.entrySet().stream()
                .filter(ce -> condition.checkCondition(ce.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static class Resources extends DynamicConfig<ResourceDefinition> implements IDynamicResources {

        @Override
        public ResourceDefinition getStrictEntry(String configKey) {

            var entry = configMap.get(configKey);

            if (entry == null)
                throw new EResourceNotFound("Resource not found: " + configKey);

            return entry;
        }

        @Override
        public ResourceDefinition getStrictEntry(String configKey, ResourceType resourceType) {

            var entry = configMap.get(configKey);

            if (entry == null)
                throw new EResourceNotFound("Resource not found: " + configKey);

            if (entry.getResourceType() != resourceType) {
                var error = String.format(
                        "Resource is the wrong type (expected [%s], got [%s])",
                        resourceType, entry.getResourceType());
                throw new EResourceNotFound(error);
            }

            return entry;
        }
    }
}
