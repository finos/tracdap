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

package org.finos.tracdap.common.plugin;

import org.finos.tracdap.common.exception.EUnexpected;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;


public class PluginRegistry {

    private final Map<Key, Object> registry;

    public PluginRegistry() {
        registry = new ConcurrentHashMap<>();
    }

    public <T> void registerSingleton(Class<T> pluginClass, T instance) {
        var key = new Key(pluginClass, null);
        registry.put(key, instance);
    }

    public <T> void registerNamedInstance(Class<T> pluginClass, String namedKey, T instance) {
        var key = new Key(pluginClass, namedKey);
        registry.put(key, instance);
    }

    public <T> T getSingleton(Class<T> pluginClass) {
        var key = new Key(pluginClass, null);
        var instance = registry.get(key);
        if (instance == null)
            return null;
        if (!pluginClass.isInstance(instance))
            throw new EUnexpected();
        return pluginClass.cast(instance);
    }

    public <T> T getNamedInstance(Class<T> pluginClass, String namedKey) {
        var key = new Key(pluginClass, namedKey);
        var instance = registry.get(key);
        if (instance == null)
            return null;
        if (!pluginClass.isInstance(instance))
            throw new EUnexpected();
        return pluginClass.cast(instance);
    }

    private static class Key {

        private final Class<?> pluginClass;
        private final String namedKey;

        public Key(Class<?> pluginClass, String namedKey) {
            this.pluginClass = pluginClass;
            this.namedKey = namedKey;
        }

        public Class<?> pluginClass() {
            return pluginClass;
        }

        public String namedKey() {
            return namedKey;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Objects.equals(pluginClass, key.pluginClass) && Objects.equals(namedKey, key.namedKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pluginClass, namedKey);
        }
    }
}
