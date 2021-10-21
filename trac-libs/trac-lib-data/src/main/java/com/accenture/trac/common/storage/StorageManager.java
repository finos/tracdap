/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.storage;

import com.accenture.trac.api.config.StorageConfig;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.exception.EStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class StorageManager implements IStorageManager {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<String, IStoragePlugin> plugins;
    private final Map<String, StorageBackend> storage;

    public StorageManager() {
        this.plugins = new HashMap<>();
        this.storage = new HashMap<>();
    }

    public void initStoragePlugins() {

        log.info("Looking for storage plugins...");

        var availablePlugins = ServiceLoader.load(IStoragePlugin.class);

        for (var plugin: availablePlugins) {

            var discoveryMsg = String.format("Storage plugin: [%s] (protocols: %s)",
                    plugin.name(),
                    String.join(", ", plugin.protocols()));

            log.info(discoveryMsg);

            for (var protocol : plugin.protocols())
                plugins.put(protocol, plugin);
        }
    }

    public void initStorage(Map<String, StorageConfig> storageConfigMap) {

        log.info("Configuring storage...");

        for (var store: storageConfigMap.entrySet()) {

            var storageKey = store.getKey();
            var config = store.getValue();
            var backend = new StorageBackend();

            for (var instanceConfig : config.getInstances()) {

                var protocol = instanceConfig.getStorageType();
                var rawProps = instanceConfig.getStorageProps();
                var props = new Properties();
                props.putAll(rawProps);

                log.info("Attach storage: [{}] (protocol: {})", storageKey, protocol);

                var plugin = plugins.get(protocol);

                if (plugin == null) {

                    var message = String.format("No plugin found to support storage protocol [%s]", protocol);
                    var error = new EStartup(message);

                    log.error(message, error);
                    throw error;
                }

                var instance = plugin.createFileStorage(storageKey, protocol, props);

                backend.fileInstances.add(instance);
            }

            storage.put(storageKey, backend);
        }
    }

    @Override
    public IFileStorage getFileStorage(String storageKey) {

        // TODO: Full implementation for selecting the storage backend

        // Full implementation should consider multiple instances, locations etc.
        // Also handle selection between data and file storage

        var store = this.storage.get(storageKey);

        if (store == null) {
            throw new EStorageConfig("Storage not configured for storage key: " + storageKey);
        }

        var instance = store.fileInstances.get(0);

        if (instance == null) {
            throw new EStorageConfig("Storage not configured for storage key: " + storageKey);
        }

        return instance;
    }

    private static class StorageBackend {

        List<IFileStorage> fileInstances = new ArrayList<>();
    }
}
