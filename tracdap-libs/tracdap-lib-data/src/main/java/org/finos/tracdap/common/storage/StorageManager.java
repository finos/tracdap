/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.storage;

import org.finos.tracdap.config.StorageConfig;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EStorageConfig;
import org.finos.tracdap.common.plugin.IPluginManager;
import org.finos.tracdap.common.storage.flat.FlatDataStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class StorageManager implements IStorageManager {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IPluginManager plugins;
    private final Map<String, StorageBackend> storage;

    public StorageManager(IPluginManager plugins) {
        this.plugins = plugins;
        this.storage = new HashMap<>();
    }

    public void initStorage(Map<String, StorageConfig> storageConfigMap, ICodecManager formats) {

        log.info("Configuring storage...");

        for (var store: storageConfigMap.entrySet()) {

            var storageKey = store.getKey();
            var config = store.getValue();
            var backend = new StorageBackend();

            for (var instanceConfig : config.getInstancesList()) {

                var protocol = instanceConfig.getStorageType();
                var rawProps = instanceConfig.getStoragePropsMap();
                var props = new Properties();
                props.put(PROP_STORAGE_KEY, storageKey);
                props.putAll(rawProps);

                log.info("Attach storage: [{}] (protocol: {})", storageKey, protocol);

                if (plugins.isServiceAvailable(IFileStorage.class, protocol)) {

                    var fileInstance = plugins.createService(IFileStorage.class, protocol, props);
                    backend.fileInstances.add(fileInstance);
                }

                if (plugins.isServiceAvailable(IDataStorage.class, protocol)) {

                    var dataInstance = plugins.createService(IDataStorage.class, protocol, props);
                    backend.dataInstances.add(dataInstance);
                }
                else if (!backend.fileInstances.isEmpty()) {

                    log.info("Using flat data storage (datasets will be saved as files)");

                    for (var fileInstance : backend.fileInstances) {
                        var dataInstance = new FlatDataStorage(fileInstance, formats);
                        backend.dataInstances.add(dataInstance);
                    }
                }

                if (backend.fileInstances.isEmpty() && backend.dataInstances.isEmpty()) {

                    var message = String.format("No plugin found to support storage protocol [%s]", protocol);
                    var error = new EStartup(message);

                    log.error(message, error);
                    throw error;
                }
            }

            storage.put(storageKey, backend);
        }
    }

    @Override
    public IDataStorage getDataStorage(String storageKey) {

        // TODO: Full implementation for selecting the storage backend

        // Full implementation should consider multiple instances, locations etc.
        // Also handle selection between data and file storage

        var store = this.storage.get(storageKey);

        if (store == null) {
            throw new EStorageConfig("Storage not configured for storage key: " + storageKey);
        }

        var instance = store.dataInstances.get(0);

        if (instance == null) {
            throw new EStorageConfig("Storage not configured for storage key: " + storageKey);
        }

        return instance;
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

        List<IDataStorage> dataInstances = new ArrayList<>();
    }
}
