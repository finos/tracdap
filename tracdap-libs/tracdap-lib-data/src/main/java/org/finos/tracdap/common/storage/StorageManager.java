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

package org.finos.tracdap.common.storage;

import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EStorageConfig;
import org.finos.tracdap.common.plugin.IPluginManager;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.metadata.ResourceDefinition;

import io.netty.channel.EventLoopGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class StorageManager implements IStorageManager, AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IPluginManager plugins;
    private final ConfigManager configManager;
    private final ICodecManager formats;
    private final EventLoopGroup eventLoopGroup;

    private final Map<String, StorageBackend> storage;

    public StorageManager(
            IPluginManager plugins, ConfigManager configManager,
            ICodecManager formats, EventLoopGroup eventLoopGroup) {

        this.plugins = plugins;
        this.configManager = configManager;
        this.formats = formats;
        this.eventLoopGroup = eventLoopGroup;

        this.storage = new ConcurrentHashMap<>();
    }

    public void addStorage(String storageKey, ResourceDefinition resourceConfig) {
        addStorage(storageKey, ConfigHelpers.resourceToPluginConfig(resourceConfig));
    }

    public void addStorage(String storageKey, PluginConfig resourceConfig) {

        log.info("Add storage: [{}] (protocol: {})", storageKey, resourceConfig.getProtocol());

        var storageBackend = createStorageBackend(storageKey, resourceConfig);
        storage.put(storageKey, storageBackend);
    }

    public void updateStorage(String storageKey, ResourceDefinition resourceConfig) {
        updateStorage(storageKey, ConfigHelpers.resourceToPluginConfig(resourceConfig));
    }

    public void updateStorage(String storageKey, PluginConfig resourceConfig) {

        log.info("Update storage: [{}] (protocol: {})", storageKey, resourceConfig.getProtocol());

        var storageBackend = createStorageBackend(storageKey, resourceConfig);
        var previousBackend = storage.put(storageKey, storageBackend);

        if (previousBackend != null) {
            stopStorageBackend(storageKey, previousBackend);
        }
    }

    public void removeStorage(String storageKey) {

        log.info("Remove storage: [{}]", storageKey);

        var previousBackend = storage.remove(storageKey);

        if (previousBackend != null)
            stopStorageBackend(storageKey, previousBackend);
    }

    public List<String> listStorageKeys() {
        return List.copyOf(storage.keySet());
    }

    @Override
    public void close() {

        for (var i = this.storage.entrySet().iterator(); i.hasNext(); i.remove()) {

            var storage = i.next();
            var storageKey = storage.getKey();
            var backend = storage.getValue();

            log.info("Close storage: [{}]", storageKey);

            stopStorageBackend(storageKey, backend);
        }
    }

    private StorageBackend createStorageBackend(String storageKey, PluginConfig storageConfig) {

        var storageConfigWithKey = storageConfig.toBuilder()
                .putProperties(PROP_STORAGE_KEY, storageKey)
                .build();

        var backend = new StorageBackend();

        if (plugins.isServiceAvailable(IFileStorage.class, storageConfig.getProtocol())) {

            var fileInstance = plugins.createService(IFileStorage.class, storageConfigWithKey, configManager);
            fileInstance.start(eventLoopGroup);

            backend.fileInstances.add(fileInstance);
        }

        if (plugins.isServiceAvailable(IDataStorage.class, storageConfig.getProtocol())) {

            var dataInstance = plugins.createService(IDataStorage.class, storageConfigWithKey, configManager);
            dataInstance.start(eventLoopGroup);

            backend.dataInstances.add(dataInstance);
        }
        else if (!backend.fileInstances.isEmpty()) {

            log.info("Using flat data storage (datasets will be saved as files)");

            for (var fileInstance : backend.fileInstances) {

                var dataInstance = new CommonDataStorage(storageConfigWithKey, fileInstance, formats);
                dataInstance.start(eventLoopGroup);

                backend.dataInstances.add(dataInstance);
            }
        }

        if (backend.fileInstances.isEmpty() && backend.dataInstances.isEmpty()) {

            var message = String.format("No plugin found to support storage protocol [%s]", storageConfig.getProtocol());
            var error = new EStartup(message);

            log.error(message, error);
            throw error;
        }

        return backend;
    }

    private void stopStorageBackend(String storageKey, StorageBackend backend) {

        // For errors during shutdown, log the error and continue shutting down other instances
        // Don't propagate the error, that can only prevent shutdown being called for other resources

        for (var dataImpl : backend.dataInstances) {

            try {

                dataImpl.close();
            }
            catch (Exception error) {

                log.error(
                        "There was an error shutting down the storage backend: [{}] {}",
                        storageKey, error.getMessage(), error);
            }
        }

        for (var fileImpl : backend.fileInstances) {

            try {

                fileImpl.close();
            }
            catch (Exception error) {

                log.error(
                        "There was an error shutting down the storage backend: [{}] {}",
                        storageKey, error.getMessage(), error);
            }
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
