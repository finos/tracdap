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

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.config.StorageConfig;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EStorageConfig;
import org.finos.tracdap.common.plugin.IPluginManager;

import io.netty.channel.EventLoopGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class StorageManager implements IStorageManager, AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IPluginManager plugins;
    private final ConfigManager configManager;
    private final Map<String, StorageBackend> storage;

    public StorageManager(IPluginManager plugins, ConfigManager configManager) {
        this.plugins = plugins;
        this.configManager = configManager;
        this.storage = new HashMap<>();
    }

    public void initStorage(StorageConfig storageConfig, ICodecManager formats, EventLoopGroup eventLoopGroup) {

        log.info("Configuring storage...");

        for (var bucket: storageConfig.getBucketsMap().entrySet()) {

            var bucketKey = bucket.getKey();
            var bucketConfig = bucket.getValue();
            var backend = new StorageBackend();

            bucketConfig = bucketConfig.toBuilder()
                    .putProperties(PROP_STORAGE_KEY, bucketKey)
                    .build();

            log.info("Attach storage: [{}] (protocol: {})", bucketKey, bucketConfig.getProtocol());

            if (plugins.isServiceAvailable(IFileStorage.class, bucketConfig.getProtocol())) {

                var fileInstance = plugins.createService(IFileStorage.class, bucketConfig, configManager);
                fileInstance.start(eventLoopGroup);

                backend.fileInstances.add(fileInstance);
            }

            if (plugins.isServiceAvailable(IDataStorage.class, bucketConfig.getProtocol())) {

                var dataInstance = plugins.createService(IDataStorage.class, bucketConfig, configManager);
                dataInstance.start(eventLoopGroup);

                backend.dataInstances.add(dataInstance);
            }
            else if (!backend.fileInstances.isEmpty()) {

                log.info("Using flat data storage (datasets will be saved as files)");

                for (var fileInstance : backend.fileInstances) {

                    var dataInstance = new CommonDataStorage(bucketConfig, fileInstance, formats);
                    dataInstance.start(eventLoopGroup);

                    backend.dataInstances.add(dataInstance);
                }
            }

            if (backend.fileInstances.isEmpty() && backend.dataInstances.isEmpty()) {

                var message = String.format("No plugin found to support storage protocol [%s]", bucketConfig.getProtocol());
                var error = new EStartup(message);

                log.error(message, error);
                throw error;
            }

            storage.put(bucketKey, backend);
        }
    }

    @Override
    public void close() {

        for (var storage : this.storage.entrySet()) {

            var backend = storage.getValue();

            log.info("Detach storage: [{}]", storage.getKey());

            // For errors during shutdown, log the error and continue shutting down other instances
            // Don't propagate the error, that can only prevent shutdown being called for other resources

            for (var dataImpl : backend.dataInstances) {

                try {

                    dataImpl.close();
                }
                catch (Exception error) {

                    log.error(
                        "There was an error shutting down the storage backend: [{}] {}",
                        storage.getKey(), error.getMessage(), error);
                }
            }

            for (var fileImpl : backend.fileInstances) {

                try {

                    fileImpl.close();
                }
                catch (Exception error) {

                    log.error(
                            "There was an error shutting down the storage backend: [{}] {}",
                            storage.getKey(), error.getMessage(), error);
                }
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
