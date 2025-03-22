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

package org.finos.tracdap.test.helpers;

import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.storage.IFileStorage;
import org.finos.tracdap.common.storage.IStorageManager;
import org.finos.tracdap.config.DynamicConfig;

import io.netty.channel.EventLoopGroup;
import org.apache.arrow.memory.RootAllocator;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.metadata.ResourceType;

import java.time.Duration;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.getResultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;


public class StorageTestHelpers {

    public static void createStoragePrefix(
            ConfigManager config,
            PluginManager plugins,
            EventLoopGroup elg,
            String dynamicConfigPath)
            throws Exception {

        // Assuming "prefix" is common across all bucket storage implementations
        // Create a storage instance with the prefix removed, then do rmdir on the prefix

        var configSep = dynamicConfigPath.lastIndexOf("/");
        var configFile = dynamicConfigPath.substring(configSep + 1);
        var dynamicConfig = config.loadConfigObject(configFile, DynamicConfig.class);

        var execCtx = new DataContext(elg.next(), new RootAllocator());

        for (var resource : dynamicConfig.getResourcesMap().entrySet()) {

            if (resource.getValue().getResourceType() != ResourceType.INTERNAL_STORAGE)
                continue;

            if (!resource.getValue().containsProperties("prefix"))
                continue;

            var storageConfig = PluginConfig.newBuilder()
                    .setProtocol(resource.getValue().getProtocol())
                    .putProperties(IStorageManager.PROP_STORAGE_KEY, resource.getKey())
                    .removeProperties("prefix")
                    .build();

            var prefix = resource.getValue().getPropertiesOrThrow("prefix");

            try (var storage = plugins.createService(IFileStorage.class, storageConfig, config)) {

                storage.start(elg);

                var rmdir = storage.mkdir(prefix, true, execCtx);
                waitFor(Duration.ofSeconds(30), rmdir);
                getResultOf(rmdir);
            }
        }
    }

    public static void deleteStoragePrefix(
            ConfigManager config,
            PluginManager plugins,
            EventLoopGroup elg,
            String dynamicConfigPath)
            throws Exception {

        // Assuming "prefix" is common across all bucket storage implementations
        // Create a storage instance with the prefix removed, then do rmdir on the prefix

        var configSep = dynamicConfigPath.lastIndexOf("/");
        var configFile = dynamicConfigPath.substring(configSep + 1);
        var dynamicConfig = config.loadConfigObject(configFile, DynamicConfig.class);

        var execCtx = new DataContext(elg.next(), new RootAllocator());

        for (var resource : dynamicConfig.getResourcesMap().entrySet()) {

            if (resource.getValue().getResourceType() != ResourceType.INTERNAL_STORAGE)
                continue;

            if (!resource.getValue().containsProperties("prefix"))
                continue;

            var storageConfig = PluginConfig.newBuilder()
                    .setProtocol(resource.getValue().getProtocol())
                    .putProperties(IStorageManager.PROP_STORAGE_KEY, resource.getKey())
                    .removeProperties("prefix")
                    .build();

            var prefix = resource.getValue().getPropertiesOrThrow("prefix");

            try (var storage = plugins.createService(IFileStorage.class, storageConfig, config)) {

                storage.start(elg);

                var rmdir = storage.rmdir(prefix, execCtx);
                waitFor(Duration.ofSeconds(30), rmdir);
                getResultOf(rmdir);
            }
        }
    }

}
