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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;


public class StorageManager implements IStorageManager {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<String, IStoragePlugin> plugins;

    public StorageManager() {
        this.plugins = new HashMap<>();
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

    @Override
    public IFileStorage getFileStorage(String storageKey) {

        var protocol = "file";
        var plugin = plugins.get(protocol);

        return plugin.createFileStorage();
    }
}
