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

package org.finos.tracdap.svc.data.service;

import io.netty.channel.EventLoopGroup;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.common.service.TenantState;
import org.finos.tracdap.common.storage.StorageManager;
import org.finos.tracdap.metadata.ResourceType;


public class TenantStorageState extends TenantState {

    private final StorageManager storage;

    public TenantStorageState(
            PluginManager pluginManager,
            ConfigManager configManager,
            ICodecManager codecManager,
            EventLoopGroup eventLoopGroup,
            TenantConfig initialConfig) {

        storage = new StorageManager(pluginManager, configManager, codecManager, eventLoopGroup);
        storage.updateStorageDefaults(initialConfig);

        for (var resource : initialConfig.getResourcesMap().entrySet()) {
            // Currently only internal storage is supported
            if (resource.getValue().getResourceType() == ResourceType.INTERNAL_STORAGE) {
                storage.addStorage(resource.getKey(), resource.getValue());
            }
        }
    }

    public StorageManager getStorageManager() {
        return storage;
    }

    @Override
    protected boolean configIsRelevant(ConfigEntry entry) {

        // Currently only internal storage is supported
        return entry.getConfigClass().equals(ConfigKeys.TRAC_RESOURCES)
            && entry.getDetails().getResourceType() == ResourceType.INTERNAL_STORAGE;
    }

    @Override
    protected void tenantPropertiesUpdated(TenantConfig tenantConfig) {
        storage.updateStorageDefaults(tenantConfig);
    }

    @Override
    protected void configCreated(ConfigEntry entry, ObjectDefinition definition) {
        storage.addStorage(entry.getConfigKey(), definition.getResource());
    }

    @Override
    protected void configUpdated(ConfigEntry entry, ObjectDefinition definition) {
        storage.updateStorage(entry.getConfigKey(), definition.getResource());
    }

    @Override
    protected void configDeleted(ConfigEntry entry) {
        storage.removeStorage(entry.getConfigKey());
    }

    @Override
    protected void shutdown() {
        storage.close();
    }
}
