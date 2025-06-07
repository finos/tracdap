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

package org.finos.tracdap.common.service;

import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.ObjectDefinition;

import java.util.concurrent.atomic.AtomicReference;

public class TenantConfigState extends TenantState {

    private final AtomicReference<TenantConfig> liveConfig;

    public TenantConfigState(TenantConfig initialConfig) {
        this.liveConfig = new AtomicReference<>(initialConfig);
    }

    public TenantConfig getLiveConfig() {
        return liveConfig.get();
    }

    @Override
    protected boolean configIsRelevant(ConfigEntry entry) {
        return true;
    }

    @Override
    protected void tenantPropertiesUpdated(TenantConfig tenantConfig) {

        liveConfig.getAndUpdate(cfg -> cfg.toBuilder()
                .clearProperties()
                .clearSecrets()
                .putAllProperties(tenantConfig.getPropertiesMap())
                .putAllSecrets(tenantConfig.getSecretsMap())
                .build());
    }

    @Override
    protected void configCreated(ConfigEntry entry, ObjectDefinition definition) {

        // Create and update are the same for config entries
        configUpdated(entry, definition);
    }

    @Override
    protected void configUpdated(ConfigEntry entry, ObjectDefinition definition) {

        var configClass = entry.getConfigClass();
        var configKey = entry.getConfigKey();

        if (configClass.equals(ConfigKeys.TRAC_CONFIG)) {
            var configObject = definition.getConfig();
            liveConfig.getAndUpdate(cfg -> cfg.toBuilder().putConfig(configKey, configObject).build());
        }

        else if (entry.getConfigClass().equals(ConfigKeys.TRAC_RESOURCES)) {
            var resourceObject = definition.getResource();
            liveConfig.getAndUpdate(cfg -> cfg.toBuilder().putResources(configKey, resourceObject).build());
        }
    }

    @Override
    protected void configDeleted(ConfigEntry entry) {

        var configClass = entry.getConfigClass();
        var configKey = entry.getConfigKey();

        if (configClass.equals(ConfigKeys.TRAC_CONFIG)) {
            liveConfig.getAndUpdate(cfg -> cfg.toBuilder().removeConfig(configKey).build());
        }

        else if (entry.getConfigClass().equals(ConfigKeys.TRAC_RESOURCES)) {
            liveConfig.getAndUpdate(cfg -> cfg.toBuilder().removeResources(configKey).build());
        }
    }

    @Override
    protected void shutdown() {

        // NO-op, no resources to clean up
    }
}
