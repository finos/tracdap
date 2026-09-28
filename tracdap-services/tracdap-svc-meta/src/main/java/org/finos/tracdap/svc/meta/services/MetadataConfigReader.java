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

package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.api.ConfigReadResponse;
import org.finos.tracdap.api.PlatformConfigReadResponse;
import org.finos.tracdap.common.metadata.store.IMetadataStore;
import org.finos.tracdap.common.service.IConfigReader;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.PlatformConfigEntry;
import org.finos.tracdap.metadata.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class MetadataConfigReader implements IConfigReader {

    private final IMetadataStore metadataStore;

    public MetadataConfigReader(IMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public PlatformConfigReadResponse readPlatformConfig(String configClass, String configKey, boolean includeDeleted) {

        var record = metadataStore.loadPlatformConfigEntry(configClass, configKey, includeDeleted);
        return PlatformConfigService.buildReadResponse(record);
    }

    @Override
    public List<PlatformConfigEntry> listPlatformConfig(String configClass, boolean includeDeleted) {

        return metadataStore.listPlatformConfigEntries(configClass, includeDeleted);
    }

    @Override
    public List<PlatformConfigReadResponse> readPlatformConfigBatch(List<PlatformConfigEntry> entries, boolean includeDeleted) {

        return metadataStore.loadPlatformConfigEntries(entries, includeDeleted).stream()
                .map(PlatformConfigService::buildReadResponse)
                .collect(Collectors.toList());
    }

    @Override
    public ConfigReadResponse readConfig(String tenant, String configClass, String configKey, boolean includeDeleted) {

        var key = latestKey(configClass, configKey);

        var entry = metadataStore.loadConfigEntry(tenant, key, includeDeleted);

        if (entry.getConfigDeleted())
            return ConfigReadResponse.newBuilder().setEntry(entry).build();

        var tag = metadataStore.loadObject(tenant, entry.getDetails().getObjectSelector());

        return buildReadResponse(entry, tag);
    }

    @Override
    public List<ConfigEntry> listConfig(String tenant, String configClass, boolean includeDeleted) {

        return metadataStore.listConfigEntries(tenant, configClass, includeDeleted);
    }

    @Override
    public List<ConfigReadResponse> readConfigBatch(String tenant, List<ConfigEntry> entries, boolean includeDeleted) {

        var keys = entries.stream()
                .map(entry -> latestKey(entry.getConfigClass(), entry.getConfigKey()))
                .collect(Collectors.toList());

        var loadedEntries = metadataStore.loadConfigEntries(tenant, keys, includeDeleted);

        var selectors = loadedEntries.stream()
                .filter(entry -> !entry.getConfigDeleted())
                .map(entry -> entry.getDetails().getObjectSelector())
                .collect(Collectors.toList());

        var tags = metadataStore.loadObjects(tenant, selectors);
        var nextTag = tags.iterator();

        var results = new ArrayList<ConfigReadResponse>(loadedEntries.size());

        for (var entry : loadedEntries) {

            if (entry.getConfigDeleted())
                results.add(ConfigReadResponse.newBuilder().setEntry(entry).build());
            else
                results.add(buildReadResponse(entry, nextTag.next()));
        }

        return results;
    }

    private static ConfigEntry latestKey(String configClass, String configKey) {

        return ConfigEntry.newBuilder()
                .setConfigClass(configClass)
                .setConfigKey(configKey)
                .setIsLatestConfig(true)
                .build();
    }

    private static ConfigReadResponse buildReadResponse(ConfigEntry entry, Tag tag) {

        return ConfigReadResponse.newBuilder()
                .setEntry(entry)
                .setDefinition(tag.getDefinition())
                .putAllAttrs(tag.getAttrsMap())
                .build();
    }
}
