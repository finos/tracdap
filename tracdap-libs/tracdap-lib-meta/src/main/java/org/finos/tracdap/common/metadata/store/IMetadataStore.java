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

package org.finos.tracdap.common.metadata.store;

import org.finos.tracdap.metadata.*;

import java.util.List;
import java.util.UUID;


public interface IMetadataStore {

    void start();

    void stop();

    List<TenantInfo> listTenants();

    void activateTenant(TenantInfo tenantInfo);

    void updateTenant(TenantInfo tenantInfo);

    void saveBatchUpdate(String tenant, MetadataBatchUpdate batchUpdate);

    void savePreallocatedIds(String tenant, List<TagHeader> ids);

    void savePreallocatedObjects(String tenant, List<Tag> tags);

    void saveNewObjects(String tenant, List<Tag> tags);

    void saveNewVersions(String tenant, List<Tag> tags);

    void saveNewTags(String tenant, List<Tag> tags);

    Tag loadObject(String tenant, TagSelector selector);

    List<Tag> loadObjects(String tenant, List<TagSelector> selector);

    List<Tag> search(String tenant, SearchParameters searchParameters);

    // -----------------------------------------------------------------------------------------------------------------
    // ALTERNATE LOAD METHODS
    // -----------------------------------------------------------------------------------------------------------------

    // These two methods are functionally equivalent to loadObjects()
    // They are used during object / tag updates to provide different error messages to the client
    // Implementations are free to use the default versions, this may affect the readability of error messages

    default List<Tag> loadPriorObjects(String tenant, List<TagSelector> selector) {
        return loadObjects(tenant, selector);
    }

    default List<Tag> loadPriorTags(String tenant, List<TagSelector> selector) {
        return loadObjects(tenant, selector);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // CONFIG ENTRIES
    // -----------------------------------------------------------------------------------------------------------------

    void saveConfigEntries(String tenant, List<ConfigEntry> configEntries);

    List<ConfigEntry> loadConfigEntries(String tenant, List<ConfigEntry> configKeys, boolean includeDeleted);

    ConfigEntry loadConfigEntry(String tenant, ConfigEntry configKey, boolean includeDeleted);

    List<ConfigEntry> listConfigEntries(String tenant, String configClass, boolean includeDeleted);

    // -----------------------------------------------------------------------------------------------------------------
    // LEGACY LOAD API
    // -----------------------------------------------------------------------------------------------------------------

    // Legacy API for loading a single object with explicit versions
    // This is just a convenience wrapper and should not be implemented separately

    default Tag loadObject(String tenant, ObjectType objectType, UUID objectId, int objectVersion, int tagVersion) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(objectVersion)
                .setTagVersion(tagVersion)
                .build();

        return loadObject(tenant, selector);
    }
}
