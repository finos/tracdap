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

package org.finos.tracdap.svc.meta.dal;

import org.finos.tracdap.metadata.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public interface IMetadataDal {

    void start();

    void stop();

    List<TenantInfo> listTenants();

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
    // They are provided as alternates in case implementations want to perform different error handling

    default List<Tag> loadPriorObjects(String tenant, List<TagSelector> selector) {
        return loadObjects(tenant, selector);
    }

    default List<Tag> loadPriorTags(String tenant, List<TagSelector> selector) {
        return loadObjects(tenant, selector);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LEGACY LOAD API
    // -----------------------------------------------------------------------------------------------------------------

    // Legacy API for loading a single object with explicit versions
    // Now this is just a wrapper around loadObject(), there is no need to implement separately

    default Tag loadTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion, int tagVersion) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(objectVersion)
                .setTagVersion(tagVersion)
                .build();

        return loadObject(tenant, selector);
    }
}
