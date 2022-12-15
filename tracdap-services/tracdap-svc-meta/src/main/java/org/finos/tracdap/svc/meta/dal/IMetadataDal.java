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

import java.util.List;
import java.util.UUID;


public interface IMetadataDal {

    void start();

    void stop();

    List<TenantInfo> listTenants();

    void saveNewObjects(String tenant, List<Tag> tags);

    void saveNewVersions(String tenant, List<Tag> tags);

    void saveNewTags(String tenant, List<Tag> tags);

    void preallocateObjectIds(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds);

    void savePreallocatedObjects(String tenant, List<Tag> tags);

    Tag loadObject(String tenant, TagSelector selector);

    List<Tag> loadObjects(String tenant, List<TagSelector> selector);

    Tag loadTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion, int tagVersion);

    List<Tag> loadTags(String tenant, List<ObjectType> objectType, List<UUID> objectId, List<Integer> objectVersion, List<Integer> tagVersion);

    Tag loadLatestTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion);

    List<Tag> loadLatestTags(String tenant, List<ObjectType> objectType, List<UUID> objectId, List<Integer> objectVersion);

    Tag loadLatestVersion(String tenant, ObjectType objectType, UUID objectId);

    List<Tag> loadLatestVersions(String tenant, List<ObjectType> objectType, List<UUID> objectId);

    List<Tag> search(String tenant, SearchParameters searchParameters);

}
