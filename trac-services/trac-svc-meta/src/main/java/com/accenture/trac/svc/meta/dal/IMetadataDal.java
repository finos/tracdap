/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.dal;

import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.common.metadata.TagSelector;
import com.accenture.trac.common.metadata.search.SearchParameters;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


public interface IMetadataDal {

    CompletableFuture<Void> saveNewObject(String tenant, Tag tag);

    CompletableFuture<Void> saveNewObjects(String tenant, List<Tag> tags);

    CompletableFuture<Void> saveNewVersion(String tenant, Tag tag);

    CompletableFuture<Void> saveNewVersions(String tenant, List<Tag> tags);

    CompletableFuture<Void> saveNewTag(String tenant, Tag tag);

    CompletableFuture<Void> saveNewTags(String tenant, List<Tag> tags);

    CompletableFuture<Void> preallocateObjectId(String tenant, ObjectType objectType, UUID objectId);

    CompletableFuture<Void> preallocateObjectIds(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds);

    CompletableFuture<Void> savePreallocatedObject(String tenant, Tag tag);

    CompletableFuture<Void> savePreallocatedObjects(String tenant, List<Tag> tags);

    CompletableFuture<Tag>
    loadObject(String tenant, TagSelector selector);

    CompletableFuture<List<Tag>>
    loadObjects(String tenant, List<TagSelector> selector);

    CompletableFuture<Tag>
    loadTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion, int tagVersion);

    CompletableFuture<List<Tag>>
    loadTags(String tenant, List<ObjectType> objectType, List<UUID> objectId, List<Integer> objectVersion, List<Integer> tagVersion);

    CompletableFuture<Tag>
    loadLatestTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion);

    CompletableFuture<List<Tag>>
    loadLatestTags(String tenant, List<ObjectType> objectType, List<UUID> objectId, List<Integer> objectVersion);

    CompletableFuture<Tag>
    loadLatestVersion(String tenant, ObjectType objectType, UUID objectId);

    CompletableFuture<List<Tag>>
    loadLatestVersions(String tenant, List<ObjectType> objectType, List<UUID> objectId);


    CompletableFuture<List<Tag>>
    search(String tenant, SearchParameters searchParameters);

}
