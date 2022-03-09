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

package com.accenture.trac.svc.meta.services;

import com.accenture.trac.metadata.TagSelector;
import com.accenture.trac.metadata.ObjectType;
import com.accenture.trac.metadata.Tag;
import com.accenture.trac.svc.meta.dal.IMetadataDal;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


public class MetadataReadService {

    private final IMetadataDal dal;

    public MetadataReadService(IMetadataDal dal) {
        this.dal = dal;
    }

    // Literally all of the read logic is in the DAL at present!
    // Which is fine, keep a thin logic class here anyway to have a consistent pattern

    public CompletableFuture<Tag> readObject(String tenant, TagSelector selector) {

        return dal.loadObject(tenant, selector);
    }

    public CompletableFuture<List<Tag>> readObjects(String tenant, List<TagSelector> selectors) {

        return dal.loadObjects(tenant, selectors);
    }

    public CompletableFuture<Tag> loadTag(
            String tenant, ObjectType objectType,
            UUID objectId, int objectVersion, int tagVersion) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(objectVersion)
                .setTagVersion(tagVersion)
                .build();

        return dal.loadObject(tenant, selector);
    }

    public CompletableFuture<Tag> loadLatestTag(
            String tenant, ObjectType objectType,
            UUID objectId, int objectVersion) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(objectVersion)
                .setLatestTag(true)
                .build();

        return dal.loadObject(tenant, selector);
    }

    public CompletableFuture<Tag> loadLatestObject(
            String tenant, ObjectType objectType,
            UUID objectId) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        return dal.loadObject(tenant, selector);
    }
}
