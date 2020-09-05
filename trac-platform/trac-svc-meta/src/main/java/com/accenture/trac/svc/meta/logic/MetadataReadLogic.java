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

package com.accenture.trac.svc.meta.logic;

import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.Tag;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;


public class MetadataReadLogic {

    private final IMetadataDal dal;

    public MetadataReadLogic(IMetadataDal dal) {
        this.dal = dal;
    }

    public CompletableFuture<Tag> loadTag(
            String tenant, ObjectType objectType,
            UUID objectId, int objectVersion, int tagVersion) {

        return dal.loadTag(tenant, objectType, objectId, objectVersion, tagVersion);
    }

    public CompletableFuture<Tag> loadLatestTag(
            String tenant, ObjectType objectType,
            UUID objectId, int objectVersion) {

        return dal.loadLatestTag(tenant, objectType, objectId, objectVersion);
    }

    public CompletableFuture<Tag> loadLatestObject(
            String tenant, ObjectType objectType,
            UUID objectId) {

        return dal.loadLatestVersion(tenant, objectType, objectId);
    }
}
