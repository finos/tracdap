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

package com.accenture.trac.svc.data.service;

import org.finos.tracdap.api.MetadataBatchRequest;
import org.finos.tracdap.api.MetadataReadRequest;
import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.metadata.*;
import com.google.protobuf.Message;

import java.util.List;

public class MetadataBuilders {

    static MetadataWriteRequest preallocateRequest(String tenant, ObjectType objectType) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(objectType)
                .build();
    }

    static MetadataReadRequest requestForSelector(String tenant, TagSelector selector) {

        return MetadataReadRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(selector)
                .build();
    }

    static MetadataBatchRequest requestForBatch(String tenant, TagSelector... selectors) {

        var batch = MetadataBatchRequest.newBuilder()
                .setTenant(tenant);

        for (var selector : selectors)
            batch.addSelector(selector);

        return batch.build();
    }

    static TagHeader bumpVersion(TagHeader priorVersion) {

        return priorVersion.toBuilder()
                .setObjectVersion(priorVersion.getObjectVersion() + 1)
                .setTagVersion(1)
                .build();
    }

    static ObjectDefinition objectOf(Message def) {

        if (def instanceof DataDefinition)
            return objectOf((DataDefinition) def);

        if (def instanceof FileDefinition)
            return objectOf((FileDefinition) def);

        if (def instanceof SchemaDefinition)
            return objectOf((SchemaDefinition) def);

        if (def instanceof StorageDefinition)
            return objectOf((StorageDefinition) def);

        throw new EUnexpected();
    }

    static ObjectDefinition objectOf(DataDefinition def) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setData(def)
                .build();
    }

    static ObjectDefinition objectOf(FileDefinition def) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.FILE)
                .setFile(def)
                .build();
    }

    static ObjectDefinition objectOf(SchemaDefinition def) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setSchema(def)
                .build();
    }

    static ObjectDefinition objectOf(StorageDefinition def) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.STORAGE)
                .setStorage(def)
                .build();
    }

    static <TDef extends Message> MetadataWriteRequest buildCreateObjectReq(
            String tenant, TagSelector priorVersion,
            TDef definition, List<TagUpdate> tagUpdates) {

        var objectDef = objectOf(definition);

        return MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(objectDef.getObjectType())
                .setPriorVersion(priorVersion)
                .setDefinition(objectDef)
                .addAllTagUpdates(tagUpdates)
                .build();
    }
}
