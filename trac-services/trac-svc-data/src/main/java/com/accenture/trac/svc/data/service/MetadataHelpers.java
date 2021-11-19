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

import com.accenture.trac.api.MetadataReadRequest;
import com.accenture.trac.api.MetadataWriteRequest;
import com.accenture.trac.api.TrustedMetadataApiGrpc;
import com.accenture.trac.common.concurrent.Futures;
import com.accenture.trac.common.metadata.MetadataUtil;
import com.accenture.trac.metadata.*;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

class MetadataHelpers {

    static <TDef> CompletionStage<TagHeader> createObject(
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi,
            String tenant, List<TagUpdate> tags, ObjectType objectType, TDef def,
            BiFunction<ObjectDefinition.Builder, TDef, ObjectDefinition.Builder> objSetter) {

        var objBuilder = ObjectDefinition.newBuilder().setObjectType(objectType);
        var obj = objSetter.apply(objBuilder, def);

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(objectType)
                .setDefinition(obj)
                .addAllTagUpdates(tags)
                .build();

        return Futures.javaFuture(metaApi.createObject(request));
    }

    static <TDef> CompletionStage<TagHeader> createPreallocated(
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi,
            String tenant, List<TagUpdate> tags, TagHeader objectHeader, TDef def,
            BiFunction<ObjectDefinition.Builder, TDef, ObjectDefinition.Builder> objSetter) {

        var objectType = objectHeader.getObjectType();
        var objBuilder = ObjectDefinition.newBuilder().setObjectType(objectType);
        var obj = objSetter.apply(objBuilder, def);

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(objectType)
                .setPriorVersion(MetadataUtil.selectorFor(objectHeader))
                .setDefinition(obj)
                .addAllTagUpdates(tags)
                .build();

        return Futures.javaFuture(metaApi.createPreallocatedObject(request));
    }

    static <TDef> CompletionStage<TagHeader> updateObject(
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi,
            String tenant, TagSelector priorVersion, TDef def, List<TagUpdate> tags,
            BiFunction<ObjectDefinition.Builder, TDef, ObjectDefinition.Builder> objSetter) {

        var objBuilder = ObjectDefinition.newBuilder().setObjectType(priorVersion.getObjectType());
        var obj = objSetter.apply(objBuilder, def);

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(priorVersion.getObjectType())
                .setPriorVersion(priorVersion)
                .setDefinition(obj)
                .addAllTagUpdates(tags)
                .build();

        return Futures.javaFuture(metaApi.updateObject(request));
    }

    static CompletionStage<Tag> readObject(
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi,
            String tenant, TagSelector selector) {

        var metaRequest = MetadataReadRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(selector)
                .build();

        return Futures.javaFuture(metaApi.readObject(metaRequest));
    }
}
