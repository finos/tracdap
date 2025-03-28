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

package org.finos.tracdap.svc.admin.services;

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.ConfigUpdate;
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.middleware.GrpcConcern;

import io.grpc.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;


public class ConfigService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient;
    private final NotifierService notifier;

    private final GrpcConcern commonConcerns;

    public ConfigService(
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient,
            GrpcConcern commonConcerns, NotifierService notifier) {

        this.metadataClient = metadataClient;
        this.commonConcerns = commonConcerns;
        this.notifier = notifier;
    }

    public ConfigWriteResponse createConfigObject(ConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var result = client.createConfigObject(request);

        var update = ConfigUpdate.newBuilder()
                .setTenant(request.getTenant())
                .setUpdateType(ConfigUpdateType.CREATE)
                .setConfigEntry(result.getEntry())
                .build();

        notifier.configUpdate(update);

        return result;
    }

    public ConfigWriteResponse updateConfigObject(ConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var result = client.updateConfigObject(request);

        var update = ConfigUpdate.newBuilder()
                .setTenant(request.getTenant())
                .setUpdateType(ConfigUpdateType.UPDATE)
                .setConfigEntry(result.getEntry())
                .build();

        notifier.configUpdate(update);

        return result;
    }

    public ConfigWriteResponse deleteConfigObject(ConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var result = client.deleteConfigObject(request);

        var update = ConfigUpdate.newBuilder()
                .setTenant(request.getTenant())
                .setUpdateType(ConfigUpdateType.DELETE)
                .setConfigEntry(result.getEntry())
                .build();

        notifier.configUpdate(update);

        return result;
    }

    public ConfigReadResponse readConfigObject(ConfigReadRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var entry = client.readConfigEntry(request);
        var selector = entry.getEntry().getDetails().getObjectSelector();

        var objectRequest = MetadataReadRequest.newBuilder()
                .setTenant(request.getTenant())
                .setSelector(selector)
                .build();

        var object = client.readObject(objectRequest);

        return ConfigReadResponse.newBuilder()
                .setEntry(entry.getEntry())
                .setDefinition(object.getDefinition())
                .putAllAttrs(object.getAttrsMap())
                .build();
    }

    public ConfigReadBatchResponse readConfigBatch(ConfigReadBatchRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var entries = client.readConfigBatch(request);
        var selectors = entries.getEntriesList().stream()
                .map(entry -> entry.getEntry().getDetails().getObjectSelector())
                .collect(Collectors.toList());

        var objectRequest = MetadataBatchRequest.newBuilder()
                .setTenant(request.getTenant())
                .addAllSelector(selectors)
                .build();

        var objects = client.readBatch(objectRequest);

        var batchResponse = ConfigReadBatchResponse.newBuilder();

        for (int i = 0; i < request.getEntriesCount(); i++) {

            var entry = ConfigReadResponse.newBuilder()
                    .setEntry(entries.getEntries(i).getEntry())
                    .setDefinition(objects.getTag(i).getDefinition())
                    .putAllAttrs(objects.getTag(i).getAttrsMap());

            batchResponse.addEntries(entry);
        }

        return batchResponse.build();
    }

    public ConfigListResponse listConfigEntries(ConfigListRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        return client.listConfigEntries(request);
    }
}
