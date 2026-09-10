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
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.metadata.PlatformConfigEntry;

import io.grpc.Context;


public class PlatformConfigService {

    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient;
    private final GrpcConcern commonConcerns;
    private final NotifierService notifier;

    public PlatformConfigService(
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient,
            GrpcConcern commonConcerns, NotifierService notifier) {

        this.metadataClient = metadataClient;
        this.commonConcerns = commonConcerns;
        this.notifier = notifier;
    }

    public PlatformConfigWriteResponse createPlatformConfigObject(PlatformConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var result = client.createPlatformConfigObject(request);

        notifyUpdate(ConfigUpdateType.CREATE, result.getEntry());

        return result;
    }

    public PlatformConfigWriteResponse updatePlatformConfigObject(PlatformConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var result = client.updatePlatformConfigObject(request);

        notifyUpdate(ConfigUpdateType.UPDATE, result.getEntry());

        return result;
    }

    public PlatformConfigWriteResponse deletePlatformConfigObject(PlatformConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var result = client.deletePlatformConfigObject(request);

        notifyUpdate(ConfigUpdateType.DELETE, result.getEntry());

        return result;
    }

    public PlatformConfigReadResponse readPlatformConfigObject(PlatformConfigReadRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        return client.readPlatformConfigEntry(request);
    }

    public PlatformConfigReadBatchResponse readPlatformConfigBatch(PlatformConfigReadBatchRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        return client.readPlatformConfigBatch(request);
    }

    public PlatformConfigListResponse listPlatformConfigEntries(PlatformConfigListRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        return client.listPlatformConfigEntries(request);
    }

    private void notifyUpdate(ConfigUpdateType updateType, PlatformConfigEntry entry) {

        var update = PlatformConfigUpdate.newBuilder()
                .setUpdateType(updateType)
                .setConfigEntry(entry)
                .build();

        notifier.platformConfigUpdate(update);
    }
}
