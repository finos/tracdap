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
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.exception.EConsistencyValidation;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.metadata.ResourceType;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;


public class TenantService {

    private static final String STORAGE_ROOT = ".";

    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient;
    private final TracStorageApiGrpc.TracStorageApiBlockingStub storageClient;
    private final GrpcConcern commonConcerns;
    private final NotifierService notifier;

    public TenantService(
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient,
            TracStorageApiGrpc.TracStorageApiBlockingStub storageClient,
            GrpcConcern commonConcerns, NotifierService notifier) {

        this.metadataClient = metadataClient;
        this.storageClient = storageClient;
        this.commonConcerns = commonConcerns;
        this.notifier = notifier;
    }

    public CreateTenantResponse createTenant(CreateTenantRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var result = client.createTenant(request);

        // Services that do not know the tenant yet discover it from these notifications
        for (var entry : result.getEntriesList()) {

            var update = ConfigUpdate.newBuilder()
                    .setTenant(request.getTenantCode())
                    .setUpdateType(ConfigUpdateType.CREATE)
                    .setConfigEntry(entry)
                    .setSecretsUpdated(false)
                    .build();

            notifier.configUpdate(update);
        }

        return result;
    }

    public CheckTenantStorageResponse checkTenantStorage(CheckTenantStorageRequest request) {

        if (storageClient == null)
            throw new EConsistencyValidation("No data service is configured");

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var listRequest = ConfigListRequest.newBuilder()
                .setTenant(request.getTenantCode())
                .setConfigClass(ConfigKeys.TRAC_RESOURCES)
                .setResourceType(ResourceType.INTERNAL_STORAGE)
                .build();

        var storageEntries = client.listConfigEntries(listRequest);

        // The caller has already been authorized, storage is checked under the platform's own identity
        var rootStorageClient = commonConcerns
                .prepareClientCall(Context.ROOT)
                .configureClient(storageClient);

        var response = CheckTenantStorageResponse.newBuilder();

        for (var entry : storageEntries.getEntriesList()) {

            var storageRequest = StorageRequest.newBuilder()
                    .setTenant(request.getTenantCode())
                    .setStorageKey(entry.getConfigKey())
                    .setStoragePath(STORAGE_ROOT)
                    .build();

            response.putStorage(entry.getConfigKey(), storageExists(rootStorageClient, storageRequest));
        }

        return response.build();
    }

    private boolean storageExists(TracStorageApiGrpc.TracStorageApiBlockingStub client, StorageRequest request) {

        try {
            return client.exists(request).getExists();
        }
        catch (StatusRuntimeException e) {

            // The data service reports an unknown tenant as NOT_FOUND and an unknown storage key as INTERNAL
            // Both mean the data service has not loaded the tenant's storage yet
            var code = e.getStatus().getCode();

            if (code == Status.Code.NOT_FOUND || code == Status.Code.INTERNAL)
                return false;

            throw e;
        }
    }
}
