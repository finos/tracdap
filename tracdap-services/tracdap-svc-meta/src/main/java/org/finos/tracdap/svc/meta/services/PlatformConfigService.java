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

package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.metadata.store.IMetadataStore;

import io.grpc.Context;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.stream.Collectors;


public class PlatformConfigService {

    private final IMetadataStore metadataStore;

    public PlatformConfigService(IMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    public PlatformConfigWriteResponse createPlatformConfigObject(PlatformConfigWriteRequest request) {

        var timestamp = RequestMetadata.get(Context.current()).requestTimestamp().toInstant();
        var value = request.getDefinition().toByteArray();

        var entry = metadataStore.createPlatformConfigEntry(
                request.getConfigClass(), request.getConfigKey(), timestamp, value);

        return PlatformConfigWriteResponse.newBuilder().setEntry(entry).build();
    }

    public PlatformConfigWriteResponse updatePlatformConfigObject(PlatformConfigWriteRequest request) {

        var timestamp = RequestMetadata.get(Context.current()).requestTimestamp().toInstant();
        var value = request.getDefinition().toByteArray();

        var entry = metadataStore.updatePlatformConfigEntry(request.getPriorEntry(), timestamp, value);

        return PlatformConfigWriteResponse.newBuilder().setEntry(entry).build();
    }

    public PlatformConfigWriteResponse deletePlatformConfigObject(PlatformConfigWriteRequest request) {

        var timestamp = RequestMetadata.get(Context.current()).requestTimestamp().toInstant();

        var entry = metadataStore.deletePlatformConfigEntry(request.getPriorEntry(), timestamp);

        return PlatformConfigWriteResponse.newBuilder().setEntry(entry).build();
    }

    public PlatformConfigReadResponse readPlatformConfigEntry(PlatformConfigReadRequest request) {

        var key = request.getEntry();

        var record = metadataStore.loadPlatformConfigEntry(
                key.getConfigClass(), key.getConfigKey(), /* includeDeleted = */ false);

        return PlatformConfigReadResponse.newBuilder()
                .setEntry(record.entry())
                .setDefinition(parseDefinition(record.value()))
                .build();
    }

    public PlatformConfigReadBatchResponse readPlatformConfigBatch(PlatformConfigReadBatchRequest request) {

        var records = metadataStore.loadPlatformConfigEntries(
                request.getEntriesList(), /* includeDeleted = */ false);

        var entries = records.stream()
                .map(record -> PlatformConfigReadResponse.newBuilder()
                        .setEntry(record.entry())
                        .setDefinition(parseDefinition(record.value()))
                        .build())
                .collect(Collectors.toList());

        return PlatformConfigReadBatchResponse.newBuilder().addAllEntries(entries).build();
    }

    public PlatformConfigListResponse listPlatformConfigEntries(PlatformConfigListRequest request) {

        var entries = metadataStore.listPlatformConfigEntries(
                request.getConfigClass(), request.getIncludeDeleted());

        return PlatformConfigListResponse.newBuilder().addAllEntries(entries).build();
    }

    private org.finos.tracdap.metadata.ObjectDefinition parseDefinition(byte[] value) {

        try {
            return org.finos.tracdap.metadata.ObjectDefinition.parseFrom(value);
        }
        catch (InvalidProtocolBufferException e) {

            // Platform config values are always written by this service as serialized ObjectDefinition bytes
            // A parse failure here means the stored bytes have been corrupted, which is an internal error
            throw new EUnexpected();
        }
    }
}
