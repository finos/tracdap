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

package org.finos.tracdap.common.service;

import io.grpc.Context;
import org.finos.tracdap.api.PlatformConfigListRequest;
import org.finos.tracdap.api.PlatformConfigReadBatchRequest;
import org.finos.tracdap.api.PlatformConfigReadRequest;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.api.internal.ReceivedCode;
import org.finos.tracdap.api.internal.ReceivedStatus;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.PlatformConfigEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Tenant-less equivalent of {@link TenantStateManager}: holds a live, in-memory view of a
 * single platform config class, keyed by configKey, updated on receipt of platformConfigUpdate.
 */
public class PlatformStateManager implements IPlatformConfigListener {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String configClass;
    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient;
    private final GrpcConcern commonConcerns;
    private final ConcurrentHashMap<String, ObjectDefinition> liveConfig;

    public PlatformStateManager(
            String configClass,
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient,
            GrpcConcern commonConcerns) {

        this.configClass = configClass;
        this.metaClient = metaClient;
        this.commonConcerns = commonConcerns;
        this.liveConfig = new ConcurrentHashMap<>();
    }

    public void init() {

        log.info("Loading platform config: class = [{}]", configClass);

        var clientState = commonConcerns.prepareClientCall(Context.ROOT);
        var client = clientState.configureClient(metaClient);

        var listRequest = PlatformConfigListRequest.newBuilder()
                .setConfigClass(configClass)
                .build();

        var listResponse = client.listPlatformConfigEntries(listRequest);

        if (listResponse.getEntriesCount() == 0) {
            log.info("No platform config found: class = [{}]", configClass);
            return;
        }

        var readRequest = PlatformConfigReadBatchRequest.newBuilder()
                .addAllEntries(listResponse.getEntriesList())
                .build();

        var readResponse = client.readPlatformConfigBatch(readRequest);

        for (var entry : readResponse.getEntriesList())
            liveConfig.put(entry.getEntry().getConfigKey(), entry.getDefinition());

        log.info("Loaded {} platform config entrie(s): class = [{}]", liveConfig.size(), configClass);
    }

    public boolean hasConfig(String configKey) {
        return liveConfig.containsKey(configKey);
    }

    public ObjectDefinition getConfig(String configKey) {
        return liveConfig.get(configKey);
    }

    @Override
    public ReceivedStatus applyConfigUpdate(PlatformConfigUpdate update) {

        var entry = update.getConfigEntry();

        if (!entry.getConfigClass().equals(configClass)) {
            log.info("Platform config update ignored (not relevant: class = [{}])", entry.getConfigClass());
            return ReceivedStatus.newBuilder().setCode(ReceivedCode.IGNORED).build();
        }

        switch (update.getUpdateType()) {

            case DELETE:
                liveConfig.remove(entry.getConfigKey());
                break;

            case CREATE:
            case UPDATE:
                var definition = fetchDefinition(entry.getConfigKey());
                liveConfig.put(entry.getConfigKey(), definition);
                break;

            default:
                throw new EUnexpected();
        }

        log.info("Platform config update applied successfully: class = [{}], key = [{}]", configClass, entry.getConfigKey());

        return ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();
    }

    private ObjectDefinition fetchDefinition(String configKey) {

        var clientState = commonConcerns.prepareClientCall(Context.ROOT);
        var client = clientState.configureClient(metaClient);

        var entry = PlatformConfigEntry.newBuilder()
                .setConfigClass(configClass)
                .setConfigKey(configKey)
                .build();

        var readRequest = PlatformConfigReadRequest.newBuilder()
                .setEntry(entry)
                .build();

        return client.readPlatformConfigEntry(readRequest).getDefinition();
    }
}
