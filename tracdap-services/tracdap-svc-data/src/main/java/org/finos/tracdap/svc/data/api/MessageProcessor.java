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

package org.finos.tracdap.svc.data.api;

import org.finos.tracdap.api.MetadataReadRequest;
import org.finos.tracdap.api.internal.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.storage.StorageManager;
import org.finos.tracdap.metadata.ResourceDefinition;
import org.finos.tracdap.metadata.ResourceType;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;


public class MessageProcessor extends InternalMessagingApiGrpc.InternalMessagingApiImplBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final StorageManager storageManager;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metadataApi;
    private final ExecutorService offloadExecutor;

    public MessageProcessor(
            StorageManager storageManager,
            TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metadataApi,
            ExecutorService offloadExecutor) {

        this.storageManager = storageManager;
        this.metadataApi = metadataApi;
        this.offloadExecutor = offloadExecutor;
    }

    @Override
    public void configUpdate(ConfigUpdate request, StreamObserver<ReceivedStatus> response) {

        // Do not execute config updates on the primary event loop!
        offloadExecutor.execute(() -> configUpdateOffloaded(request, response));
    }

    public void configUpdateOffloaded(ConfigUpdate request, StreamObserver<ReceivedStatus> response) {

        log.info("Processing config update");
        log.info(request.toString());

        var entry = request.getConfigEntry();
        ReceivedStatus status;

        if (entry.getConfigClass().equals(ConfigKeys.TRAC_RESOURCES) &&
            entry.getDetails().getResourceType() == ResourceType.INTERNAL_STORAGE) {

            ResourceDefinition storageResource;

            switch (request.getUpdateType()) {

            case CREATE:
                storageResource = fetchResource(request);
                storageManager.addStorage(entry.getConfigKey(), storageResource);
                break;

            case UPDATE:
                storageResource = fetchResource(request);
                storageManager.updateStorage(entry.getConfigKey(), storageResource);
                break;

            case DELETE:
                storageManager.removeStorage(entry.getConfigKey());
                break;

            default:
                throw new EUnexpected();
            }

            status = ReceivedStatus.newBuilder()
                    .setCode(ReceivedCode.OK)
                    .build();
        }
        else {

            status = ReceivedStatus.newBuilder()
                    .setCode(ReceivedCode.IGNORED)
                    .build();
        }

        response.onNext(status);
        response.onCompleted();

        log.info("Config update complete");
    }

    private ResourceDefinition fetchResource(ConfigUpdate request) {

        var readRequest = MetadataReadRequest.newBuilder()
                .setTenant(request.getTenant())
                .setSelector(request.getConfigEntry().getDetails().getObjectSelector())
                .build();

        var configObject = metadataApi.readObject(readRequest);

        return configObject.getDefinition().getResource();
    }
}
