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

package org.finos.tracdap.svc.orch.api;

import io.grpc.Context;
import org.finos.tracdap.api.MetadataReadRequest;
import org.finos.tracdap.api.internal.*;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.DynamicConfig;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.metadata.ResourceDefinition;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MessageProcessor extends InternalMessagingApiGrpc.InternalMessagingApiImplBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataApi;
    private final GrpcConcern commonConcerns;
    private final DynamicConfig.Resources resources;

    public MessageProcessor(
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataApi,
            GrpcConcern commonConcerns, DynamicConfig.Resources resources) {

        this.metadataApi = metadataApi;
        this.commonConcerns = commonConcerns;
        this.resources = resources;
    }

    @Override
    public void  configUpdate(ConfigUpdate request, StreamObserver<ReceivedStatus> response) {

        log.info("Processing config update");
        log.info(request.toString());

        var entry = request.getConfigEntry();
        ReceivedStatus status;

        if (entry.getConfigClass().equals(ConfigKeys.TRAC_RESOURCES)) {

            ResourceDefinition storageResource;

            switch (request.getUpdateType()) {

                case CREATE:
                    storageResource = fetchResource(request);
                    resources.addEntry(entry.getConfigKey(), storageResource);
                    break;

                case UPDATE:
                    storageResource = fetchResource(request);
                    resources.updateEntry(entry.getConfigKey(), storageResource);
                    break;

                case DELETE:
                    resources.removeEntry(entry.getConfigKey());
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

        // Apply common concerns using the current gRPC context
        var clientState = commonConcerns.prepareClientCall(Context.current());
        var client = clientState.configureClient(metadataApi);

        var configObject = client.readObject(readRequest);

        return configObject.getDefinition().getResource();
    }
}
