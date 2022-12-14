/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.orch.service;

import io.grpc.MethodDescriptor;
import org.finos.tracdap.api.MetadataWriteBatchRequest;
import org.finos.tracdap.api.MetadataWriteBatchResponse;
import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.api.TrustedMetadataApiGrpc;
import org.finos.tracdap.common.grpc.GrpcClientWrap;
import org.finos.tracdap.common.metadata.MetadataConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Optimize GRPC API usage by batching <class>MetadataWriteRequest</class>.
 */
class BatchMetadataWriteAPI {
    private static final MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> CREATE_OBJECT_BATCH_METHOD = TrustedMetadataApiGrpc.getCreateObjectBatchMethod();
    private static final MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> UPDATE_OBJECT_BATCH_METHOD = TrustedMetadataApiGrpc.getUpdateObjectBatchMethod();
    private static final MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> UPDATE_TAG_BATCH_METHOD = TrustedMetadataApiGrpc.getUpdateTagBatchMethod();
    private static final MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> CREATE_PREALLOCATED_OBJECT_BATCH_METHOD = TrustedMetadataApiGrpc.getCreatePreallocatedObjectBatchMethod();

    private final List<MetadataWriteRequest> createPreallocatedObject = new ArrayList<>();
    private final List<MetadataWriteRequest> createObject = new ArrayList<>();
    private final List<MetadataWriteRequest> updateObject = new ArrayList<>();
    private final List<MetadataWriteRequest> updateTag = new ArrayList<>();

    private final String tenant;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metadataClient;
    private final GrpcClientWrap grpcWrap;

    BatchMetadataWriteAPI(String tenant, TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub metadataClient, GrpcClientWrap grpcWrap) {
        this.tenant = tenant;
        this.metadataClient = metadataClient;
        this.grpcWrap = grpcWrap;
    }

    /**
     * Add a request without sending it.
     * @param update An update to be added to waiting queue.
     */
    public void add(MetadataWriteRequest update) {
        var targetList = updateObject;

        if (!update.hasDefinition()) {
            targetList = updateTag;
        } else if (!update.hasPriorVersion()) {
            targetList = createObject;
        } else if (update.getPriorVersion().getObjectVersion() < MetadataConstants.OBJECT_FIRST_VERSION) {
            targetList = createPreallocatedObject;
        }

        targetList.add(scrapTenant(update));
    }

    /**
     * Send all requests in a batch manner. Clear the requests after the operation.
     */
    public void send() {
        callApi(
                this.grpcWrap,
                tenant,
                CREATE_PREALLOCATED_OBJECT_BATCH_METHOD,
                this.metadataClient::createPreallocatedObjectBatch,
                createPreallocatedObject
        );

        callApi(
                this.grpcWrap,
                tenant,
                CREATE_OBJECT_BATCH_METHOD,
                this.metadataClient::createObjectBatch,
                createObject
        );

        callApi(
                this.grpcWrap,
                tenant,
                UPDATE_OBJECT_BATCH_METHOD,
                this.metadataClient::updateObjectBatch,
                updateObject
        );

        callApi(
                this.grpcWrap,
                tenant,
                UPDATE_TAG_BATCH_METHOD,
                this.metadataClient::updateTagBatch,
                updateTag
        );
    }

    private void callApi(
            GrpcClientWrap grpcWrap,
            String tenant,
            MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> methodDescriptor,
            Function<MetadataWriteBatchRequest, MetadataWriteBatchResponse> methodImpl,
            List<MetadataWriteRequest> requests
    ) {
        if (requests.isEmpty()) {
            return;
        }

        grpcWrap.unaryCall(
                methodDescriptor,
                MetadataWriteBatchRequest.newBuilder()
                        .setTenant(tenant)
                        .addAllRequests(requests)
                        .build(),
                methodImpl);

        requests.clear();
    }

    /**
     * Remove tenant from write request.
     * Necessary when you want to add the request to a batch write request.
     */
    public static MetadataWriteRequest scrapTenant(MetadataWriteRequest request) {
        return MetadataWriteRequest.newBuilder(request)
                .clearTenant().build();
    }
}
