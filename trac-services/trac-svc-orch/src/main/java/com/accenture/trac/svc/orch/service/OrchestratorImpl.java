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

package com.accenture.trac.svc.orch.service;

import com.accenture.trac.api.*;
import com.accenture.trac.common.grpc.GrpcClientWrap;
import com.accenture.trac.metadata.ObjectDefinition;
import io.grpc.MethodDescriptor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class OrchestratorImpl {

    private static final MethodDescriptor<MetadataBatchRequest, MetadataBatchResponse> READ_BATCH_METHOD = TrustedMetadataApiGrpc.getReadBatchMethod();

    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient;
    private final GrpcClientWrap grpcWrap;

    public OrchestratorImpl(TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient) {

        this.metaClient = metaClient;
        this.grpcWrap = new GrpcClientWrap(getClass());
    }


    public interface IJobExecutor {

        void executeJob(JobConfig)
    }

    public interface IJobState {

    }

    public interface IJobLogic {

    }


    public interface INode {

    }

    public record JobNode (
            UUID nodeId,
            int nodeType) {}


    public record Graph (
            Map<String, INode> nodes,
            List<Map.Entry<String, String>> edges) {}

    public record SubGraph () {}





    public CompletionStage<JobStatus> executeJob(JobRequest request) {

        var stateRef = new RequestState();

        return CompletableFuture.completedFuture(stateRef)

                .thenCompose(state -> loadJobMetadata(request, state))

                /*

                - VALIDATED METADATA
                - BUILD LOGICAL JOB (maybe validate needs to be part of this step anyway)
                - RESOLVE TREE
                    * convert to execution tree
                    * generate node hashes
                    * check availability of all nodes, prune branches where possible
                    * for branches with no available inputs, expand back and repeat
                    * segment the tree into connected execution segments, which will be tasks
                    * create physical task definitions for each segment


                JOB tree node types

                    load external data
                    load internal data
                    query internal data
                    query external data
                    save data
                    run internal model
                    run external model
                    run task
                    import model


                 */

                // VALIDATE METADATA

                // BUILD LOGICAL JOB (or do together with validate)

                // resolve tree
                //




                .thenApply(state_ -> JobStatus.newBuilder().build());
    }



    private CompletionStage<RequestState>
    loadJobMetadata(JobRequest request, RequestState state) {

        var batchRequest = MetadataBatchRequest.newBuilder()
                .setTenant(request.getTenant());

        if (request.hasTarget())
            batchRequest.addSelector(request.getTarget());

        var orderedResources = new ArrayList<String>(request.getResourcesCount());

        for (var resource : request.getResourcesMap().entrySet()) {
            orderedResources.add(resource.getKey());
            batchRequest.addSelector(resource.getValue());
        }

        var finalBatchRequest = batchRequest.build();

        return grpcWrap
                .unaryCall(READ_BATCH_METHOD, finalBatchRequest, metaClient::readBatch)
                .thenApply(batchResponse -> loadJobMetadataResponse(request, batchResponse, orderedResources, state));
    }

    private RequestState loadJobMetadataResponse(
            JobRequest jobRequest, MetadataBatchResponse batchResponse,
            List<String> orderedResources, RequestState state) {

        if (jobRequest.hasTarget())
            state.target = batchResponse.getTag(0).getDefinition();

        for (var i = 0; i < jobRequest.getResourcesCount(); i++) {

            var resourceKey = orderedResources.get(i);
            var responseIndex = jobRequest.hasTarget() ? i + 1 : i;
            var resourceDef = batchResponse.getTag(responseIndex).getDefinition();

            state.resources.put(resourceKey, resourceDef);
        }

        return state;
    }


    static class RequestState {

        ObjectDefinition target;
        Map<String, ObjectDefinition> resources = new HashMap<>();
    }
}
