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

package com.accenture.trac.svc.orch.service;

import com.accenture.trac.api.*;
import com.accenture.trac.common.grpc.GrpcClientWrap;
import com.accenture.trac.metadata.JobDefinition;
import com.accenture.trac.metadata.JobType;
import com.accenture.trac.metadata.ObjectDefinition;
import com.accenture.trac.metadata.ObjectType;
import com.accenture.trac.metadata.TagHeader;

import com.accenture.trac.svc.orch.jobs.JobLogic;
import io.grpc.MethodDescriptor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class JobApiService {

    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> CREATE_OBJECT_METHOD = TrustedMetadataApiGrpc.getCreateObjectMethod();

    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient;
    private final GrpcClientWrap grpcWrap;

    public JobApiService(TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient) {

        this.metaClient = metaClient;
        this.grpcWrap = new GrpcClientWrap(getClass());
    }

    public CompletableFuture<JobStatus> validateJob(JobRequest request) {

        var state = new RequestState(request);

        return CompletableFuture.completedFuture(state)

                .thenApply(s -> jobStatus(s, JobStatusCode.PREPARING))

                .thenCompose(this::assembleAndValidate)

                .thenApply(s -> jobStatus(s, JobStatusCode.VALIDATED))

                .thenApply(this::reportStatus);
    }

    public CompletableFuture<JobStatus> executeJob(JobRequest request) {

        var state = new RequestState(request);

        return CompletableFuture.completedFuture(state)

                .thenApply(s -> jobStatus(s, JobStatusCode.PREPARING))

                .thenCompose(this::assembleAndValidate)

                .thenApply(s -> jobStatus(s, JobStatusCode.VALIDATED))
                .thenApply(s -> jobStatus(s, JobStatusCode.PENDING))

                .thenCompose(this::saveMetadata)

                .thenCompose(this::submitForExecution)

                .thenApply(this::reportStatus);
    }

    void checkJob() {

    }

    void getJobResult() {

    }



    private RequestState jobStatus(RequestState state, JobStatusCode statusCode) {

        state.statusCode = statusCode;
        return state;
    }


    private CompletionStage<RequestState> assembleAndValidate(RequestState request) {

        // static validate
        // load related metadata
        // semantic validate

        return CompletableFuture.completedFuture(buildMetadata(request));
    }

    private RequestState buildMetadata(RequestState request) {

        var jobLogic = JobLogic.forJobType(request.jobType);

        request.jobDef = jobLogic.buildJobDefinition(request.jobRequest);

        return request;
    }

    private CompletionStage<RequestState> saveMetadata(RequestState request) {

        var jobObj = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.JOB)
                .setJob(request.jobDef)
                .build();

        var jobWriteReq = MetadataWriteRequest.newBuilder()
                .setTenant(request.tenant)
                .setDefinition(jobObj)
                .build();

        var grpcCall = grpcWrap.unaryCall(
                CREATE_OBJECT_METHOD, jobWriteReq,
                metaClient::createObject);

        return grpcCall.thenApply(x -> request);
    }

    private CompletionStage<RequestState> submitForExecution(RequestState request) {

        return CompletableFuture.failedFuture(new RuntimeException("not implemented yet"));
    }

    private JobStatus reportStatus(RequestState request) {

        return JobStatus.newBuilder()
                .setStatus(request.statusCode)
                .setMessage("")
                .build();
    }



    private static class RequestState {

        RequestState(JobRequest request) {

            this.tenant = request.getTenant();
            this.jobType = request.getJobType();
            this.jobRequest = request;
        }

        String tenant;
        JobType jobType;
        JobRequest jobRequest;

        JobStatusCode statusCode;

        JobDefinition jobDef;
    }
}
