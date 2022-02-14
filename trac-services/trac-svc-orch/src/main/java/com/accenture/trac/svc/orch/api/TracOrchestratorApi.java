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

package com.accenture.trac.svc.orch.api;

import com.accenture.trac.api.*;
import com.accenture.trac.common.grpc.GrpcServerWrap;
import com.accenture.trac.metadata.TagSelector;
import com.accenture.trac.svc.orch.service.JobApiService;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;


public class TracOrchestratorApi extends TracOrchestratorApiGrpc.TracOrchestratorApiImplBase {

    private static final MethodDescriptor<JobRequest, JobStatus> VALIDATE_JOB_METHOD = TracOrchestratorApiGrpc.getValidateJobMethod();
    private static final MethodDescriptor<JobRequest, JobStatus> SUBMIT_JOB_METHOD = TracOrchestratorApiGrpc.getSubmitJobMethod();
    private static final MethodDescriptor<JobStatusRequest, JobStatus> CHECK_JOB_METHOD = TracOrchestratorApiGrpc.getCheckJobMethod();
    private static final MethodDescriptor<JobStatusRequest, JobStatus> FOLLOW_JOB_METHOD = TracOrchestratorApiGrpc.getFollowJobMethod();
    private static final MethodDescriptor<TagSelector, JobStatus> CANCEL_JOB_METHOD = TracOrchestratorApiGrpc.getCancelJobMethod();

    private final JobApiService orchestrator;
    private final GrpcServerWrap grpcWrap;

    public TracOrchestratorApi(JobApiService orchestrator) {

        this.orchestrator = orchestrator;
        this.grpcWrap = new GrpcServerWrap(getClass());
    }

    @Override
    public void validateJob(JobRequest request, StreamObserver<JobStatus> responseObserver) {

        grpcWrap.unaryCall(VALIDATE_JOB_METHOD, request, responseObserver, orchestrator::validateJob);
    }

    @Override
    public void submitJob(JobRequest request, StreamObserver<JobStatus> responseObserver) {

        grpcWrap.unaryCall(SUBMIT_JOB_METHOD, request, responseObserver, orchestrator::submitJob);
    }

    @Override
    public void checkJob(JobStatusRequest request, StreamObserver<JobStatus> responseObserver) {
        super.followJob(request, responseObserver);
    }

    @Override
    public void followJob(JobStatusRequest request, StreamObserver<JobStatus> responseObserver) {
        super.followJob(request, responseObserver);
    }

    @Override
    public void cancelJob(TagSelector request, StreamObserver<JobStatus> responseObserver) {
        super.cancelJob(request, responseObserver);
    }
}
