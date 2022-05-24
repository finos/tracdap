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

package org.finos.tracdap.svc.orch.api;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.grpc.GrpcServerWrap;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.svc.orch.service.JobApiService;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;


public class TracOrchestratorApi extends TracOrchestratorApiGrpc.TracOrchestratorApiImplBase {

    private static final String SERVICE_NAME = TracOrchestratorApiGrpc.SERVICE_NAME.substring(TracOrchestratorApiGrpc.SERVICE_NAME.lastIndexOf(".") + 1);
    private static final Descriptors.ServiceDescriptor TRAC_ORCHESTRATOR_SERVICE = Orchestrator.getDescriptor().findServiceByName(SERVICE_NAME);

    private static final MethodDescriptor<JobRequest, JobStatus> VALIDATE_JOB_METHOD = TracOrchestratorApiGrpc.getValidateJobMethod();
    private static final MethodDescriptor<JobRequest, JobStatus> SUBMIT_JOB_METHOD = TracOrchestratorApiGrpc.getSubmitJobMethod();
    private static final MethodDescriptor<JobStatusRequest, JobStatus> CHECK_JOB_METHOD = TracOrchestratorApiGrpc.getCheckJobMethod();
    private static final MethodDescriptor<JobStatusRequest, JobStatus> FOLLOW_JOB_METHOD = TracOrchestratorApiGrpc.getFollowJobMethod();
    private static final MethodDescriptor<TagSelector, JobStatus> CANCEL_JOB_METHOD = TracOrchestratorApiGrpc.getCancelJobMethod();

    private final Validator validator;
    private final JobApiService orchestrator;
    private final GrpcServerWrap grpcWrap;

    public TracOrchestratorApi(JobApiService orchestrator) {

        this.validator = new Validator();
        this.orchestrator = orchestrator;
        this.grpcWrap = new GrpcServerWrap(getClass());
    }

    @Override
    public void validateJob(JobRequest request, StreamObserver<JobStatus> responseObserver) {

        grpcWrap.unaryCall(
                VALIDATE_JOB_METHOD, request, responseObserver,
                apiFunc(VALIDATE_JOB_METHOD, orchestrator::validateJob));
    }

    @Override
    public void submitJob(JobRequest request, StreamObserver<JobStatus> responseObserver) {

        grpcWrap.unaryCall(
                SUBMIT_JOB_METHOD, request, responseObserver,
                apiFunc(SUBMIT_JOB_METHOD, orchestrator::submitJob));
    }

    @Override
    public void checkJob(JobStatusRequest request, StreamObserver<JobStatus> responseObserver) {

        grpcWrap.unaryCall(
                CHECK_JOB_METHOD, request, responseObserver,
                apiFunc(CHECK_JOB_METHOD, orchestrator::checkJob));
    }

    @Override
    public void followJob(JobStatusRequest request, StreamObserver<JobStatus> responseObserver) {
        super.followJob(request, responseObserver);
    }

    @Override
    public void cancelJob(TagSelector request, StreamObserver<JobStatus> responseObserver) {
        super.cancelJob(request, responseObserver);
    }

    private <TReq extends Message, TResp extends Message>
    Function<TReq, CompletionStage<TResp>>
    apiFunc(MethodDescriptor<TReq, TResp> method, Function<TReq, CompletionStage<TResp>> func) {

        var protoMethod = TRAC_ORCHESTRATOR_SERVICE.findMethodByName(method.getBareMethodName());

        return req -> {

            validator.validateFixedMethod(req, protoMethod);

            return func.apply(req);
        };
    }
}
