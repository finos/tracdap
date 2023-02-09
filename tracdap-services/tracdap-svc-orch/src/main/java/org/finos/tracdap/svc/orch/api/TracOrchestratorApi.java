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
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.grpc.GrpcServerWrap;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.validation.Validator;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import org.finos.tracdap.svc.orch.service.JobManager;
import org.finos.tracdap.svc.orch.service.JobProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;


public class TracOrchestratorApi extends TracOrchestratorApiGrpc.TracOrchestratorApiImplBase {

    private static final String SERVICE_NAME = TracOrchestratorApiGrpc.SERVICE_NAME.substring(TracOrchestratorApiGrpc.SERVICE_NAME.lastIndexOf(".") + 1);
    private static final Descriptors.ServiceDescriptor TRAC_ORCHESTRATOR_SERVICE = Orchestrator.getDescriptor().findServiceByName(SERVICE_NAME);

    private static final MethodDescriptor<JobRequest, JobStatus> VALIDATE_JOB_METHOD = TracOrchestratorApiGrpc.getValidateJobMethod();
    private static final MethodDescriptor<JobRequest, JobStatus> SUBMIT_JOB_METHOD = TracOrchestratorApiGrpc.getSubmitJobMethod();
    private static final MethodDescriptor<JobStatusRequest, JobStatus> CHECK_JOB_METHOD = TracOrchestratorApiGrpc.getCheckJobMethod();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Validator validator;
    private final GrpcServerWrap grpcWrap;
    private final JobManager jobManager;
    private final JobProcessor jobProcessor;


    public TracOrchestratorApi(JobManager jobManager, JobProcessor jobProcessor) {

        this.validator = new Validator();
        this.grpcWrap = new GrpcServerWrap();
        this.jobManager = jobManager;
        this.jobProcessor = jobProcessor;
    }

    @Override
    public void validateJob(JobRequest request, StreamObserver<JobStatus> responseObserver) {

        grpcWrap.unaryCall(
                request, responseObserver,
                apiFunc(VALIDATE_JOB_METHOD, this::validateJobImpl));
    }

    @Override
    public void submitJob(JobRequest request, StreamObserver<JobStatus> responseObserver) {

        grpcWrap.unaryCall(
                request, responseObserver,
                apiFunc(SUBMIT_JOB_METHOD, this::submitJobImpl));
    }

    @Override
    public void checkJob(JobStatusRequest request, StreamObserver<JobStatus> responseObserver) {

        grpcWrap.unaryCall(
                request, responseObserver,
                apiFunc(CHECK_JOB_METHOD, this::checkJobImpl));
    }

    @Override
    public void followJob(JobStatusRequest request, StreamObserver<JobStatus> responseObserver) {
        super.followJob(request, responseObserver);
    }

    @Override
    public void cancelJob(JobStatusRequest request, StreamObserver<JobStatus> responseObserver) {
        super.cancelJob(request, responseObserver);
    }

    private <TReq extends Message, TResp extends Message>
    Function<TReq, TResp>
    apiFunc(MethodDescriptor<TReq, TResp> method, Function<TReq, TResp> func) {

        var protoMethod = TRAC_ORCHESTRATOR_SERVICE.findMethodByName(method.getBareMethodName());

        return req -> {

            validator.validateFixedMethod(req, protoMethod);

            return func.apply(req);
        };
    }

    private JobStatus validateJobImpl(JobRequest request) {

        var jobState = jobProcessor.newJob(request);
        var assembled = jobProcessor.assembleAndValidate(jobState);

        return jobProcessor.getStatus(assembled);
    }

    private JobStatus submitJobImpl(JobRequest request) {

        var jobState = jobProcessor.newJob(request);
        var assembled = jobProcessor.assembleAndValidate(jobState);
        var cached = jobManager.addNewJob(assembled);

        return jobProcessor.getStatus(cached);
    }

    private JobStatus checkJobImpl(JobStatusRequest request) {

        // TODO: Keys for other selector types
        if (!request.getSelector().hasObjectVersion())
            throw new EUnexpected();

        var jobKey = MetadataUtil.objectKey(request.getSelector());
        var jobState = jobManager.queryJob(jobKey);

        // TODO: Should there be a different error for jobs not found in the cache? EJobNotLive?
        if (jobState == null) {
            var message = String.format("Job not found (it may have completed): [%s]", jobKey);
            log.error(message);
            throw new EMetadataNotFound(message);
        }

        return jobProcessor.getStatus(jobState);
    }
}
