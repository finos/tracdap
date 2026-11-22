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
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.exception.ECacheCorruption;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.svc.orch.service.JobManager;
import org.finos.tracdap.svc.orch.service.JobProcessor;
import org.finos.tracdap.svc.orch.service.JobState;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


public class TracOrchestratorApi extends TracOrchestratorApiGrpc.TracOrchestratorApiImplBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JobManager jobManager;
    private final JobProcessor jobProcessor;
    private final GrpcConcern commonConcerns;


    public TracOrchestratorApi(PluginRegistry registry, GrpcConcern commonConcerns) {

        this.jobManager = registry.getSingleton(JobManager.class);
        this.jobProcessor = registry.getSingleton(JobProcessor.class);
        this.commonConcerns = commonConcerns;
    }

    @Override
    public void validateJob(JobRequest request, StreamObserver<JobStatus> response) {

        try {
            var result = validateJobImpl(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    @Override
    public void submitJob(JobRequest request, StreamObserver<JobStatus> response) {

        try {
            var result = submitJobImpl(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    @Override
    public void checkJob(JobStatusRequest request, StreamObserver<JobStatus> response) {

        try {
            var result = checkJobImpl(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    @Override
    public void followJob(JobStatusRequest request, StreamObserver<JobStatus> response) {
        super.followJob(request, response);
    }

    @Override
    public void cancelJob(JobStatusRequest request, StreamObserver<JobStatus> response) {
        super.cancelJob(request, response);
    }

    private JobStatus validateJobImpl(JobRequest request) {

        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var jobState = jobProcessor.newJob(request, clientConfig);
        var assembled = jobProcessor.assembleAndValidate(jobState);

        return jobProcessor.getStatus(assembled);
    }

    private JobStatus submitJobImpl(JobRequest request) {

        var clientConfig = commonConcerns.prepareClientCall(Context.current());
        var jobState = jobProcessor.newJob(request, clientConfig);
        var assembled = jobProcessor.assembleAndValidate(jobState);
        var cached = jobManager.addNewJob(assembled);

        return jobProcessor.getStatus(cached);
    }

    private JobStatus checkJobImpl(JobStatusRequest request) {

        var selector = request.getSelector();

        // A JOB is only ever tagged, so object version 1 is the only version that exists
        if (selector.hasObjectVersion() && selector.getObjectVersion() != 1) {

            var message = String.format(
                    "Metadata not found for %s [%s] (version = %d)",
                    selector.getObjectType(), selector.getObjectId(), selector.getObjectVersion());

            throw new EMetadataNotFound(message);
        }

        var jobSelector = TagSelector.newBuilder()
                .setObjectType(selector.getObjectType())
                .setObjectId(selector.getObjectId())
                .setObjectVersion(1)
                .setLatestTag(true)
                .build();

        var jobKey = MetadataUtil.objectKey(jobSelector);

        return jobProcessor.checkJob(request, jobSelector, queryCachedJob(jobKey));
    }

    private Optional<JobState> queryCachedJob(String jobKey) {

        try {
            return jobManager.queryJob(jobKey);
        }
        catch (ECacheCorruption e) {
            log.warn("Cache entry for [{}] is not readable, status will be read from metadata: {}", jobKey, e.getMessage());
            return Optional.empty();
        }
    }
}
