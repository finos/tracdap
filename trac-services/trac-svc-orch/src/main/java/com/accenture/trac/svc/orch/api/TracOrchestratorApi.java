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

import com.accenture.trac.api.JobRequest;
import com.accenture.trac.api.JobStatus;
import com.accenture.trac.api.TracOrchestratorApiGrpc;
import com.accenture.trac.metadata.TagSelector;
import io.grpc.stub.StreamObserver;

public class TracOrchestratorApi extends TracOrchestratorApiGrpc.TracOrchestratorApiImplBase {

    @Override
    public void validateJob(JobRequest request, StreamObserver<JobStatus> responseObserver) {
        super.validateJob(request, responseObserver);
    }

    @Override
    public void executeJob(JobRequest request, StreamObserver<JobStatus> responseObserver) {
        super.executeJob(request, responseObserver);
    }

    @Override
    public void cancelJob(TagSelector request, StreamObserver<JobStatus> responseObserver) {
        super.cancelJob(request, responseObserver);
    }

    @Override
    public void followJob(TagSelector request, StreamObserver<JobStatus> responseObserver) {
        super.followJob(request, responseObserver);
    }
}
