/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.exec;

import org.finos.tracdap.api.internal.RuntimeJobResult;
import org.finos.tracdap.api.internal.RuntimeJobStatus;
import org.finos.tracdap.common.grpc.GrpcChannelFactory;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.RuntimeConfig;
import org.finos.tracdap.metadata.TagHeader;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Flow;


public interface IJobExecutor<TState extends Serializable> {

    void start(GrpcChannelFactory channelFactory);
    void stop();

    Class<TState> stateClass();

    TState submitJob();
    TState submitOneshotJob(TagHeader jobId, JobConfig jobConfig, RuntimeConfig runtimeConfig);
    TState submitExternalJob();
    TState cancelJob(TState jobState);
    void deleteJob(TState jobState);

    List<RuntimeJobStatus> listJobs();
    RuntimeJobStatus getJobStatus(TState jobState);
    Flow.Publisher<RuntimeJobStatus> followJobStatus(TState jobState);

    RuntimeJobResult getJobResult(TState jobState);
}
