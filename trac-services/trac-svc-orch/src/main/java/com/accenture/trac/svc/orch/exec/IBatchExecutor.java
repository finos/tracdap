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

package com.accenture.trac.svc.orch.exec;

import java.util.Map;
import java.util.Set;


public interface IBatchExecutor {

    // Interface for running batch jobs, i.e. a job that runs using one-shot using a one-shot process

    void executorStatus();

    JobExecState createBatchSandbox(String jobKey);

    JobExecState writeTextConfig(String jobKey, JobExecState jobState, Map<String, String> configFiles);

    JobExecState writeBinaryConfig(String jobKey, JobExecState jobState, Map<String, byte[]> configFiles);

    JobExecState startBatch(String jobKey, JobExecState jobState, Set<String> configFiles);

    void getBatchStatus(String jobKey, JobExecState jobState);

    void readBatchResult(String jobKey, JobExecState jobState);

    JobExecState cancelBatch(String jobKey, JobExecState jobState);

    JobExecState cleanUpBatch(String jobKey, JobExecState jobState);

    void pollAllBatches();
}
