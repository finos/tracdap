/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.orch.process;

import org.finos.tracdap.svc.orch.cache.JobState;


public class JobProcessor {


    // Metadata actions

    public JobState prepareMetadata(String jobKey, JobState jobState) {

        return jobState;
    }

    public JobState recordMetadata(String jobKey, JobState jobState) {

        return jobState;
    }

    public JobState recordUpdate(String jobKey, JobState jobState) {

        return jobState;
    }

    public JobState processResult(String jobKey, JobState jobState) {

        return jobState;
    }

    public JobState recordResult(String jobKey, JobState jobState) {

        return jobState;
    }


    // Executor actions

    public JobState submitJob(String jobKey, JobState jobState) {

        // Use a submission ID to avoid clash on repeat

        return jobState;
    }

    public JobState fetchJobResult(String jobKey, JobState jobState) {

        return jobState;
    }

    public JobState cleanUpJob(String jobKey, JobState jobState) {

        return jobState;
    }
}
