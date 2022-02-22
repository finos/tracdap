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

package com.accenture.trac.svc.orch.jobs;


import com.accenture.trac.api.JobRequest;
import com.accenture.trac.api.MetadataWriteRequest;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.config.JobConfig;
import com.accenture.trac.config.JobResult;
import com.accenture.trac.metadata.JobDefinition;
import com.accenture.trac.metadata.JobType;
import com.accenture.trac.metadata.TagHeader;
import com.accenture.trac.metadata.TagSelector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RunModelJob implements IJobLogic {

    private static final String TRAC_MODEL_RESOURCE_KEY = "trac_model";

    @Override
    public Map<String, TagSelector> requiredMetadata(JobDefinition job) {

        if (job.getJobType() != JobType.RUN_MODEL)
            throw new EUnexpected();

        var runModel = job.getRunModel();

        var resources = new HashMap<String, TagSelector>(runModel.getInputsCount() + 1);
        resources.putAll(runModel.getInputsMap());
        resources.put(TRAC_MODEL_RESOURCE_KEY, runModel.getModel());

        return resources;
    }

    @Override
    public JobConfig buildJobConfig(TagHeader jobId, JobDefinition job) {

        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public List<MetadataWriteRequest> buildResultMetadata(String tenant, JobRequest request, JobResult result) {

        return List.of();
    }
}
