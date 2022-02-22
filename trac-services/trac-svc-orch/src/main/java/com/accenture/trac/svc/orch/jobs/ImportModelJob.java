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
import com.accenture.trac.metadata.*;

import java.util.List;
import java.util.Map;

import static com.accenture.trac.common.metadata.MetadataCodec.encodeValue;
import static com.accenture.trac.common.metadata.MetadataConstants.*;


public class ImportModelJob implements IJobLogic {

    @Override
    public Map<String, TagSelector> requiredMetadata(JobDefinition job) {

        // No extra metadata needed for an import_model job

        return Map.of();
    }

    @Override
    public JobConfig buildJobConfig(TagHeader jobId, JobDefinition job) {

        if (job.getJobType() != JobType.IMPORT_MODEL)
            throw new EUnexpected();

        return JobConfig.newBuilder()
                .setJobId(jobId)
                .setJob(job)
                .build();
    }

    @Override
    public List<MetadataWriteRequest> buildResultMetadata(String tenant, JobRequest request, JobResult result) {

        var modelObjMaybe = result.getObjectsMap().values().stream().findFirst();

        if (modelObjMaybe.isEmpty())
            throw new EUnexpected();

        var modelObj = modelObjMaybe.get();
        var modelDef = modelObj.getModel();

        var controlledAttrs = List.of(

                TagUpdate.newBuilder()
                        .setAttrName(TRAC_MODEL_LANGUAGE)
                        .setValue(encodeValue(modelDef.getLanguage()))
                        .build(),

                TagUpdate.newBuilder()
                        .setAttrName(TRAC_MODEL_REPOSITORY)
                        .setValue(encodeValue(modelDef.getRepository()))
                        .build(),

                TagUpdate.newBuilder()
                        .setAttrName(TRAC_MODEL_PATH)
                        .setValue(encodeValue(modelDef.getPath()))
                        .build(),

                TagUpdate.newBuilder()
                        .setAttrName(TRAC_MODEL_ENTRY_POINT)
                        .setValue(encodeValue(modelDef.getEntryPoint()))
                        .build(),

                TagUpdate.newBuilder()
                        .setAttrName(TRAC_MODEL_VERSION)
                        .setValue(encodeValue(modelDef.getVersion()))
                        .build());

        var suppliedAttrs = request.getJob()
                .getImportModel()
                .getModelAttrsList();

        var modelReq = MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(modelObj)
                .addAllTagUpdates(controlledAttrs)
                .addAllTagUpdates(suppliedAttrs)
                .build();

        return List.of(modelReq);
    }
}
