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

package org.finos.tracdap.svc.orch.jobs;

import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.TagUpdateList;
import org.finos.tracdap.metadata.*;

import java.util.List;
import java.util.Map;

import static org.finos.tracdap.common.metadata.MetadataCodec.encodeValue;
import static org.finos.tracdap.common.metadata.MetadataConstants.*;


public class ImportModelJob implements IJobLogic {

    @Override
    public List<TagSelector> requiredMetadata(JobDefinition job) {

        // No extra metadata needed for an import_model job

        return List.of();
    }

    @Override
    public List<TagSelector> requiredMetadata(
            Map<String, ObjectDefinition> newResources) {

        // No extra metadata needed for an import_model job

        return List.of();
    }

    @Override
    public Map<String, MetadataWriteRequest> newResultIds(
            String tenant, JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        return Map.of();  // not currently used
    }

    @Override
    public Map<String, TagHeader> priorResultIds(
            JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        // Model updates not supported yet

        return Map.of();
    }

    @Override
    public JobDefinition setResultIds(
            JobDefinition job,
            Map<String, TagHeader> resultMapping,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping) {

        return job;
    }

    @Override
    public List<MetadataWriteRequest> buildResultMetadata(String tenant, JobConfig jobConfig, JobResult jobResult) {

        var modelKeyMaybe = jobResult.getResultsMap().keySet().stream().findFirst();

        if (modelKeyMaybe.isEmpty())
            throw new EUnexpected();

        var modelKey = modelKeyMaybe.get();
        var modelObj = jobResult.getResultsOrThrow(modelKey);
        var modelDef = modelObj.getModel();

        var modelAttrs = jobResult
                .getAttrsOrDefault(modelKey, TagUpdateList.newBuilder().build())
                .getAttrsList();

        var suppliedAttrs = jobConfig.getJob()
                .getImportModel()
                .getModelAttrsList();

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

        var modelReq = MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(modelObj)
                .addAllTagUpdates(modelAttrs)
                .addAllTagUpdates(suppliedAttrs)
                .addAllTagUpdates(controlledAttrs)
                .build();

        return List.of(modelReq);
    }
}
