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
import org.finos.tracdap.common.exception.EExecutorValidation;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.metadata.*;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.finos.tracdap.common.metadata.MetadataCodec.encodeValue;
import static org.finos.tracdap.common.metadata.MetadataConstants.*;


public class ImportModelJob implements IJobLogic {

    @Override
    public JobDefinition applyTransform(JobDefinition job, MetadataBundle metadata, PlatformConfig platformConfig) {

        // Fill in package and packageGroup properties for models using Git repos

        // TODO: This is very specialized logic
        // The intent is to set TRAC_MODEL_PACKAGE and TRAC_MODEL_PACKAGE_GROUP for models stored in Git
        // A more explicit solution would be better, e.g. explicit settings in the repo config

        if (!job.getImportModel().getPackage().isBlank())
            return job;

        var repoKey = job.getImportModel().getRepository();

        if (!platformConfig.containsRepositories(repoKey)) {
            var message = String.format("Import job refers to model repository [%s] which is not defined in the platform configuration", repoKey);
            throw new EExecutorValidation(message);
        }

        var repoConfig = platformConfig.getRepositoriesOrThrow(repoKey);

        if (!repoConfig.getProtocol().equalsIgnoreCase("git"))
            return job;

        if (!repoConfig.containsProperties("repoUrl")){
            var message = String.format("Git configuration for [%s] is missing required property [repoUrl]", repoKey);
            throw new EExecutorValidation(message);
        }

        var repoUrlProp = repoConfig.getPropertiesOrThrow("repoUrl");
        var repoUrl = URI.create(repoUrlProp);
        var repoPathSegments = repoUrl.getPath().split("[/\\\\]");

        var importDef = job.getImportModel().toBuilder();

        if (repoPathSegments.length >= 1)
            importDef.setPackage(repoPathSegments[repoPathSegments.length - 1]);

        if (repoPathSegments.length >= 2)
            importDef.setPackageGroup(repoPathSegments[repoPathSegments.length - 2]);

        return job.toBuilder()
                .setImportModel(importDef)
                .build();
    }

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

        var modelReq = MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(ObjectType.MODEL)
                .setDefinition(modelObj);

        // Add attrs defined in the model code
        for (var staticAttr : modelDef.getStaticAttributesMap().entrySet()) {

            modelReq.addTagUpdates(TagUpdate.newBuilder()
                    .setOperation(TagOperation.CREATE_OR_REPLACE_ATTR)
                    .setAttrName(staticAttr.getKey())
                    .setValue(staticAttr.getValue()));
        }

        // Add attrs defined in the job
        var suppliedAttrs = jobConfig.getJob().getImportModel().getModelAttrsList();
        modelReq.addAllTagUpdates(suppliedAttrs);

        // Add controlled attrs for models

        modelReq.addTagUpdates(TagUpdate.newBuilder()
                .setAttrName(TRAC_MODEL_LANGUAGE)
                .setValue(encodeValue(modelDef.getLanguage())));

        modelReq.addTagUpdates(TagUpdate.newBuilder()
                .setAttrName(TRAC_MODEL_REPOSITORY)
                .setValue(encodeValue(modelDef.getRepository())));

        if (modelDef.hasPackageGroup()) {

            modelReq.addTagUpdates(TagUpdate.newBuilder()
                    .setAttrName(TRAC_MODEL_PACKAGE_GROUP)
                    .setValue(encodeValue(modelDef.getPackageGroup())));
        }

        modelReq.addTagUpdates(TagUpdate.newBuilder()
                .setAttrName(TRAC_MODEL_PACKAGE)
                .setValue(encodeValue(modelDef.getPackage())));

        modelReq.addTagUpdates(TagUpdate.newBuilder()
                .setAttrName(TRAC_MODEL_VERSION)
                .setValue(encodeValue(modelDef.getVersion())));

        modelReq.addTagUpdates(TagUpdate.newBuilder()
                .setAttrName(TRAC_MODEL_ENTRY_POINT)
                .setValue(encodeValue(modelDef.getEntryPoint())));

        if (modelDef.hasPath()) {

            modelReq.addTagUpdates(TagUpdate.newBuilder()
                    .setAttrName(TRAC_MODEL_PATH)
                    .setValue(encodeValue(modelDef.getPath())));
        }

        return List.of(modelReq.build());
    }
}
