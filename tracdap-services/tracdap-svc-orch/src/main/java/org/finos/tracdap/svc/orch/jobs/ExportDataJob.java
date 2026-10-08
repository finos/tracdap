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

package org.finos.tracdap.svc.orch.jobs;

import org.finos.tracdap.common.exception.EJobResult;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.ResourceBundle;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.metadata.*;

import java.util.*;


public class ExportDataJob extends RunModelOrFlow implements IJobLogic {

    @Override
    public List<TagSelector> requiredMetadata(JobDefinition job) {

        if (job.getJobType() != JobType.EXPORT_DATA)
            throw new EUnexpected();

        var datasets = new ArrayList<TagSelector>();

        for (var placement : job.getExportData().getPlacementsMap().values())
            datasets.add(placement.getDataId());

        return datasets;
    }

    @Override
    public List<String> requiredResources(JobDefinition job, MetadataBundle metadata) {

        var resources = new HashSet<String>();

        addRequiredStorage(metadata, resources);

        for (var placement : job.getExportData().getPlacementsMap().values())
            resources.add(placement.getLocation().getStorageKey());

        return new ArrayList<>(resources);
    }

    @Override
    public JobDefinition applyJobTransform(JobDefinition job, MetadataBundle metadata, ResourceBundle resources) {

        var exportData = job.getExportData().toBuilder();

        for (var placement : job.getExportData().getPlacementsMap().entrySet()) {

            var location = ExternalLocationDetails.recordLocationDetails(placement.getValue().getLocation(), resources);
            var recorded = placement.getValue().toBuilder().setLocation(location).build();

            exportData.putPlacements(placement.getKey(), recorded);
        }

        return job.toBuilder()
                .setExportData(exportData)
                .build();
    }

    @Override
    public MetadataBundle applyMetadataTransform(JobDefinition job, MetadataBundle metadata, ResourceBundle resources) {

        return metadata;
    }

    @Override
    public Map<ObjectType, Integer> expectedOutputs(JobDefinition job, MetadataBundle metadata) {

        return Map.of();
    }

    @Override
    public JobResult processResult(JobConfig jobConfig, JobResult jobResult, Map<String, TagHeader> resultIds) {

        var placementRecords = jobResult.getResult().getPlacementsMap();

        for (var placementName : jobConfig.getJob().getExportData().getPlacementsMap().keySet()) {
            if (!placementRecords.containsKey(placementName))
                throw new EJobResult(String.format("Missing record for placement [%s]", placementName));
        }

        return JobResult.newBuilder().build();
    }
}
