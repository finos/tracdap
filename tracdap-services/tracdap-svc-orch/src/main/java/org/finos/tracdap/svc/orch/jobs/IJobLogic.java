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

import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.api.internal.RuntimeJobResult;
import org.finos.tracdap.common.config.IDynamicResources;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.metadata.*;

import java.util.List;
import java.util.Map;


public interface IJobLogic {

    JobDefinition applyTransform(JobDefinition job, MetadataBundle metadata, IDynamicResources resources);

    MetadataBundle applyMetadataTransform(JobDefinition job, MetadataBundle metadata, IDynamicResources resources);

    List<TagSelector> requiredMetadata(JobDefinition job);

    List<TagSelector> requiredMetadata(Map<String, ObjectDefinition> newResources);

    Map<String, TagHeader> priorResultIds(
            JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping);

    Map<String, MetadataWriteRequest> newResultIds(
            String tenant, JobDefinition job,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping);

    JobDefinition setResultIds(
            JobDefinition job, Map<String, TagHeader> resultMapping,
            Map<String, ObjectDefinition> resources,
            Map<String, TagHeader> resourceMapping);

    List<MetadataWriteRequest> buildResultMetadata(String tenant, JobConfig jobConfig, RuntimeJobResult jobResult);
}
