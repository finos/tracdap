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

import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.ResourceBundle;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.metadata.*;

import java.util.List;
import java.util.Map;


/**
 * Interface to provide specialized job logic for each job type
 */
public interface IJobLogic {

    /**
     * Provide a list of metadata objects required for the job to execute
     *
     * <p>Only objects directly referenced in the job definition should be returned by this method.
     * The orchestrator framework will resolve secondary dependencies. The supplied job definition
     * must match the job type of the job logic class.
     *
     * @param job The job definition to check metadata for
     * @return A list of selectors for the required metadata objects.
     */
    List<TagSelector> requiredMetadata(JobDefinition job);

    /**
     * Provide a list of resource keys required for the job to execute
     *
     * @param job The job definition to check metadata for
     * @param metadata A bundle containing the metadata referenced in the job definition
     * @return A list of resource keys for the required runtime resources.
     */
    List<String> requiredResources(JobDefinition job, MetadataBundle metadata);

    /**
     * Apply transformations to the job definition before sending it to the executor.
     *
     * @param job The original job definition, as supplied by the client
     * @param metadata A bundle containing the metadata referenced in the job definition
     * @param resources A bundle containing the resources required to run this job
     * @return The transformed job definition, which will be sent to the runtime for execution
     */
    JobDefinition applyJobTransform(JobDefinition job, MetadataBundle metadata, ResourceBundle resources);

    /**
     * Apply transformations to the metadata bundle for a job before sending it to the executor.
     *
     * @param job The original job definition being considered
     * @param metadata A bundle containing the original metadata loaded from the metadata store
     * @param resources A bundle containing the resources required to run this job
     * @return The transformed metadata bundle, which will be sent to the runtime for execution
     */
    MetadataBundle applyMetadataTransform(JobDefinition job, MetadataBundle metadata, ResourceBundle resources);

    /**
     * Provide a count of expected outputs for each object type (used to preallocate object IDs)
     *
     * @param job The job definition being considered
     * @param metadata A bundle containing the metadata referenced in the job definition
     * @return Map describing the expected number of outputs for each object type
     */
    Map<ObjectType, Integer> expectedOutputs(JobDefinition job, MetadataBundle metadata);

    /**
     * Perform post-processing on the job result received from the model runtime.
     *
     * <p>Post-processing should include filtering and validating the outputs (i.e. only
     * expected, consistent outputs are retained). Job logic is also responsible for adding
     * any specialised attributes to the output objects, e.g. for models the trac_model_xxx
     * attributes which are taken from the model definition.
     *
     * @param jobConfig The original job config sent for execution
     * @param jobResult The raw result received from the model runtime
     * @param resultIds Map of un-versioned IDs to the object IDs received from the runtime
     * @return The processed result that will be used for saving output objects
     */
    JobResult processResult(JobConfig jobConfig, JobResult jobResult, Map<String, TagHeader> resultIds);
}
