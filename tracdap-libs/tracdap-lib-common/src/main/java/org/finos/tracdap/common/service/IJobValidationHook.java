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

package org.finos.tracdap.common.service;

/**
 * Extension point for job validation, called by the orchestrator for submitJob and validateJob, after consistency
 * validation and before the job is saved. Registered as a singleton in the
 * {@link org.finos.tracdap.common.plugin.PluginRegistry} during extension startup.
 */
public interface IJobValidationHook {

    /**
     * Rejects a job by throwing EInputValidation or EConsistencyValidation.
     */
    default void validate(JobValidation validation) {
    }
}
