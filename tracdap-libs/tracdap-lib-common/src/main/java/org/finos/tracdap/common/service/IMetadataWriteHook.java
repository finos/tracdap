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
 * Extension point for metadata object writes, called by the metadata service for every object it creates or
 * updates, on the public and internal APIs, before anything in the request is saved. Registered as a singleton in the
 * {@link org.finos.tracdap.common.plugin.PluginRegistry} during extension startup.
 *
 * <p>The orchestrator's own writes (job definitions and job results) are included. A refusal of a job result fails
 * a job that has already run.</p>
 */
public interface IMetadataWriteHook {

    /**
     * Rejects an object write by throwing EInputValidation, EVersionValidation or EConsistencyValidation.
     */
    default void validateObject(ObjectWrite write) {
    }
}
