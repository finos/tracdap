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

package org.finos.tracdap.common.plugin;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.middleware.CommonConcerns;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.middleware.NettyConcern;

import com.google.protobuf.Descriptors;

import java.util.List;


public interface ITracExtension {

    String extensionName();

    default List<Descriptors.FileDescriptor> configExtensions() {
        return List.of();
    }

    default List<PluginType> pluginTypes() {
        return List.of();
    }

    @SuppressWarnings("unused")
    default CommonConcerns<GrpcConcern> addServiceConcerns(
            CommonConcerns<GrpcConcern> serviceConcerns,
            ConfigManager configManager,
            String serviceKey) {

        return serviceConcerns;
    }

    @SuppressWarnings("unused")
    default CommonConcerns<NettyConcern> addGatewayConcerns(
            CommonConcerns<NettyConcern> gatewayConcerns,
            ConfigManager configManager) {

        return gatewayConcerns;
    }

    @SuppressWarnings("unused")
    default void runStartupLogic(PluginRegistry registry) {

    }
}
