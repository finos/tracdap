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

import org.finos.tracdap.api.ConfigReadResponse;
import org.finos.tracdap.api.PlatformConfigReadResponse;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.PlatformConfigEntry;

import java.util.List;

/**
 * Read access to tenant and platform config for {@link IConfigWriteHook} implementations.
 * All reads return the latest version of each entry. Deleted tenant entries have no definition.
 */
public interface IConfigReader {

    PlatformConfigReadResponse readPlatformConfig(String configClass, String configKey, boolean includeDeleted);

    List<PlatformConfigEntry> listPlatformConfig(String configClass, boolean includeDeleted);

    List<PlatformConfigReadResponse> readPlatformConfigBatch(List<PlatformConfigEntry> entries, boolean includeDeleted);

    ConfigReadResponse readConfig(String tenant, String configClass, String configKey, boolean includeDeleted);

    List<ConfigEntry> listConfig(String tenant, String configClass, boolean includeDeleted);

    List<ConfigReadResponse> readConfigBatch(String tenant, List<ConfigEntry> entries, boolean includeDeleted);
}
