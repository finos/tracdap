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

import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.ObjectDefinition;

public abstract class TenantState {

    protected abstract boolean configIsRelevant(ConfigEntry entry);

    protected abstract void tenantPropertiesUpdated(TenantConfig tenantConfig);

    protected abstract void configCreated(ConfigEntry entry, ObjectDefinition definition);

    protected abstract void configUpdated(ConfigEntry entry, ObjectDefinition definition);

    protected abstract void configDeleted(ConfigEntry entry);

    protected abstract void shutdown();
}
