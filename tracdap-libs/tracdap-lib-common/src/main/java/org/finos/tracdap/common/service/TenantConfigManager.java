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

import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.config.TenantConfigMap;


public class TenantConfigManager extends TenantStateManager<TenantConfigState> {

    public TenantConfigManager(
            TenantConfigMap staticConfig, ConfigManager configManager,
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient,
            GrpcConcern clientConcern) {

        super(staticConfig, configManager, metaClient, clientConcern);
    }

    public TenantConfig getTenantConfig(String tenantCode) {
        return getTenant(tenantCode).getLiveConfig();
    }

    @Override
    protected TenantConfigState initTenant(String tenantCode, TenantConfig liveConfig) {
        return new TenantConfigState(liveConfig);
    }
}
