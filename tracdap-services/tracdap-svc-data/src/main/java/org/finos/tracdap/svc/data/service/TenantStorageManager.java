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

package org.finos.tracdap.svc.data.service;

import io.netty.channel.EventLoopGroup;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.TenantStateManager;
import org.finos.tracdap.common.storage.StorageManager;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.config.TenantConfigMap;

public class TenantStorageManager extends TenantStateManager<TenantStorageState> {

    private final PluginManager pluginManager;
    private final ConfigManager configManager;
    private final ICodecManager codecManager;
    private final EventLoopGroup eventLoopGroup;

    public TenantStorageManager(
            PluginManager pluginManager, ConfigManager configManager, ICodecManager codecManager,
            EventLoopGroup eventLoopGroup, InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metaClient,
            TenantConfigMap staticConfigMap)  {

        super(staticConfigMap, configManager, metaClient);

        this.pluginManager = pluginManager;
        this.configManager = configManager;
        this.codecManager = codecManager;
        this.eventLoopGroup = eventLoopGroup;
    }

    @Override
    protected TenantStorageState initTenant(String tenantCode, TenantConfig initialConfig) {
        return new TenantStorageState(pluginManager, configManager, codecManager, eventLoopGroup, initialConfig);
    }

    public StorageManager getTenantStorage(String tenantCode) {
        return getTenant(tenantCode).getStorageManager();
    }
}
