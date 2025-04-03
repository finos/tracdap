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

import java.util.List;


public class PluginType {

    public static final List<PluginType> CORE_PLUGIN_TYPES = List.of(
            new PluginType("org.finos.tracdap.common.config.IConfigLoader", PluginServiceInfo.CONFIG_SERVICE_TYPE, true),
            new PluginType("org.finos.tracdap.common.config.ISecretService", PluginServiceInfo.SECRETS_SERVICE_TYPE, true),
            new PluginType("org.finos.tracdap.common.metadata.dal.IMetadataDal", PluginServiceInfo.METADATA_DAL_SERVICE_TYPE, false),
            new PluginType("org.finos.tracdap.common.storage.IFileStorage", PluginServiceInfo.FILE_STORAGE_SERVICE_TYPE, false),
            new PluginType("org.finos.tracdap.common.storage.IDataStorage", PluginServiceInfo.DATA_STORAGE_SERVICE_TYPE, false),
            new PluginType("org.finos.tracdap.common.codec.ICodec", PluginServiceInfo.FORMAT_SERVICE_TYPE, false),
            new PluginType("org.finos.tracdap.common.exec.IBatchExecutor", PluginServiceInfo.EXECUTION_SERVICE_TYPE, false),
            new PluginType("org.finos.tracdap.common.cache.IJobCacheManager", PluginServiceInfo.JOB_CACHE_SERVICE_TYPE, false));


    private final String serviceClassName;
    private final String serviceType;
    private final boolean isConfigPlugin;

    public static PluginType forClass(Class<?> pluginClass, String serviceType) {
        return new PluginType(pluginClass.getName(), serviceType, false);
    }

    public PluginType(String serviceClassName, String serviceType, boolean isConfigPlugin) {
        this.serviceClassName = serviceClassName;
        this.serviceType = serviceType;
        this.isConfigPlugin = isConfigPlugin;
    }

    public String serviceClassName() {
        return serviceClassName;
    }

    public String serviceType() {
        return serviceType;
    }

    public boolean isConfigPlugin() {
        return isConfigPlugin;
    }
}
