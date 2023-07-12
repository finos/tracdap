/*
 * Copyright 2023 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.orch;

import org.finos.tracdap.common.cache.IJobCache;
import org.finos.tracdap.common.cache.local.LocalJobCache;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.exec.IBatchExecutor;
import org.finos.tracdap.common.exec.local.LocalBatchExecutor;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;

import java.util.List;
import java.util.Properties;


public class CoreOrchestratorPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "CORE_ORCHESTRATOR";

    private static final String LOCAL_EXECUTOR_NAME = "LOCAL_EXECUTOR";
    private static final String LOCAL_JOB_CACHE_NAME = "LOCAL_JOB_CACHE";

    private static final List<PluginServiceInfo> psi = List.of(
            new PluginServiceInfo(IBatchExecutor.class, LOCAL_EXECUTOR_NAME, List.of("LOCAL")),
            new PluginServiceInfo(IJobCache.class, LOCAL_JOB_CACHE_NAME, List.of("LOCAL")));


    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return psi;
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T createService(String serviceName, Properties properties, ConfigManager configManager) {

        switch (serviceName) {

            case LOCAL_EXECUTOR_NAME: return (T) new LocalBatchExecutor(properties);
            case LOCAL_JOB_CACHE_NAME: return (T) new LocalJobCache<>();

            default:

                var message = String.format(
                        "Plugin [%s] does not support the service [%s]",
                        pluginName(), serviceName);

                throw new EPluginNotAvailable(message);
        }
    }
}
