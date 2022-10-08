/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.exec.local;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;
import org.finos.tracdap.common.exec.IBatchExecutor;

import java.util.List;
import java.util.Properties;

public class LocalExecutorPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "LOCAL_EXECUTOR";

    private static final String LOCAL_BATCH_EXECUTOR_NAME = "LOCAL_BATCH";

    private static final List<PluginServiceInfo> psi = List.of(
            new PluginServiceInfo(IBatchExecutor.class, LOCAL_BATCH_EXECUTOR_NAME, List.of("LOCAL")));


    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return psi;
    }

    @Override @SuppressWarnings("unchecked")
    public <T> T createService(String serviceName, Properties properties) {

        switch (serviceName) {

            case LOCAL_BATCH_EXECUTOR_NAME: return (T) new LocalBatchExecutor(properties);

            default:
                throw new EUnexpected();
        }
    }
}
