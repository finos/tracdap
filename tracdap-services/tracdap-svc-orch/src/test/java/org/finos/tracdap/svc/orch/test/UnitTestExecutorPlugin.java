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

package org.finos.tracdap.svc.orch.test;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exec.IBatchExecutor;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;

import java.util.List;
import java.util.Properties;


public class UnitTestExecutorPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "UNIT_TEST_EXECUTOR";
    private static final String EXECUTOR_NAME = "UNIT_TEST_EXECUTOR";

    private static final List<PluginServiceInfo> psi = List.of(
            new PluginServiceInfo(IBatchExecutor.class, EXECUTOR_NAME, List.of("UNIT_TEST")));


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

        if (serviceName.equals(EXECUTOR_NAME))
            return (T) new UnitTestExecutor();

        return super.createService(serviceName, properties, configManager);
    }

}
