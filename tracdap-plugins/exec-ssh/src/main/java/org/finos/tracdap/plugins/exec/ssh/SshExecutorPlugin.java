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

package org.finos.tracdap.plugins.exec.ssh;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.exec.IBatchExecutor;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;

import java.util.List;
import java.util.Properties;


public class SshExecutorPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "SSH_EXECUTOR";

    private static final String SSH_EXECUTOR_NAME = "SSH_EXECUTOR";

    private static final List<PluginServiceInfo> psi = List.of(
            new PluginServiceInfo(IBatchExecutor.class, SSH_EXECUTOR_NAME, List.of("ssh")));


    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return psi;
    }

    @Override @SuppressWarnings("unchecked")
    public <T> T createService(String serviceName, Properties properties, ConfigManager configManager) {

        if (serviceName.equals(SSH_EXECUTOR_NAME))
            return (T) new SshExecutor(properties);

        var message = String.format("Plugin [%s] does not support the service [%s]", pluginName(), serviceName);
        throw new EPluginNotAvailable(message);
    }
}
