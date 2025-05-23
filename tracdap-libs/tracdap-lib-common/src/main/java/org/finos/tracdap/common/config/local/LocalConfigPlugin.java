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

package org.finos.tracdap.common.config.local;

import org.finos.tracdap.common.config.IConfigLoader;
import org.finos.tracdap.common.config.ISecretService;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;

import java.util.List;
import java.util.Properties;


public class LocalConfigPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "LOCAL_CONFIG";
    private static final String FILE_LOADER = "FILE_LOADER";
    private static final String JKS_SECRET_LOADER = "JKS_SECRET_LOADER";

    private static final List<PluginServiceInfo> serviceInfo = List.of(
            new PluginServiceInfo(IConfigLoader.class, FILE_LOADER, List.of("LOCAL", "file")),
            new PluginServiceInfo(ISecretService.class, JKS_SECRET_LOADER, List.of("PKCS12", "JCEKS", "JKS")));


    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return serviceInfo;
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T createConfigService(String serviceName, Properties properties) {

        if (serviceName.equals(FILE_LOADER))
            return (T) new LocalConfigLoader();

        if (serviceName.equals(JKS_SECRET_LOADER))
            return (T) new JksSecretService(properties);

        var message = String.format("Plugin [%s] does not support the service [%s]", pluginName(), serviceName);
        throw new EPluginNotAvailable(message);
    }
}
