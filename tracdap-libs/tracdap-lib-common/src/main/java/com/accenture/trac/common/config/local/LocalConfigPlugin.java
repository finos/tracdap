/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.config.local;

import com.accenture.trac.common.config.IConfigLoader;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.plugin.PluginServiceInfo;
import com.accenture.trac.common.plugin.TracPlugin;

import java.util.List;
import java.util.Properties;


public class LocalConfigPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "LOCAL_CONFIG";
    private static final String SERVICE_NAME = "LOCAL_CONFIG";

    private static final PluginServiceInfo psi = new PluginServiceInfo(
            PLUGIN_NAME, IConfigLoader.class,
            SERVICE_NAME, List.of("file"));

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return List.of(psi);
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T createService(String serviceName, Properties properties) {

        if (serviceName.equals(SERVICE_NAME))
            return (T) new LocalConfigLoader();

        throw new EUnexpected();
    }
}
