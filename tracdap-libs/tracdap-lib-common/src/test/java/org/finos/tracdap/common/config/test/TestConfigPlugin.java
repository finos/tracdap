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

package org.finos.tracdap.common.config.test;

import org.finos.tracdap.common.config.IConfigLoader;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;


public class TestConfigPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "TEST_CONFIG";
    private static final String SERVICE_NAME = "TEST_CONFIG";

    private static final PluginServiceInfo serviceInfo = new PluginServiceInfo(
            PLUGIN_NAME, IConfigLoader.class,
            SERVICE_NAME, List.of("test"));

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return List.of(serviceInfo);
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T createService(String serviceName, Properties properties) {

        if (tempDir == null || !Files.exists(tempDir))
            throw new RuntimeException("Temp dir must be set for TestConfigPlugin");

        if (serviceName.equals(SERVICE_NAME))
            return (T) new TestConfigLoader(tempDir);

        throw new EUnexpected();
    }

    public static void setCurrentTempDir(Path tempDir) {
        TestConfigPlugin.tempDir = tempDir;
    }

    private static Path tempDir;
}
