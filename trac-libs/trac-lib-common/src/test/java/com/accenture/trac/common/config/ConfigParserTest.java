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

package com.accenture.trac.common.config;

import com.accenture.trac.common.config.test.TestConfigPlugin;
import com.accenture.trac.common.plugin.PluginManager;
import com.accenture.trac.common.startup.Startup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Map;

public class ConfigParserTest {

    private static final String TEST_CONFIG_1 = "/config_mgr_test/root.yaml";

    @BeforeAll
    public static void testLoader() {

        // The test plugin for config loaders is on the class path and will error if not initialized
        TestConfigPlugin.setCurrentTempDir(Paths.get("."));
    }

    @Test
    public void testYaml_basicLoadOk() throws Exception {

        var configFileUrl = getClass().getResource(TEST_CONFIG_1);
        Assertions.assertNotNull(configFileUrl);

        var configFilePath = Paths.get(".")
                .toAbsolutePath()
                .relativize(Paths.get(configFileUrl.toURI()))
                .toString()
                .replace("\\", "/");

        var configPlugins = new PluginManager();
        configPlugins.initConfigPlugins();

        var args = new StandardArgs(Paths.get("."), configFilePath, "password");
        var config = new ConfigManager(args, configPlugins);
        var configText = config.loadRootConfigAsText();

        var configRoot = ConfigParser.parseStructuredConfig(configText, ConfigFormat.YAML, RootConfig.class);
        Assertions.assertNotNull(config);

        var sample = configRoot.root;
        Assertions.assertNotNull(sample);
        Assertions.assertEquals("hello", sample.getProp1());
        Assertions.assertEquals(42, sample.getProp2());
    }


    public static class RootConfig {

        public Map<String, Object> config;
        public SampleConfig root;
    }


    public static class SampleConfig {

        private String prop1;
        private int prop2;

        public String getProp1() {
            return prop1;
        }

        public void setProp1(String prop1) {
            this.prop1 = prop1;
        }

        public int getProp2() {
            return prop2;
        }

        public void setProp2(int prop2) {
            this.prop2 = prop2;
        }
    }
}
