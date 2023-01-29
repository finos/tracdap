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

package org.finos.tracdap.common.config;

import org.finos.tracdap.common.config.test.TestConfigPlugin;
import org.finos.tracdap.common.exception.EConfigParse;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.common.util.ResourceHelpers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Random;


public class ConfigParserTest {

    private static final String SAMPLE_YAML_CONFIG = "/config_mgr_test/sample-config.yaml";
    private static final String SAMPLE_JSON_CONFIG = "/config_mgr_test/sample-config.json";

    private static final String UNKNOWN_ITEM_YAML_CONFIG = "/config_mgr_test/unknown-item.yaml";
    private static final String UNKNOWN_ITEM_JSON_CONFIG = "/config_mgr_test/unknown-item.json";

    @BeforeAll
    public static void testLoader() {

        // The test plugin for config loaders is on the class path and will error if not initialized
        TestConfigPlugin.setCurrentTempDir(Paths.get("."));
    }

    @Test
    public void testJson_basicLoadOk() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(SAMPLE_JSON_CONFIG);
        var configObject = ConfigParser.parseConfig(configBytes, ConfigFormat.JSON, PlatformConfig.class);

        Assertions.assertNotNull(configObject);
        Assertions.assertInstanceOf(PlatformConfig.class, configObject);

        var metaDbConfig = configObject.getMetadata().getDatabase();

        Assertions.assertInstanceOf(PluginConfig.class, metaDbConfig);
        Assertions.assertEquals("JDBC", metaDbConfig.getProtocol());
    }

    @Test
    public void testJson_garbled() {

        var configBytes = new byte[1000];
        new Random().nextBytes(configBytes);

        Assertions.assertThrows(EConfigParse.class, () ->
                ConfigParser.parseConfig(configBytes, ConfigFormat.JSON, PlatformConfig.class));
    }

    @Test
    public void testJson_unknownItem() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(UNKNOWN_ITEM_JSON_CONFIG);

        Assertions.assertThrows(EConfigParse.class, () ->
                ConfigParser.parseConfig(configBytes, ConfigFormat.JSON, PlatformConfig.class));
    }

    @Test
    public void testJson_lenient() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(UNKNOWN_ITEM_JSON_CONFIG);

        Assertions.assertDoesNotThrow(() ->
                ConfigParser.parseConfig(configBytes, ConfigFormat.JSON, PlatformConfig.class, true));
    }

    @Test
    public void testYaml_basicLoadOk() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(SAMPLE_YAML_CONFIG);
        var configObject = ConfigParser.parseConfig(configBytes, ConfigFormat.YAML, PlatformConfig.class);

        Assertions.assertNotNull(configObject);
        Assertions.assertInstanceOf(PlatformConfig.class, configObject);

        var metaDbConfig = configObject.getMetadata().getDatabase();

        Assertions.assertInstanceOf(PluginConfig.class, metaDbConfig);
        Assertions.assertEquals("JDBC", metaDbConfig.getProtocol());
    }

    @Test
    public void testYaml_garbled() {

        var configBytes = new byte[1000];
        new Random().nextBytes(configBytes);

        Assertions.assertThrows(EConfigParse.class, () ->
                ConfigParser.parseConfig(configBytes, ConfigFormat.YAML, PlatformConfig.class));
    }

    @Test
    public void testYaml_unknownItem() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(UNKNOWN_ITEM_YAML_CONFIG);

        Assertions.assertThrows(EConfigParse.class, () ->
                ConfigParser.parseConfig(configBytes, ConfigFormat.YAML, PlatformConfig.class));
    }

    @Test
    public void testYaml_lenient() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(UNKNOWN_ITEM_YAML_CONFIG);

        Assertions.assertDoesNotThrow(() ->
                ConfigParser.parseConfig(configBytes, ConfigFormat.YAML, PlatformConfig.class, true));
    }
}
