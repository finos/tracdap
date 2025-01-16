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

package org.finos.tracdap.common.config;

import org.finos.tracdap.common.config.test.TestConfigExtension;
import org.finos.tracdap.common.config.test.TestConfigPlugin;
import org.finos.tracdap.common.exception.EConfigParse;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.common.util.ResourceHelpers;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.test.config.TestConfigExt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.List;
import java.util.Random;


public class ConfigParserTest {

    private static final String SAMPLE_YAML_CONFIG = "/config_mgr_test/sample-config.yaml";
    private static final String SAMPLE_JSON_CONFIG = "/config_mgr_test/sample-config.json";

    private static final String UNKNOWN_ITEM_YAML_CONFIG = "/config_mgr_test/unknown-item.yaml";
    private static final String UNKNOWN_ITEM_JSON_CONFIG = "/config_mgr_test/unknown-item.json";

    private static final String EXTENSION_TEST_CONFIG = "/config_mgr_test/extension-test.yaml";

    private ConfigParser configParser;

    @BeforeAll
    public static void testLoader() {

        // The test plugin for config loaders is on the class path and will error if not initialized
        TestConfigPlugin.setCurrentTempDir(Paths.get("."));
    }

    @BeforeEach
    void setupParser() {
        configParser = new ConfigParser();
    }

    @Test
    public void testJson_basicLoadOk() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(SAMPLE_JSON_CONFIG);
        var configObject = configParser.parseConfig(configBytes, ConfigFormat.JSON, PlatformConfig.class);

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
                configParser.parseConfig(configBytes, ConfigFormat.JSON, PlatformConfig.class));
    }

    @Test
    public void testJson_unknownItem() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(UNKNOWN_ITEM_JSON_CONFIG);

        Assertions.assertThrows(EConfigParse.class, () ->
                configParser.parseConfig(configBytes, ConfigFormat.JSON, PlatformConfig.class));
    }

    @Test
    public void testJson_lenient() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(UNKNOWN_ITEM_JSON_CONFIG);

        Assertions.assertDoesNotThrow(() ->
                configParser.parseConfig(configBytes, ConfigFormat.JSON, PlatformConfig.class, true));
    }

    @Test
    public void testYaml_basicLoadOk() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(SAMPLE_YAML_CONFIG);
        var configObject = configParser.parseConfig(configBytes, ConfigFormat.YAML, PlatformConfig.class);

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
                configParser.parseConfig(configBytes, ConfigFormat.YAML, PlatformConfig.class));
    }

    @Test
    public void testYaml_unknownItem() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(UNKNOWN_ITEM_YAML_CONFIG);

        Assertions.assertThrows(EConfigParse.class, () ->
                configParser.parseConfig(configBytes, ConfigFormat.YAML, PlatformConfig.class));
    }

    @Test
    public void testYaml_lenient() {

        var configBytes = ResourceHelpers.loadResourceAsBytes(UNKNOWN_ITEM_YAML_CONFIG);

        Assertions.assertDoesNotThrow(() ->
                configParser.parseConfig(configBytes, ConfigFormat.YAML, PlatformConfig.class, true));
    }

    @Test
    public void testConfigExtensions() throws Exception {

        var configBytes = ResourceHelpers.loadResourceAsBytes(EXTENSION_TEST_CONFIG);

        var extension = new TestConfigExtension();
        var parser = new ConfigParser(List.of(extension));

        var config = parser.parseConfig(configBytes, ConfigFormat.YAML, PlatformConfig.class);

        Assertions.assertEquals(3, config.getExtensionsCount());
        Assertions.assertTrue(config.containsExtensions("test_core_metadata_type"));
        Assertions.assertTrue(config.containsExtensions("test_core_config_type"));
        Assertions.assertTrue(config.containsExtensions("test_ext_type"));

        var coreMeta = config.getExtensionsOrThrow("test_core_metadata_type");
        var coreConfig = config.getExtensionsOrThrow("test_core_config_type");
        var extConfig = config.getExtensionsOrThrow("test_ext_type");

        Assertions.assertTrue(coreMeta.is(TagSelector.class));
        Assertions.assertEquals(ObjectType.MODEL, coreMeta.unpack(TagSelector.class).getObjectType());

        Assertions.assertTrue(coreConfig.is(PluginConfig.class));
        Assertions.assertEquals("DUMMY_PROTOCOL", coreConfig.unpack(PluginConfig.class).getProtocol());

        Assertions.assertTrue(extConfig.is(TestConfigExt.class));
        Assertions.assertEquals("some_value", extConfig.unpack(TestConfigExt.class).getSetting1());
    }
}
