/*
 * Copyright 2020 Accenture Global Solutions Limited
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
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.plugin.PluginManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class ConfigManagerTest {

    List<String> CONFIG_SAMPLES = List.of(
            "/config_mgr_test/root.yaml",
            "/config_mgr_test/secondary.json",
            "/config_mgr_test/extra.xml",
            "/config_mgr_test/log-test.xml");

    PluginManager plugins;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setup() throws Exception {

        plugins = new PluginManager();
        plugins.initConfigPlugins();

        Files.createDirectory(tempDir.resolve("config_dir"));

        for (var sourceFile : CONFIG_SAMPLES) {

            var fileName = Paths.get(sourceFile).getFileName();
            var targetPath = tempDir.resolve("config_dir").resolve(fileName);

            try (var inputStream = getClass().getResourceAsStream(sourceFile);
                 var outputStream = Files.newOutputStream(targetPath)) {

                if (inputStream == null)
                    throw new EUnexpected();

                inputStream.transferTo(outputStream);
            }
        }

        TestConfigPlugin.setCurrentTempDir(tempDir);
    }

    @Test
    void configRoot_file() {

        var configUrl = "config_dir/root.yaml";
        var expectedRoot = tempDir.resolve("config_dir").normalize();
        
        var manager = new ConfigManager(configUrl, tempDir, plugins);
        var configRoot = Paths.get(manager.configRoot()).normalize();

        assertEquals(expectedRoot, configRoot);
        assertTrue(Files.exists(configRoot));
        assertTrue(Files.isDirectory(configRoot));
    }

    @Test
    void configRoot_nonFile() {
        
        var configUrl = "test://config_svr/config_dir/root.yaml";
        var expectedRoot = URI.create("test://config_svr/config_dir/");

        var manager = new ConfigManager(configUrl, tempDir, plugins);
        var configRoot = manager.configRoot();

        assertEquals(expectedRoot, configRoot);
    }

    @Test
    void loadText_absolutePathOk() {
        
        var absoluteFilePath = tempDir.resolve("config_dir/secondary.json")
                .toAbsolutePath()
                .normalize()
                .toString();

        var absoluteTestUrl = "test://config_svr/config_dir/secondary.json";

        // Using file loader

        var fileConfigUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(fileConfigUrl, tempDir, plugins);
        
        var contentsFromFile = manager.loadConfigFile(absoluteFilePath);
        assertFalse(contentsFromFile.isBlank());

        var contentsFromTest = manager.loadConfigFile(absoluteTestUrl);
        assertFalse(contentsFromTest.isBlank());

        // Using test-protocol loader

        var testConfigUrl = "test://config_svr/config_dir/root.yaml";
        var manager2 = new ConfigManager(testConfigUrl, tempDir, plugins);

        var contentsFromFile2 = manager2.loadConfigFile(absoluteFilePath);
        assertFalse(contentsFromFile2.isBlank());

        var contentsFromTest2 = manager2.loadConfigFile(absoluteTestUrl);
        assertFalse(contentsFromTest2.isBlank());
    }

    @Test
    void loadText_absolutePathNoProtocol() {

        // Missing protocol interpreted as "file" protocol for absolute paths
        
        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var absoluteUrl = "//config_svr/config_dir/secondary.json";

        assertThrows(EStartup.class, () -> manager.loadConfigFile(absoluteUrl));
    }

    @Test
    void loadText_absolutePathUnknownProtocol() {

        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var absoluteUrl = "unknown://config_svr/config_dir/secondary.json";

        assertThrows(EStartup.class, () -> manager.loadConfigFile(absoluteUrl));
    }

    @Test
    void loadText_relativePathOk() {

        // Using file loader

        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "secondary.json";

        var contents = manager.loadConfigFile(relativePath);
        assertFalse(contents.isBlank());

        // Using test-protocol loader
        
        var configUrl2 = "test://config_svr/config_dir/root.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        var relativePath2 = "secondary.json";

        var contents2 = manager2.loadConfigFile(relativePath2);
        assertFalse(contents2.isBlank());
    }

    @Test
    void loadText_relativePathWithProtocol() {

        // Using file loader

        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "file:./secondary.json";

        assertThrows(EStartup.class, () -> manager.loadConfigFile(relativePath));

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/root.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);
        
        var relativePath2 = "test:./secondary.json";

        assertThrows(EStartup.class, () -> manager2.loadConfigFile(relativePath2));
    }

    @Test
    void loadText_missingFile() {

        // Using file loader

        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "missing.properties";

        assertThrows(EStartup.class, () -> manager.loadConfigFile(relativePath));

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/root.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        assertThrows(EStartup.class, () -> manager2.loadConfigFile(relativePath));
    }

    @Test
    void loadText_nullOrBlankUrl() {

        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        assertThrows(EStartup.class, () -> manager.loadConfigFile(null));
        assertThrows(EStartup.class, () -> manager.loadConfigFile(""));
        assertThrows(EStartup.class, () -> manager.loadConfigFile(" "));
        assertThrows(EStartup.class, () -> manager.loadConfigFile("\n"));
    }

    @Test
    void loadText_invalidUrl() {

        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        assertThrows(EStartup.class, () -> manager.loadConfigFile("file:::-:"));
        assertThrows(EStartup.class, () -> manager.loadConfigFile("//_>>@"));
    }

    // loadProperties uses the same loading mechanism as loadTextFile
    // So, no need to repeate all the error cases!
    // Test the happy path, + 1 error case to make sure errors are still being handled

    @Test
    void loadConfig_ok() {

        // Using file loader

        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "root.yaml";

        var config = manager.loadConfigObject(relativePath, ConfigParserTest.SampleConfig.class);
        var prop1 = config.test.getProp1();

        assertEquals("hello", prop1);

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/root.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        var config2 = manager2.loadConfigObject(relativePath, ConfigParserTest.SampleConfig.class);
        var prop2 = config2.test.getProp2();
        assertEquals(42, prop2);
    }

    @Test
    void loadConfig_missingFile() {

        // Using file loader

        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "missing.yaml";

        assertThrows(EStartup.class, () -> manager.loadConfigObject(relativePath, ConfigParserTest.SampleConfig.class));

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/root.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        assertThrows(EStartup.class, () -> manager2.loadConfigObject(relativePath, ConfigParserTest.SampleConfig.class));
    }

    @Test
    void loadRootConfig_ok() {

        // Using file loader

        var configUrl = "config_dir/root.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var config = manager.loadRootConfigObject(ConfigParserTest.SampleConfig.class);
        var prop1 = config.test.getProp1();
        assertEquals("hello", prop1);

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/root.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        var config2 = manager2.loadRootConfigObject(ConfigParserTest.SampleConfig.class);
        var prop2 = config2.test.getProp2();
        assertEquals(42, prop2);
    }

    @Test
    void loadRootConfig_missingFile() {

        // Using file loader

        var configUrl = "config_dir/missing.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        assertThrows(EStartup.class, () -> manager.loadRootConfigObject(ConfigParserTest.SampleConfig.class));

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/missing.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        assertThrows(EStartup.class, () -> manager2.loadRootConfigObject(ConfigParserTest.SampleConfig.class));
    }

    @Test
    void loadRootConfig_nullOrBlankUrl() {

        assertThrows(EStartup.class, () -> new ConfigManager(null, tempDir, plugins));
        assertThrows(EStartup.class, () -> new ConfigManager("", tempDir, plugins));
        assertThrows(EStartup.class, () -> new ConfigManager(" ", tempDir, plugins));
        assertThrows(EStartup.class, () -> new ConfigManager("\n", tempDir, plugins));
    }

    @Test
    void loadRootConfig_invalidUrl() {

        var configUrl1 = "file:::-:";
        assertThrows(EStartup.class, () -> new ConfigManager(configUrl1, tempDir, plugins)
                .loadRootConfigObject(ConfigParserTest.SampleConfig.class));

        var configUrl2 = "//_>>@";
        assertThrows(EStartup.class, () -> new ConfigManager(configUrl2, tempDir, plugins)
                .loadRootConfigObject(ConfigParserTest.SampleConfig.class));
    }
}
