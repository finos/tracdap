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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class ConfigManagerTest {

    List<String> CONFIG_SAMPLES = List.of(
            "/config_mgr_test/root.properties",
            "/config_mgr_test/secondary.properties",
            "/config_mgr_test/extra.xml",
            "/config_mgr_test/log-test.xml");

    @TempDir
    Path tempDir;

    @BeforeEach
    void setupConfigDir() throws Exception {

        Files.createDirectory(tempDir.resolve("config_dir"));

        for (var sourceFile : CONFIG_SAMPLES) {

            var fileName = Paths.get(sourceFile).getFileName();
            var targetPath = tempDir.resolve("config_dir").resolve(fileName);

            try (var inputStream = getClass().getResourceAsStream(sourceFile);
                 var outputStream = Files.newOutputStream(targetPath)) {

                inputStream.transferTo(outputStream);
            }
        }

        TestConfigPlugin.setCurrentTempDir(tempDir);
    }

    @Test
    void configRoot_file() {

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);

        var configRoot = Paths.get(manager.configRoot()).normalize();
        var expectedRoot = tempDir.resolve("config_dir").normalize();

        assertEquals(expectedRoot, configRoot);
        assertTrue(Files.exists(configRoot));
        assertTrue(Files.isDirectory(configRoot));
    }

    @Test
    void configRoot_nonFile() {

        var args = new StandardArgs(tempDir, "test://config_svr/config_dir/root.properties", "password");
        var manager = new ConfigManager(args);

        var configRoot = manager.configRoot();
        var expectedRoot = URI.create("test://config_svr/config_dir/");

        assertEquals(expectedRoot, configRoot);
    }

    @Test
    void initPlugins() {

        var args = new StandardArgs(tempDir, "not_used.properties", "password");
        var manager = new ConfigManager(args);

        var protocolsNoPlugins = manager.protocols();
        assertTrue(protocolsNoPlugins.isEmpty());

        manager.initConfigPlugins();

        var protocols = manager.protocols();
        assertTrue(protocols.size() >= 2);
        assertTrue(protocols.contains("file"));  // "file" protocol always provided by the core implementation
        assertTrue(protocols.contains("test"));  // "test" protocol is provided by this module's test code
    }

    @Test
    void initLogging() {

        var args1 = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager1 = new ConfigManager(args1);

        manager1.initConfigPlugins();
        manager1.initLogging();

        assertDoesNotThrow(() -> LoggerFactory
                .getLogger(getClass())
                .info("Logging test completed"));

        var args2 = new StandardArgs(tempDir, "config_dir/secondary.properties", "password");
        var manager2 = new ConfigManager(args2);

        manager2.initConfigPlugins();
        manager2.initLogging();

        assertDoesNotThrow(() -> LoggerFactory
                .getLogger(getClass())
                .info("Logging test completed"));
    }

    @Test
    void loadText_absolutePathOk() {

        // Using file loader

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        var absolutePath = tempDir.resolve("config_dir/secondary.properties")
                .toAbsolutePath()
                .normalize()
                .toString();

        var absoluteUrl = "test://config_svr/config_dir/secondary.properties";

        var contentsFromFile = manager.loadTextFile(absolutePath);
        assertFalse(contentsFromFile.isBlank());

        var contentsFromTest = manager.loadTextFile(absoluteUrl);
        assertFalse(contentsFromTest.isBlank());

        // Using test-protocol loader

        var args2 = new StandardArgs(tempDir, "test://config_svr/config_dir/root.properties", "password");
        var manager2 = new ConfigManager(args2);
        manager2.initConfigPlugins();

        var contentsFromFile2 = manager2.loadTextFile(absolutePath);
        assertFalse(contentsFromFile2.isBlank());

        var contentsFromTest2 = manager2.loadTextFile(absoluteUrl);
        assertFalse(contentsFromTest2.isBlank());
    }

    @Test
    void loadText_absolutePathNoProtocol() {

        // Missing protocol interpreted as "file" protocol for absolute paths

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        var absoluteUrl = "//config_svr/config_dir/secondary.properties";

        assertThrows(EStartup.class, () -> manager.loadTextFile(absoluteUrl));
    }

    @Test
    void loadText_absolutePathUnknownProtocol() {

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        var absoluteUrl = "unknown://config_svr/config_dir/secondary.properties";

        assertThrows(EStartup.class, () -> manager.loadTextFile(absoluteUrl));
    }

    @Test
    void loadText_relativePathOk() {

        // Using file loader

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        var relativePath = "secondary.properties";

        var contents = manager.loadTextFile(relativePath);
        assertFalse(contents.isBlank());

        // Using test-protocol loader

        var args2 = new StandardArgs(tempDir, "test://config_svr/config_dir/root.properties", "password");
        var manager2 = new ConfigManager(args2);
        manager2.initConfigPlugins();

        var relativePath2 = "secondary.properties";

        var contents2 = manager2.loadTextFile(relativePath2);
        assertFalse(contents2.isBlank());
    }

    @Test
    void loadText_relativePathWithProtocol() {

        // Using file loader

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        var relativePath = "file:./secondary.properties";

        assertThrows(EStartup.class, () -> manager.loadTextFile(relativePath));

        // Using test-protocol loader

        var args2 = new StandardArgs(tempDir, "test://config_svr/config_dir/root.properties", "password");
        var manager2 = new ConfigManager(args2);
        manager2.initConfigPlugins();

        var relativePath2 = "test:./secondary.properties";

        assertThrows(EStartup.class, () -> manager2.loadTextFile(relativePath2));
    }

    @Test
    void loadText_missingFile() {

        // Using file loader

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        var relativePath = "missing.properties";

        assertThrows(EStartup.class, () -> manager.loadTextFile(relativePath));

        // Using test-protocol loader

        var args2 = new StandardArgs(tempDir, "test://config_svr/config_dir/root.properties", "password");
        var manager2 = new ConfigManager(args2);
        manager2.initConfigPlugins();

        assertThrows(EStartup.class, () -> manager2.loadTextFile(relativePath));
    }

    @Test
    void loadText_nullOrBlankUrl() {

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        assertThrows(EStartup.class, () -> manager.loadTextFile(null));
        assertThrows(EStartup.class, () -> manager.loadTextFile(""));
        assertThrows(EStartup.class, () -> manager.loadTextFile(" "));
        assertThrows(EStartup.class, () -> manager.loadTextFile("\n"));
    }

    @Test
    void loadText_invalidUrl() {

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        assertThrows(EStartup.class, () -> manager.loadTextFile("file:::-:"));
        assertThrows(EStartup.class, () -> manager.loadTextFile("//_>>@"));
    }

    // loadProperties uses the same loading mechanism as loadTextFile
    // So, no need to repeate all the error cases!
    // Test the happy path, + 1 error case to make sure errors are still being handled

    @Test
    void loadConfig_ok() {

        // Using file loader

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        var relativePath = "secondary.properties";

        var props = manager.loadProperties(relativePath);
        var prop1 = props.getProperty("secondary.prop1");
        assertEquals("world", prop1);

        // Using test-protocol loader

        var args2 = new StandardArgs(tempDir, "test://config_svr/config_dir/root.properties", "password");
        var manager2 = new ConfigManager(args2);
        manager2.initConfigPlugins();

        var props2 = manager2.loadProperties(relativePath);
        var prop2 = props2.getProperty("secondary.prop2");
        assertEquals("43", prop2);
    }

    @Test
    void loadConfig_missingFile() {

        // Using file loader

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        var relativePath = "missing.properties";

        assertThrows(EStartup.class, () -> manager.loadProperties(relativePath));

        // Using test-protocol loader

        var args2 = new StandardArgs(tempDir, "test://config_svr/config_dir/root.properties", "password");
        var manager2 = new ConfigManager(args2);
        manager2.initConfigPlugins();

        assertThrows(EStartup.class, () -> manager2.loadProperties(relativePath));
    }

    @Test
    void loadRootConfig_ok() {

        // Using file loader

        var args = new StandardArgs(tempDir, "config_dir/root.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        var props = manager.loadRootProperties();
        var prop1 = props.getProperty("root.prop1");
        assertEquals("hello", prop1);

        // Using test-protocol loader

        var args2 = new StandardArgs(tempDir, "test://config_svr/config_dir/root.properties", "password");
        var manager2 = new ConfigManager(args2);
        manager2.initConfigPlugins();

        var props2 = manager2.loadRootProperties();
        var prop2 = props2.getProperty("root.prop2");
        assertEquals("42", prop2);
    }

    @Test
    void loadRootConfig_missingFile() {

        // Using file loader

        var args = new StandardArgs(tempDir, "config_dir/missing.properties", "password");
        var manager = new ConfigManager(args);
        manager.initConfigPlugins();

        assertThrows(EStartup.class, manager::loadRootProperties);

        // Using test-protocol loader

        var args2 = new StandardArgs(tempDir, "test://config_svr/config_dir/missing.properties", "password");
        var manager2 = new ConfigManager(args2);
        manager2.initConfigPlugins();

        assertThrows(EStartup.class, manager2::loadRootProperties);
    }

    @Test
    void loadRootConfig_nullOrBlankUrl() {

        var args1 = new StandardArgs(tempDir, null, "password");
        assertThrows(EStartup.class, () -> new ConfigManager(args1));

        var args2 = new StandardArgs(tempDir, "", "password");
        assertThrows(EStartup.class, () -> new ConfigManager(args2));

        var args3 = new StandardArgs(tempDir, " ", "password");
        assertThrows(EStartup.class, () -> new ConfigManager(args3));

        var args4 = new StandardArgs(tempDir, "\n", "password");
        assertThrows(EStartup.class, () -> new ConfigManager(args4));
    }

    @Test
    void loadRootConfig_invalidUrl() {

        var args1 = new StandardArgs(tempDir, "file:::-:", "password");
        assertThrows(EStartup.class, () -> new ConfigManager(args1).loadRootProperties());

        var args2 = new StandardArgs(tempDir, "//_>>@", "password");
        assertThrows(EStartup.class, () -> new ConfigManager(args2).loadRootProperties());
    }
}
