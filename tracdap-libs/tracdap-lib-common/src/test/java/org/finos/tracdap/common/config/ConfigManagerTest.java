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
import org.finos.tracdap.common.exception.EConfigLoad;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config._ConfigFile;
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

    private static final List<String> CONFIG_SAMPLES = List.of(
            "/config_mgr_test/sample-config.yaml",
            "/config_mgr_test/sample-config.json",
            "/config_mgr_test/extra.xml",
            "/config_mgr_test/log-test.xml",
            "/config_mgr_test/secrets.p12",
            "/config_mgr_test/secrets.jceks",
            "/config_mgr_test/secrets-jceks.yaml");

    private static final String SECRET_KEY = "secret_master_key";
    private static final String SECRET_NAME = "very_secret_password";
    private static final String EXPECTED_SECRET_VALUE = "You'll_never guess, this very (!!!) secret [PASS=WORD]!";

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

        var configUrl = "config_dir/sample-config.yaml";
        var expectedRoot = tempDir.resolve("config_dir").normalize();
        
        var manager = new ConfigManager(configUrl, tempDir, plugins);
        var configRoot = Paths.get(manager.configRoot()).normalize();

        assertEquals(expectedRoot, configRoot);
        assertTrue(Files.exists(configRoot));
        assertTrue(Files.isDirectory(configRoot));
    }

    @Test
    void configRoot_nonFile() {
        
        var configUrl = "test://config_svr/config_dir/sample-config.yaml";
        var expectedRoot = URI.create("test://config_svr/config_dir/");

        var manager = new ConfigManager(configUrl, tempDir, plugins);
        var configRoot = manager.configRoot();

        assertEquals(expectedRoot, configRoot);
    }

    @Test
    void loadText_absolutePathOk() {
        
        var absoluteFilePath = tempDir.resolve("config_dir/sample-config.json")
                .toAbsolutePath()
                .normalize()
                .toString();

        var absoluteTestUrl = "test://config_svr/config_dir/sample-config.json";

        // Using file loader

        var fileConfigUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(fileConfigUrl, tempDir, plugins);
        
        var contentsFromFile = manager.loadTextConfig(absoluteFilePath);
        assertFalse(contentsFromFile.isBlank());

        var contentsFromTest = manager.loadTextConfig(absoluteTestUrl);
        assertFalse(contentsFromTest.isBlank());

        // Using test-protocol loader

        var testConfigUrl = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(testConfigUrl, tempDir, plugins);

        var contentsFromFile2 = manager2.loadTextConfig(absoluteFilePath);
        assertFalse(contentsFromFile2.isBlank());

        var contentsFromTest2 = manager2.loadTextConfig(absoluteTestUrl);
        assertFalse(contentsFromTest2.isBlank());
    }

    @Test
    void loadText_absolutePathNoProtocol() {

        // Missing protocol interpreted as "file" protocol for absolute paths
        
        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var absoluteUrl = "//config_svr/config_dir/sample-config.json";

        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig(absoluteUrl));
    }

    @Test
    void loadText_absolutePathUnknownProtocol() {

        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var absoluteUrl = "unknown://config_svr/config_dir/sample-config.json";

        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig(absoluteUrl));
    }

    @Test
    void loadText_relativePathOk() {

        // Using file loader

        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "sample-config.json";

        var contents = manager.loadTextConfig(relativePath);
        assertFalse(contents.isBlank());

        // Using test-protocol loader
        
        var configUrl2 = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        var relativePath2 = "sample-config.json";

        var contents2 = manager2.loadTextConfig(relativePath2);
        assertFalse(contents2.isBlank());
    }

    @Test
    void loadText_relativePathWithProtocol() {

        // Using file loader

        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "file:./sample-config.json";

        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig(relativePath));

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);
        
        var relativePath2 = "test:./sample-config.json";

        assertThrows(EConfigLoad.class, () -> manager2.loadTextConfig(relativePath2));
    }

    @Test
    void loadText_missingFile() {

        // Using file loader

        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "missing.properties";

        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig(relativePath));

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        assertThrows(EConfigLoad.class, () -> manager2.loadTextConfig(relativePath));
    }

    @Test
    void loadText_nullOrBlankUrl() {

        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig(null));
        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig(""));
        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig(" "));
        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig("\n"));
    }

    @Test
    void loadText_invalidUrl() {

        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig("file:::-:"));
        assertThrows(EConfigLoad.class, () -> manager.loadTextConfig("//_>>@"));
    }

    // loadProperties uses the same loading mechanism as loadTextFile
    // So, no need to repeate all the error cases!
    // Test the happy path, + 1 error case to make sure errors are still being handled

    @Test
    void loadConfig_ok() {

        // Using file loader

        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "sample-config.yaml";

        var config1 = manager.loadConfigObject(relativePath, PlatformConfig.class);
        var prop1 = config1.getConfigMap().get("logging");
        assertEquals("log-test.xml", prop1);

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        var config2 = manager2.loadConfigObject(relativePath, PlatformConfig.class);
        var prop2 = config2.getConfigMap().get("logging");
        assertEquals("log-test.xml", prop2);
    }

    @Test
    void loadConfig_missingFile() {

        // Using file loader

        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var relativePath = "missing.yaml";

        assertThrows(EConfigLoad.class, () -> manager.loadConfigObject(relativePath, _ConfigFile.class));

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        assertThrows(EConfigLoad.class, () -> manager2.loadConfigObject(relativePath, _ConfigFile.class));
    }

    @Test
    void loadRootConfig_ok() {

        // Using file loader

        var configUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        var config1 = manager.loadRootConfigObject(PlatformConfig.class);
        var prop1 = config1.getConfigMap().get("logging");
        assertEquals("log-test.xml", prop1);

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        var config2 = manager2.loadRootConfigObject(PlatformConfig.class);
        var prop2 = config2.getConfigMap().get("logging");
        assertEquals("log-test.xml", prop2);
    }

    @Test
    void loadRootConfig_missingFile() {

        // Using file loader

        var configUrl = "config_dir/missing.yaml";
        var manager = new ConfigManager(configUrl, tempDir, plugins);

        assertThrows(EConfigLoad.class, () -> manager.loadRootConfigObject(_ConfigFile.class));

        // Using test-protocol loader

        var configUrl2 = "test://config_svr/config_dir/missing.yaml";
        var manager2 = new ConfigManager(configUrl2, tempDir, plugins);

        assertThrows(EConfigLoad.class, () -> manager2.loadRootConfigObject(_ConfigFile.class));
    }

    @Test
    void loadRootConfig_nullOrBlankUrl() {

        assertThrows(EConfigLoad.class, () -> new ConfigManager(null, tempDir, plugins));
        assertThrows(EConfigLoad.class, () -> new ConfigManager("", tempDir, plugins));
        assertThrows(EConfigLoad.class, () -> new ConfigManager(" ", tempDir, plugins));
        assertThrows(EConfigLoad.class, () -> new ConfigManager("\n", tempDir, plugins));
    }

    @Test
    void loadRootConfig_invalidUrl() {

        var configUrl1 = "file:::-:";
        assertThrows(EConfigLoad.class, () -> new ConfigManager(configUrl1, tempDir, plugins)
                .loadRootConfigObject(_ConfigFile.class));

        var configUrl2 = "//_>>@";
        assertThrows(EConfigLoad.class, () -> new ConfigManager(configUrl2, tempDir, plugins)
                .loadRootConfigObject(_ConfigFile.class));
    }

    @Test
    void loadPassword_pkcs12() {

        // Using file loader

        var fileConfigUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(fileConfigUrl, tempDir, plugins, SECRET_KEY);
        manager.prepareSecrets();

        var textSecret = manager.loadPassword(SECRET_NAME);
        assertEquals(EXPECTED_SECRET_VALUE, textSecret);

        // Using test-protocol loader
        // This will still use the JKS secret loader, but the JKS file will be loaded over the test protocol

        var testConfigUrl = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(testConfigUrl, tempDir, plugins, SECRET_KEY);
        manager2.prepareSecrets();

        var textSecret2 = manager2.loadPassword(SECRET_NAME);
        assertEquals(EXPECTED_SECRET_VALUE, textSecret2);
    }

    @Test
    void loadPassword_jceks() {

        // Using file loader

        var fileConfigUrl = "config_dir/secrets-jceks.yaml";
        var manager = new ConfigManager(fileConfigUrl, tempDir, plugins, SECRET_KEY);
        manager.prepareSecrets();

        var textSecret = manager.loadPassword(SECRET_NAME);
        assertEquals(EXPECTED_SECRET_VALUE, textSecret);

        // Using test-protocol loader
        // This will still use the JKS secret loader, but the JKS file will be loaded over the test protocol

        var testConfigUrl = "test://config_svr/config_dir/secrets-jceks.yaml";
        var manager2 = new ConfigManager(testConfigUrl, tempDir, plugins, SECRET_KEY);
        manager2.prepareSecrets();

        var textSecret2 = manager2.loadPassword(SECRET_NAME);
        assertEquals(EXPECTED_SECRET_VALUE, textSecret2);
    }

    @Test
    void loadPassword_missing() {

        // Using file loader

        var fileConfigUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(fileConfigUrl, tempDir, plugins, SECRET_KEY);
        manager.prepareSecrets();

        assertThrows(EConfigLoad.class, () -> manager.loadPassword("UNKNOWN_SECRET"));

        // Using test-protocol loader
        // This will still use the JKS secret loader, but the JKS file will be loaded over the test protocol

        var testConfigUrl = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(testConfigUrl, tempDir, plugins, SECRET_KEY);
        manager2.prepareSecrets();

        assertThrows(EConfigLoad.class, () -> manager2.loadPassword("UNKNOWN_SECRET"));
    }

    @Test
    void loadPassword_invalid() {

        // Using file loader

        var fileConfigUrl = "config_dir/sample-config.yaml";
        var manager = new ConfigManager(fileConfigUrl, tempDir, plugins, SECRET_KEY);
        manager.prepareSecrets();

        assertThrows(EConfigLoad.class, () -> manager.loadPassword("$@£$%@-  ++03>?"));

        // Using test-protocol loader
        // This will still use the JKS secret loader, but the JKS file will be loaded over the test protocol

        var testConfigUrl = "test://config_svr/config_dir/sample-config.yaml";
        var manager2 = new ConfigManager(testConfigUrl, tempDir, plugins, SECRET_KEY);
        manager2.prepareSecrets();

        assertThrows(EConfigLoad.class, () -> manager2.loadPassword("$@£$%@-  ++03>?"));
    }
}
