/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.plugins.ssh.executor;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exec.ExecutorBasicTestSuite;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.test.config.ConfigHelpers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;


@Tag("integration")
@Tag("int-executor")
@Tag("ssh")
public class SshExecutorBasicTest extends ExecutorBasicTestSuite {

    // This integration test can be run offline by setting up an execution host and configuring these settings

    // TODO: Add this test to the integration CI workflow
    // Use env vars to pass in configuration

    private static final String REMOTE_ADDRESS = "";
    private static final String REMOTE_PORT = "";
    private static final String BATCH_USER = "";
    private static final String KEY_FILE = "";

    private static final String VENV_PATH = "/opt/trac/venv";
    private static final String BATCH_DIR = "/opt/trac/jobs";
    private static final String BATCH_PERSIST = "false";

    @BeforeEach
    public void setupExecutor(@TempDir Path tempDir) throws Exception {

        ConfigHelpers.prepareConfig(List.of(KEY_FILE), tempDir, Map.of());

        var pluginManager = new PluginManager();
        pluginManager.initConfigPlugins();
        pluginManager.initRegularPlugins();

        var workingDir = Paths.get(".");
        var configManager = new ConfigManager(tempDir.toString(), workingDir, pluginManager);

        var executorProps = new Properties();
        executorProps.setProperty("remoteHost", REMOTE_ADDRESS);
        executorProps.setProperty("remotePort", REMOTE_PORT);
        executorProps.setProperty("batchUser", BATCH_USER);
        executorProps.setProperty("keyFile", tempDir.resolve(KEY_FILE).toString());
        executorProps.setProperty("venvPath", VENV_PATH);
        executorProps.setProperty("batchDir", BATCH_DIR);
        executorProps.setProperty("batchPersist", BATCH_PERSIST);

        executor = new SshExecutor(executorProps, configManager);
        executor.start();
    }

    @Override
    protected boolean targetIsWindows() {

        // SSH target is never Windows
        return false;
    }
}
