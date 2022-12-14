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

package org.finos.tracdap.test.meta;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.common.util.InterfaceLogging;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.svc.meta.dal.IMetadataDal;
import org.finos.tracdap.svc.meta.dal.jdbc.JdbcMetadataDal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.*;

import java.nio.file.Paths;


public class JdbcIntegration implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback {

    private static final String TRAC_CONFIG_FILE = "TRAC_CONFIG_FILE";
    private static final String TRAC_SECRET_KEY = "TRAC_SECRET_KEY";

    private ConfigManager configManager;
    private PluginConfig metaDbConfig;
    private JdbcMetadataDal dal;

    @Override
    public void beforeAll(ExtensionContext context) {

        // Method for getting standard args from environment instead of command line
        // It may be useful to have this in StandardArgsProcessor

        var env = System.getenv();
        var workingDir = Paths.get(".").toAbsolutePath().normalize();
        var configFile = env.get(TRAC_CONFIG_FILE);
        var keystoreKey = env.get(TRAC_SECRET_KEY);

        if (configFile == null || configFile.isBlank())
            throw new EStartup("Missing environment variable for integration testing: " + TRAC_CONFIG_FILE);

        configManager = Startup.quickConfig(workingDir, configFile, keystoreKey);

        var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
        metaDbConfig = platformConfig.getMetadata().getDatabase();
    }

    @Override
    public void beforeEach(ExtensionContext context) {

        var testClass = context.getTestClass();

        if (testClass.isEmpty() || !IDalTestable.class.isAssignableFrom(testClass.get()))
            Assertions.fail("JUnit extension for DAL testing requires the test class to implement IDalTestable");

        var dialect = JdbcSetup.getSqlDialect(metaDbConfig);
        var source = JdbcSetup.createDatasource(configManager, metaDbConfig);

        dal = new JdbcMetadataDal(dialect, source);
        dal.start();

        var dalWithLogging = InterfaceLogging.wrap(dal, IMetadataDal.class);
        var testInstance = context.getTestInstance();

        if (testInstance.isPresent()) {
            var testCase = (IDalTestable) testInstance.get();
            testCase.setDal(dalWithLogging);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {

        if (dal != null) {
            dal.stop();
            dal = null;
        }
    }
}
