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

package com.accenture.trac.test.meta;

import com.accenture.trac.common.db.JdbcSetup;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.startup.Startup;
import com.accenture.trac.common.util.InterfaceLogging;
import com.accenture.trac.common.db.JdbcDialect;
import com.accenture.trac.config.PlatformConfig;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcMetadataDal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.*;

import javax.sql.DataSource;
import java.nio.file.Paths;
import java.util.Properties;


public class JdbcIntegration implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

    private static final String TRAC_CONFIG_FILE = "TRAC_CONFIG_FILE";
    private static final String TRAC_KEYSTORE_KEY = "TRAC_KEYSTORE_KEY";

    private JdbcDialect dialect;
    private DataSource source;
    private JdbcMetadataDal dal;

    @Override
    public void beforeAll(ExtensionContext context) {

        // Method for getting standard args from environment instead of command line
        // It may be useful to have this in StandardArgsProcessor

        var env = System.getenv();
        var workingDir = Paths.get(".").toAbsolutePath().normalize();
        var configFile = env.get(TRAC_CONFIG_FILE);
        var keystoreKey = env.get(TRAC_KEYSTORE_KEY);

        if (configFile == null || configFile.isBlank())
            throw new EStartup("Missing environment variable for integration testing: " + TRAC_CONFIG_FILE);

        var configManager = Startup.quickConfig(workingDir, configFile, keystoreKey);
        var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
        var metaConfig = platformConfig.getServices().getMeta();

        var dalProps = new Properties();
        dalProps.putAll(metaConfig.getDalPropsMap());

        dialect = JdbcSetup.getSqlDialect(dalProps, "");
        source = JdbcSetup.createDatasource(dalProps, "");
    }

    @Override
    public void beforeEach(ExtensionContext context) {

        var testClass = context.getTestClass();

        if (testClass.isEmpty() || !IDalTestable.class.isAssignableFrom(testClass.get()))
            Assertions.fail("JUnit extension for DAL testing requires the test class to implement IDalTestable");

        var dal = new JdbcMetadataDal(dialect, source, Runnable::run);
        dal.startup();

        this.dal = dal;

        var dalWithLogging = InterfaceLogging.wrap(dal, IMetadataDal.class);
        var testInstance = context.getTestInstance();

        if (testInstance.isPresent()) {
            var testCase = (IDalTestable) testInstance.get();
            testCase.setDal(dalWithLogging);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {

        if (dal != null)
            dal.shutdown();
    }

    @Override
    public void afterAll(ExtensionContext context) {

        JdbcSetup.destroyDatasource(source);
        source = null;
    }
}
