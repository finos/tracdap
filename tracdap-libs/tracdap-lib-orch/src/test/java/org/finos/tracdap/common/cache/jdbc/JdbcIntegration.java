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

package org.finos.tracdap.common.cache.jdbc;

import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.config.PlatformConfig;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.nio.file.Paths;


public class JdbcIntegration implements BeforeAllCallback, AfterAllCallback {

    private static final String TRAC_CONFIG_FILE = "TRAC_CONFIG_FILE";
    private static final String TRAC_SECRET_KEY = "TRAC_SECRET_KEY";

    private DataSource dataSource;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        try {

            var env = System.getenv();
            var workingDir = Paths.get(".").toAbsolutePath().normalize();
            var configFile = env.get(TRAC_CONFIG_FILE);
            var keystoreKey = env.get(TRAC_SECRET_KEY);

            if (configFile == null || configFile.isBlank())
                throw new EStartup("Missing environment variable for integration testing: " + TRAC_CONFIG_FILE);

            var configManager = Startup.quickConfig(workingDir, configFile, keystoreKey);
            var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
            var cacheConfig = platformConfig.getJobCache();

            var dialect = JdbcSetup.getSqlDialect(cacheConfig);
            dataSource= JdbcSetup.createDatasource(configManager, cacheConfig);

            var cacheManager = new JdbcJobCacheManager(dataSource, dialect);

            var testClassMaybe = context.getTestClass();

            if (testClassMaybe.isEmpty())
                throw new EUnexpected();

            var testClass = testClassMaybe.get();
            var managerField = testClass.getDeclaredField("cacheManager");
            managerField.setAccessible(true);
            managerField.set(null, cacheManager);
        }
        catch (Exception e) {

            if (dataSource != null) {
                JdbcSetup.destroyDatasource(dataSource);
                dataSource = null;
            }

            throw e;
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {

        if (dataSource != null) {
            JdbcSetup.destroyDatasource(dataSource);
            dataSource = null;
        }
    }
}
