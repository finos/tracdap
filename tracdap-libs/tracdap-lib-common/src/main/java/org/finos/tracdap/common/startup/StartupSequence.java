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

package org.finos.tracdap.common.startup;

import org.apache.logging.log4j.LogManager;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.util.VersionInfo;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.finos.tracdap.config._ConfigFile;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;


/**
 * Standard startup sequence for service components and offline tools.
 *
 * <p>The startup sequence is responsible for these tasks:
 * - Printing the startup banner
 * - Setting up the config manager and plugin manager
 * - Loading plugins required to access config files
 * - Loading logging config and initializing logging
 * </p>
 *
 * <p>The start up sequence is not responsible for setting up the main service / tool.
 * Standard command line args must be parsed before using the startup sequence.</p>
 */
public class StartupSequence {

    private static final String STARTUP_LOG_CONFIG = "/log4j2_startup.xml";

    private final Class<?> serviceClass;
    private final StandardArgs standardArgs;
    private final boolean doPrintBanner;

    private boolean sequenceComplete = false;
    private PluginManager plugins;
    private ConfigManager config;

    /** Create a startup sequence for the given service class and command line arguments **/
    StartupSequence(Class<?> serviceClass, StandardArgs standardArgs, boolean doPrintBanner) {
        this.serviceClass = serviceClass;
        this.standardArgs = standardArgs;
        this.doPrintBanner = doPrintBanner;
    }

    /** Run the startup sequence **/
    public void runStartupSequence() {

        if (doPrintBanner)
            printBanner(serviceClass);

        printSubBanner();

        initStartupLogging();
        initConfigPlugins();
        loadConfig();

        initLogging();

        sequenceComplete = true;
    }

    /**
     * Get the plugin manager that was configured during the startup sequence
     *
     * @return The PluginManager instance
     * **/
    public PluginManager getPlugins() {

        if (!sequenceComplete)
            throw new ETracInternal("Startup sequence has not been run");

        return plugins;
    }

    /**
     * Get the config manager that was configured during the startup sequence
     *
     * @return The ConfigManager instance
     **/
    public ConfigManager getConfig() {

        if (!sequenceComplete)
            throw new ETracInternal("Startup sequence has not been run");

        return config;
    }

    /**
     * Get the standard args that were used for this startup sequence
     *
     * @return The StandardArgs instance
     **/
    public StandardArgs getArgs() {
        return standardArgs;
    }

    /**
     * Print the headline banner for this service or tool
     *
     * @param serviceClass The main class of this service or tool
     **/
    public static void printBanner(Class<?> serviceClass) {

        var componentName = VersionInfo.getComponentName(serviceClass);
        var componentVersion = VersionInfo.getComponentVersion(serviceClass);

        var startupBanner = String.format(">>> %s %s", componentName, componentVersion);
        System.out.println(startupBanner);
    }

    private void printSubBanner() {
        System.out.println(">>> Working directory: " + standardArgs.getWorkingDir());
        System.out.println(">>> Config file: " + standardArgs.getConfigFile());
        System.out.println();
    }

    @SuppressWarnings("resource")
    private void initStartupLogging() {

        try (var logConfig = Startup.class.getResourceAsStream(STARTUP_LOG_CONFIG)) {

            if (logConfig == null)
                throw new EStartup("Failed to load logging config for bootstrap");

            var configSource = new ConfigurationSource(logConfig);
            Configurator.initialize(Startup.class.getClassLoader(), configSource);
        }
        catch (IOException e) {
            throw new EStartup("Failed to load logging config for startup sequence (this is a bug)");
        }
    }

    private void initConfigPlugins() {

        plugins = new PluginManager();
        plugins.initConfigPlugins();
    }

    private void loadConfig() {

        var configFile = standardArgs.getConfigFile();
        var workingDir = standardArgs.getWorkingDir();

        config = new ConfigManager(configFile, workingDir, plugins);
    }


    /**
     * Initialize the logging framework.
     *
     * <p>Logging can be configured by setting the property logging.url in the config
     * section of the root configuration, to point to the location of a logging config file.
     * TRAC uses Log4j2 as a backend for slf4j, so the logging config file must be a valid
     * log4j2 config file. If no logging config is provided, messages will be logged
     * to stdout.</p>
     *
     * <p>This method should be called immediately after calling initConfigPlugins(),
     * since it uses the config loading mechanism which relies on those plugins being
     * available.</p>
     *
     * @throws EStartup There was an error processing the logging config
     */
    @SuppressWarnings("resource")
    private void initLogging() {

        // Logger configured using initStartupLogging
        var log = LoggerFactory.getLogger(getClass());

        var baseConfig = config.loadRootConfigObject(_ConfigFile.class, /* leniency = */ true);
        var loggingConfigUrl = baseConfig.getConfigOrDefault(ConfigKeys.LOGGING_CONFIG_KEY, "");

        if (!loggingConfigUrl.isBlank()) {

            var loggingConfig = config.loadTextConfig(loggingConfigUrl);

            try (var configStream = new ByteArrayInputStream(loggingConfig.getBytes())) {

                var configSource = new ConfigurationSource(configStream);

                log.info("Initialize logging...");

                LogManager.shutdown();
                Configurator.initialize(getClass().getClassLoader(), configSource);

                // Invalid logging configuration cause the startup sequence to bomb out
            }
            catch (IOException e) {

                // Unexpected error condition - IO error reading from a byte buffer
                throw new EUnexpected(e);
            }
        }
        else {

            log.info("No logging config provided, using default...");
            Configurator.reconfigure();
        }
    }
}
