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

package com.accenture.trac.common.startup;

import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.config.StandardArgs;
import com.accenture.trac.common.config.StandardArgsProcessor;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.plugin.IPluginManager;
import com.accenture.trac.common.util.VersionInfo;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;


public class Startup {

    public static StartupSequence useCommandLine(Class<?> serviceClass, String[] args) {

        return useCommandLine(serviceClass, args, List.of());
    }

    public static StartupSequence useCommandLine(Class<?> serviceClass, String[] args, List<StandardArgs.Task> tasks) {

        StartupSequence.printBanner(serviceClass);

        var componentName = VersionInfo.getComponentName(serviceClass);
        var standardArgs = StandardArgsProcessor.processArgs(componentName, args, tasks);

        return new StartupSequence(serviceClass, standardArgs, false);
    }

    public static StartupSequence useConfigFile(Class<?> serviceClass, String configFile) {

        var keystoreKey = "";
        return useConfigFile(serviceClass, configFile, keystoreKey);
    }

    public static StartupSequence useConfigFile(Class<?> serviceClass, String configFile, String keystoreKey) {

        var workingDir = Paths.get(".").toAbsolutePath().normalize();
        return useConfigFile(serviceClass, workingDir, configFile, keystoreKey);
    }

    public static StartupSequence useConfigFile(Class<?> serviceClass, Path workingDir, String configFile, String keystoreKey) {

        StartupSequence.printBanner(serviceClass);

        var standardArgs = new StandardArgs(workingDir, configFile, keystoreKey);

        return new StartupSequence(serviceClass, standardArgs, false);
    }

    public static ConfigManager quickConfig(String configFile) {

        var workingDir = Paths.get(".").toAbsolutePath().normalize();
        var keystoreKey = "";

        return quickConfig(workingDir, configFile, keystoreKey);
    }

    public static ConfigManager quickConfig(Path workingDir, String configFile, String keystoreKey) {

        var standardArgs = new StandardArgs(workingDir, configFile, keystoreKey);
        var startup = new StartupSequence(null, standardArgs, false);
        startup.runStartupSequence();

        return startup.getConfig();
    }

//    public static ConfigManager quickConfig(Class<?> serviceClass, Path workingDir, String configFile, String keystoreKey) {
//
//        var startup = useConfigFile(serviceClass, workingDir, configFile, keystoreKey);
//        startup.runStartupSequence();
//
//        return startup.getConfig();
//    }

//    public static StartupSequence useConfigFile(String configFile) {
//
//        var workingDir = Paths.get(".").toAbsolutePath().normalize();
//        var keystoreKey = "";
//
//        var standardArgs = new StandardArgs(workingDir, configFile, keystoreKey);
//
//        return new StartupSequence(serviceClass, standardArgs);
//    }
}
