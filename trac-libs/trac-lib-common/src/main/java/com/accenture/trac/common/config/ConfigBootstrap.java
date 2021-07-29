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

package com.accenture.trac.common.config;

import com.accenture.trac.common.util.VersionInfo;

import java.nio.file.Path;
import java.nio.file.Paths;


public class ConfigBootstrap {

    public static ConfigManager useCommandLine(Class<?> serviceClass, String[] args) {

        printBanner(serviceClass);

        var componentName = VersionInfo.getComponentName(serviceClass);
        var standardArgs = StandardArgsProcessor.processArgs(componentName, args);

        return loadConfig(standardArgs);
    }

    public static ConfigManager useConfigFile(Class<?> serviceClass, String configFile) {

        var keystoreKey = "";
        return useConfigFile(serviceClass, configFile, keystoreKey);
    }

    public static ConfigManager useConfigFile(Class<?> serviceClass, String configFile, String keystoreKey) {

        var workingDir = Paths.get(".").toAbsolutePath().normalize();
        return useConfigFile(serviceClass, workingDir, configFile, keystoreKey);
    }

    public static ConfigManager useConfigFile(Class<?> serviceClass, Path workingDir, String configFile, String keystoreKey) {

        printBanner(serviceClass);

        var standardArgs = new StandardArgs(workingDir, configFile, keystoreKey);

        return loadConfig(standardArgs);
    }

    private static void printBanner(Class<?> serviceClass) {

        var componentName = VersionInfo.getComponentName(serviceClass);
        var componentVersion = VersionInfo.getComponentVersion(serviceClass);

        var startupBanner = String.format(">>> %s %s", componentName, componentVersion);
        System.out.println(startupBanner);
    }

    private static ConfigManager loadConfig(StandardArgs standardArgs) {

        System.out.println(">>> Working directory: " + standardArgs.getWorkingDir());
        System.out.println(">>> Config file: " + standardArgs.getConfigFile());
        System.out.println();

        var configManager = new ConfigManager(standardArgs);
        configManager.initConfigPlugins();
        configManager.initLogging();

        return configManager;
    }
}
