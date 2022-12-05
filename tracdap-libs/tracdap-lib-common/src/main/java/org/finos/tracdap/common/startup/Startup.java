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

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.util.VersionInfo;

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
        var standardArgs = StandardArgsProcessor.processArgs(componentName, args, tasks, null);

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

    public static ConfigManager quickConfig(Path workingDir, String configFile, String keystoreKey) {

        var standardArgs = new StandardArgs(workingDir, configFile, keystoreKey);
        var startup = new StartupSequence(null, standardArgs, false);
        startup.runStartupSequence();

        return startup.getConfig();
    }
}
