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

package com.accenture.trac.test.config;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class ConfigHelpers {

    private static final String BACKSLASH = "/";

    public static URL prepareConfig(
            String routConfig, Path targetDir,
            Map<String, String> substitutions)
            throws Exception {

        var rootConfigUrl = ConfigHelpers.class
                .getClassLoader()
                .getResource(routConfig);

        if (rootConfigUrl == null) {
            var err = String.format("Config resource not found: [%s]", routConfig);
            throw new RuntimeException(err);
        }

        var rootConfigPath = rootConfigUrl.getPath();
        var pathSeparator = FileSystems.getDefault().getSeparator();

        if (rootConfigUrl.getPath().startsWith(BACKSLASH) && !BACKSLASH.equals(pathSeparator))
            rootConfigPath = rootConfigPath.substring(1);

        var sourceDir = Paths.get(rootConfigPath).getParent();
        var targetRootFile = targetDir.resolve(Paths.get(rootConfigPath).getFileName());

        ConfigHelpers.copyConfigDir(sourceDir, targetDir);
        ConfigHelpers.setConfigVars(targetRootFile, substitutions);

        return targetRootFile.toUri().toURL();
    }

    public static void copyConfigDir(Path sourceDir, Path targetDir) throws IOException {

        try (var sourceFiles = Files.walk(sourceDir)) {

            sourceFiles
                .filter(sourceFile -> !sourceFile.equals(sourceDir))
                .forEach(sourceFile -> { try {

                    var targetFile = targetDir.resolve(sourceFile.getFileName());

                    if (Files.isDirectory(sourceFile)) {

                        Files.createDirectory(targetFile);
                        copyConfigDir(sourceFile, targetFile);
                    }
                    else {
                        Files.copy(sourceFile, targetFile);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });
        }
    }

    public static void setConfigVars(Path configFile, Map<String, String> substitutions) throws IOException {

        var configText = Files.readString(configFile);

        for (var sub : substitutions.entrySet())
            configText = configText.replace(sub.getKey(), sub.getValue());

        Files.writeString(configFile, configText);
    }
}
