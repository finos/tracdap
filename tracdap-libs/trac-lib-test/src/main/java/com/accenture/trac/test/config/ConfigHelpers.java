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

import com.accenture.trac.common.exception.EUnexpected;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.*;
import java.util.Map;
import java.util.jar.JarFile;


public class ConfigHelpers {

    private static final String BACKSLASH = "/";

    public static URL prepareConfig(
            String rootConfigFile, Path targetDir,
            Map<String, String> substitutions)
            throws Exception {

        // URL of config file resource in JAR or on file system

        var rootConfigUrl = ConfigHelpers.class
                .getClassLoader()
                .getResource(rootConfigFile);

        if (rootConfigUrl == null) {
            var err = String.format("Config resource not found: [%s]", rootConfigFile);
            throw new RuntimeException(err);
        }

        // Root item to copy - can be a directory

        var rootConfigDir = rootConfigFile.contains("/")
                ? rootConfigFile.substring(0, rootConfigFile.lastIndexOf("/") + 1)
                : rootConfigFile;

        var pathSeparator = FileSystems.getDefault().getSeparator();

        // Copy config resources, either from JAR or FS depending on test configuration

        if (rootConfigUrl.getProtocol().equals("jar")) {

            var resourcePath = ConfigHelpers.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .getPath();

            if (resourcePath.startsWith(BACKSLASH) && !BACKSLASH.equals(pathSeparator))
                resourcePath = resourcePath.substring(1);

            ConfigHelpers.copyConfigFromJar(resourcePath, rootConfigDir, targetDir);

        }
        else {

            var rootConfigPath = rootConfigUrl.getPath();

            if (rootConfigPath.startsWith(BACKSLASH) && !BACKSLASH.equals(pathSeparator))
                rootConfigPath = rootConfigPath.substring(1);

            var sourceDir = Paths.get(rootConfigPath).getParent();
            var targetConfigDir = targetDir.resolve(rootConfigDir);
            Files.createDirectories(targetConfigDir);
            ConfigHelpers.copyConfigDir(sourceDir, targetConfigDir);
        }

        // Apply config substitutions

        var targetRootFile = targetDir.resolve(rootConfigFile);
        ConfigHelpers.setConfigVars(targetRootFile, substitutions);

        // URL of the root file in the target location

        return targetRootFile.toUri().toURL();
    }

    public static void copyConfigFromJar(String jarPath, String rootConfigPath, Path targetDir) throws IOException {

        try (var jar = new JarFile(jarPath)) {
        for (var entries = jar.entries(); entries.hasMoreElements(); ) {

            var entry = entries.nextElement();
            var name = entry.getName();

            if (!name.startsWith(rootConfigPath))
                continue;

            if (entry.isDirectory())
                Files.createDirectory(targetDir.resolve(name));

            else {
                try (var stream = jar.getInputStream(entry)) {

                    var size = entry.getSize();
                    var bytes = new byte[(int) size];
                    var sizeRead = stream.read(bytes);

                    if (sizeRead != size)
                        throw new EUnexpected();

                    Files.write(targetDir.resolve(name), bytes);
                }
            }

        } }
    }


    public static void copyConfigDir(Path sourceDir, Path targetDir) throws IOException {

        try (var sourceFiles = Files.walk(sourceDir)) {

            sourceFiles

                // Do not try to create the root of targetDir, which already exists
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
