/*
 * Copyright 2020 Accenture Global Solutions Limited
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

import java.nio.file.Path;


/**
 * The standard arguments needed to initialise a ConfigManager
 *
 * @see StandardArgsProcessor
 * @see ConfigManager
 */
public class StandardArgs {

    private final Path workingDir;
    private final String configFile;
    private final String keystoreKey;

    public StandardArgs(Path workingDir, String configFile, String keystoreKey) {
        this.workingDir = workingDir;
        this.configFile = configFile;
        this.keystoreKey = keystoreKey;
    }

    public Path getWorkingDir() {
        return workingDir;
    }

    public String getConfigFile() {
        return configFile;
    }

    public String getKeystoreKey() {
        return keystoreKey;
    }
}
