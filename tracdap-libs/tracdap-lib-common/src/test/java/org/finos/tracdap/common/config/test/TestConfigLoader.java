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

package org.finos.tracdap.common.config.test;

import org.finos.tracdap.common.config.IConfigLoader;
import org.finos.tracdap.common.exception.EStartup;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;


public class TestConfigLoader implements IConfigLoader {

    private final Path tempDir;

    public TestConfigLoader(Path tempDir) {
        this.tempDir = tempDir;
    }

    @Override
    public byte[] loadBinaryFile(URI uri) {

        var relativePath = uri.getPath().substring(1);  // Ignore leading slash on path component
        var absolutePath = tempDir.resolve(relativePath);

        try {
            return Files.readAllBytes(absolutePath);
        }
        catch (IOException e) {

            throw new EStartup("Config file could not be read: " + e.getMessage(), e);
        }
    }

    @Override
    public String loadTextFile(URI uri) {

        var bytes = loadBinaryFile(uri);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
