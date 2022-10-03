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

package org.finos.tracdap.common.config.local;

import org.finos.tracdap.common.config.IConfigLoader;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETracInternal;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;


/**
 * A config loader implementation for loading from the local filesystem.
 */
public class LocalConfigLoader implements IConfigLoader {

    @Override
    public String loadTextFile(URI url) {

        var bytes = loadBinaryFile(url);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] loadBinaryFile(URI uri) {

        var ERROR_MSG_TEMPLATE = "Failed to load config file: %2$s [%1$s]";

        Path path = null;

        try {

            path = Paths.get(uri);
            return Files.readAllBytes(path);
        }
        catch (IllegalArgumentException e) {

            // This should not happen
            // ConfigManager should only pass file URLs to this loader
            // Treat this as an internal error

            var message = String.format("URL is not a file path: [%s]", uri);
            throw new ETracInternal(message, e);
        }
        catch (NoSuchFileException e) {

            var message = String.format(ERROR_MSG_TEMPLATE, path, "File does not exist");
            throw new EStartup(message, e);
        }
        catch (AccessDeniedException e) {

            var message = String.format(ERROR_MSG_TEMPLATE, path, "Access denied");
            throw new EStartup(message, e);
        }
        catch (IOException e) {

            var message = String.format(ERROR_MSG_TEMPLATE, path, e.getMessage());
            throw new EStartup(message, e);
        }
    }
}
