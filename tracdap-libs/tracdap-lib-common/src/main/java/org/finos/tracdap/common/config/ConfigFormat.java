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

package org.finos.tracdap.common.config;

import org.finos.tracdap.common.exception.EStartup;
import com.google.common.io.Files;

import java.net.URI;
import java.util.List;


/**
 * Enumeration of available config file formats, associated with standard file extensions
 */
public enum ConfigFormat {

    /** YAML format (text) **/
    YAML ("yaml", "yml"),

    /** JSON format (text) **/
    JSON ("json"),

    /** Protobuf format (binary) **/
    PROTO ("proto");

    ConfigFormat(String... extensions) {
        this.extensions = List.of(extensions);
    }

    private final List<String> extensions;

    /** Look up the format of a config file based on its URL **/
    public static ConfigFormat fromExtension(URI configFileUrl) {

        var path = configFileUrl.getPath();
        var ext = Files.getFileExtension(path);

        if (ext.isEmpty())
            throw new EStartup(String.format("Unknown config format for file: [%s]", configFileUrl));

        for (var format : ConfigFormat.values())
            if (format.extensions.contains(ext))
                return format;

        throw new EStartup(String.format("Unknown config format for file: [%s]", configFileUrl));
    }
}
