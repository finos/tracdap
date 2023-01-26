/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.webserver;

import org.finos.tracdap.common.exception.EStartup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class MimeTypes {

    private static final String MIME_TYPES_FILE = "mime.types";

    public static Map<String, String> loadMimeTypeMap() {

        try {

            var loader = MimeTypes.class.getClassLoader();

            try (var stream = loader.getResourceAsStream(MIME_TYPES_FILE)) {

                if (stream == null)
                    throw new EStartup("Failed to load mime types: [mime.types] resource could not be opened");

                var reader = new BufferedReader(new InputStreamReader(stream));

                var lines = reader.lines()
                        .map(String::strip)
                        .filter(line -> !(line.isEmpty() || line.startsWith("#")))
                        .map(line -> line.split("\\s+"))
                        .collect(Collectors.toList());

                var mapping = new HashMap<String, String>();

                for (var line : lines) {

                    if (line.length < 2)
                        throw new EStartup("Invalid entry in mime types" );

                    var mimeType = line[0];

                    for (var i = 1; i < line.length; i++) {
                        var extension = line[i];
                        mapping.put(extension, mimeType);
                    }
                }

                return mapping;
            }

        }
        catch (IOException e) {
            throw new EStartup("Failed to load mime types: " + e.getMessage(), e);
        }
    }
}
