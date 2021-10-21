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

package com.accenture.trac.plugins.config.gcp;

import com.accenture.trac.common.config.IConfigLoader;
import com.accenture.trac.common.exception.EStartup;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.BufferedReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;


/**
 * A config loader implementation for loading from AWS S3.
 */
public class GcpConfigLoader implements IConfigLoader {

    @Override
    public String loaderName() {
        return "GCP Storage";
    }

    @Override
    public List<String> protocols() {
        return List.of("gcp");
    }

    @Override
    public String loadTextFile(URI uri) {

        var ERROR_MSG_TEMPLATE = "Failed to load config file from GCP: %2$s [%1$s]";

        String path = uri.getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        StorageOptions options = StorageOptions.newBuilder().build();
        Storage storage = options.getService();
        Blob blob = storage.get(uri.getHost(), path);
        if (blob == null) {
            var message = String.format("Cannot load config from GCP: [%s]. File does not exist.", uri);
            throw new EStartup(message);
        }
        ReadChannel readChannel = blob.reader();
        BufferedReader br = new BufferedReader(Channels.newReader(readChannel, StandardCharsets.UTF_8));

        String output;
        try {
            output = br.lines().collect(Collectors.joining());
            return output;
        } catch (IllegalArgumentException | UncheckedIOException e) {

            var message = String.format(ERROR_MSG_TEMPLATE, path, e.getMessage());
            throw new EStartup(message, e);
        }
    }
}
