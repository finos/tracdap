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

package org.finos.tracdap.plugins.gcp.config;

import org.finos.tracdap.common.config.IConfigLoader;
import org.finos.tracdap.common.exception.EStartup;

import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageException;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


/**
 * A config loader implementation for loading files from GCP storage
 */
public class GcpConfigLoader implements IConfigLoader {

    private static final String ERROR_MSG_TEMPLATE = "Failed to load config file from GCP: %2$s [%1$s]";

    @Override
    public byte[] loadBinaryFile(URI uri) {

        try {

            var path = uri.getPath().startsWith("/")
                    ? uri.getPath().substring(1)
                    : uri.getPath();

            var storageOptions = StorageOptions.newBuilder().build();
            var storage = storageOptions.getService();

            var blob = storage.get(uri.getHost(), path);

            if (blob == null) {
                var message = String.format(ERROR_MSG_TEMPLATE, uri, "File does not exist");
                throw new EStartup(message);
            }

            try (var channel = blob.reader()) {

                var size = (int)(long) blob.getSize();
                var buffer = ByteBuffer.allocate(size);

                var bytesRead = 0;
                var totalBytesRead = 0;

                do {
                    bytesRead = channel.read(buffer);
                    totalBytesRead += bytesRead;
                }
                while (bytesRead >= 0);

                if (totalBytesRead != size) {
                    var message = String.format(ERROR_MSG_TEMPLATE, uri, "File does not match reported size");
                    throw new EStartup(message);
                }

                return buffer.array();
            }
        }
        catch (StorageException | IOException e) {

            var message = String.format(ERROR_MSG_TEMPLATE, uri, e.getMessage());
            throw new EStartup(message, e);
        }
    }

    @Override
    public String loadTextFile(URI uri) {

        var bytes = loadBinaryFile(uri);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
