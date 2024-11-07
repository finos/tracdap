/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.util;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.MissingResourceException;


public class ResourceHelpers {

    public static ByteString loadResourceAsByteString(String resourcePath) {

        return loadResourceAsByteString(resourcePath, ResourceHelpers.class);
    }

    public static ByteString loadResourceAsByteString(String resourcePath, Class<?> clazz) {

        var bytes = loadResourceAsBytes(resourcePath, clazz);
        return ByteString.copyFrom(bytes);
    }

    public static String loadResourceAsString(String resourcePath, Class<?> clazz) {

        var bytes = loadResourceAsBytes(resourcePath, clazz);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] loadResourceAsBytes(String resourcePath) {

        return loadResourceAsBytes(resourcePath, ResourceHelpers.class);
    }

    public static byte[] loadResourceAsBytes(String resourcePath, Class<?> clazz) {

        try (var stream = clazz.getResourceAsStream(resourcePath)) {

            if (stream == null)
                throw new IOException("Failed to load resource: [" + resourcePath + "]");

            return stream.readAllBytes();
        }
        catch (IOException e) {
            throw new MissingResourceException(e.getMessage(), clazz.getName(), resourcePath);
        }
    }
}
