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

package org.finos.tracdap.test.helpers;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class TestResourceHelpers {

    public static ByteString loadResourceAsByteString(String resourcePath) {

        return loadResourceAsByteString(resourcePath, TestResourceHelpers.class);
    }

    public static ByteString loadResourceAsByteString(String resourcePath, Class<?> clazz) {

        var bytes = loadResourceAsBytes(resourcePath, clazz);
        return ByteString.copyFrom(bytes);
    }

    public static String loadResourceAsString(String resourcePath) {

        return loadResourceAsString(resourcePath, TestResourceHelpers.class);
    }

    public static String loadResourceAsString(String resourcePath, Class<?> clazz) {

        var bytes = loadResourceAsBytes(resourcePath, clazz);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] loadResourceAsBytes(String resourcePath) {

        return loadResourceAsBytes(resourcePath, TestResourceHelpers.class);
    }

    public static byte[] loadResourceAsBytes(String resourcePath, Class<?> clazz) {

        try (var stream = clazz.getResourceAsStream(resourcePath)) {

            if (stream == null)
                throw new IOException("Failed to read resource: " + resourcePath);

            return stream.readAllBytes();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
