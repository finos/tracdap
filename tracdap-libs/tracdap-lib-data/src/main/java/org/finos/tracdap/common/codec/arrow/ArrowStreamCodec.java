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

package org.finos.tracdap.common.codec.arrow;

import org.finos.tracdap.common.codec.ICodec;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.memory.BufferAllocator;

import java.util.List;
import java.util.Map;


public class ArrowStreamCodec implements ICodec {

    private static final String DEFAULT_FILE_EXTENSION = "arrows";

    @Override
    public List<String> options() {
        return List.of();
    }

    @Override
    public String defaultFileExtension() {
        return DEFAULT_FILE_EXTENSION;
    }

    @Override
    public Encoder getEncoder(BufferAllocator arrowAllocator, Schema schema, Map<String, String> options) {
        return new ArrowStreamEncoder();
    }

    @Override
    public Decoder getDecoder(BufferAllocator arrowAllocator, Schema schema, Map<String, String> options) {
        return new ArrowStreamDecoder(arrowAllocator);
    }
}
