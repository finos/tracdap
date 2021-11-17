/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.codec.arrow;

import com.accenture.trac.common.codec.ICodec;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.data.IDataContext;
import com.accenture.trac.metadata.SchemaDefinition;
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
    public Encoder getEncoder(BufferAllocator arrowAllocator, SchemaDefinition schema, Map<String, String> options) {
        return new ArrowStreamEncoder(arrowAllocator);
    }

    @Override
    public Decoder getDecoder(BufferAllocator arrowAllocator, SchemaDefinition schema, Map<String, String> options) {
        return new ArrowStreamDecoder(arrowAllocator);
    }
}
