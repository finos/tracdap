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

package org.finos.tracdap.common.codec.text.consumers;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.arrow.vector.VarCharVector;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class JsonVarCharConsumer extends BaseJsonConsumer<VarCharVector> {

    private final BufferStream buffer;
    private final Writer bufferWriter;

    public JsonVarCharConsumer(VarCharVector vector) {
        super(vector);
        this.buffer = new BufferStream();
        this.bufferWriter = new OutputStreamWriter(buffer, StandardCharsets.UTF_8);
    }

    @Override
    public boolean consumeElement(JsonParser parser) throws IOException {

        // Using the buffer writer avoids the need for allocation on each cell
        // Bytes are written directly from the Jackson internal buffer to the writer buffer
        // The buffer is only reallocated up to the maximum size needed for any one element

        buffer.reset();

        parser.getText(bufferWriter);
        bufferWriter.flush();

        byte[] buf = buffer.buffer();
        int length = buffer.size();

        // For variable width vectors, the required size of the content buffer is not known up front
        // Arrow makes an initial guess, but sometimes it will need to reallocate on write
        // So, we need to call setSafe() instead of set(), to avoid a buffer overflow

        vector.setSafe(currentIndex++, buf, 0, length);

        return true;
    }

    private static class BufferStream extends ByteArrayOutputStream {

        byte[] buffer() {
            return buf;
        }
    }
}
