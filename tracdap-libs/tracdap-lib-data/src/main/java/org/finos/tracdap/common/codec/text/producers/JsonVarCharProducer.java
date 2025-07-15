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

package org.finos.tracdap.common.codec.text.producers;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.arrow.vector.VarCharVector;

import java.io.IOException;


public class JsonVarCharProducer extends BaseJsonProducer<VarCharVector> {

    // Avoid reallocating buffer for each cell
    private static final int INITIAL_BUFFER_SIZE = 64;
    private byte[] buffer;

    public JsonVarCharProducer(VarCharVector vector) {
        super(vector);
        buffer = new byte[INITIAL_BUFFER_SIZE];
    }

    @Override
    public void produceElement(JsonGenerator generator) throws IOException {

        int start = vector.getStartOffset(currentIndex);
        int end = vector.getEndOffset(currentIndex);
        int length = end - start;

        // JSON generator does not accept ByteBuffer, it requires data in a byte[]
        // The alternative is using a Reader and processing the encoding (unnecessary, Arrow is already UTF-8)
        // So one copy operation is required, but the same buffer can be recycled for each value

        if (length > buffer.length)
            buffer = new byte[length];

        vector.getDataBuffer().getBytes(start, buffer, 0, length);
        generator.writeUTF8String(buffer, 0, length);

        currentIndex++;
    }
}
