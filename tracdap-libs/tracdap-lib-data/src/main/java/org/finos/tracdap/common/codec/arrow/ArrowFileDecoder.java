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

package org.finos.tracdap.common.codec.arrow;

import org.finos.tracdap.common.data.util.ByteSeekableChannel;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.List;


public class ArrowFileDecoder extends ArrowDecoder {

    private final BufferAllocator arrowAllocator;

    public ArrowFileDecoder(BufferAllocator arrowAllocator) {
        super(arrowAllocator);
        this.arrowAllocator = arrowAllocator;
    }

    @Override
    protected ArrowReader createReader(List<ArrowBuf> buffer) {
        var channel = new ByteSeekableChannel(buffer);
        return new ArrowFileReader(channel, arrowAllocator);
    }
}
