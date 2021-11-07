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

import com.accenture.trac.common.codec.BaseDecoder;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.util.ByteSeekableChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ArrowFileDecoder extends BaseDecoder {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;

    public ArrowFileDecoder(BufferAllocator arrowAllocator) {

        this.arrowAllocator = arrowAllocator;
    }

    @Override
    protected void decodeChunk() {

    }

    @Override
    protected void decodeLastChunk() {

        try (var stream = new ByteSeekableChannel(buffer);
             var reader = new ArrowFileReader(stream, arrowAllocator)) {

            var schema = reader.getVectorSchemaRoot().getSchema();
            outQueue.add(DataBlock.forSchema(schema));

            var root = reader.getVectorSchemaRoot();
            var unloader = new VectorUnloader(root);

            while (reader.loadNextBatch()) {

                var batch = unloader.getRecordBatch();
                outQueue.add(DataBlock.forRecords(batch));
            }
        }
        catch (IOException e) {

            // todo
            log.error(e.getMessage(), e);
            throw new ETracInternal(e.getMessage(), e);
        }

    }
}
