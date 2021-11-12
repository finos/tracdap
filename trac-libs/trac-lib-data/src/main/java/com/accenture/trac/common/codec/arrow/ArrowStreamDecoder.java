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
import com.accenture.trac.common.exception.EDataCorruption;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.util.ByteSeekableChannel;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ArrowStreamDecoder extends BaseDecoder {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;

    public ArrowStreamDecoder(BufferAllocator arrowAllocator) {

        super(BUFFERED_DECODER);

        this.arrowAllocator = arrowAllocator;
    }

    @Override
    protected void decodeStart() {
        // No-op, current version of CSV decode buffers the full input
    }

    @Override
    protected void decodeChunk(ByteBuf chunk) {

        try (var stream = new ByteSeekableChannel(chunk);
             var reader = new ArrowStreamReader(stream, arrowAllocator)) {

            var schema = reader.getVectorSchemaRoot().getSchema();
            emitBlock(DataBlock.forSchema(schema));

            var root = reader.getVectorSchemaRoot();
            var unloader = new VectorUnloader(root);

            while (reader.loadNextBatch()) {

                var batch = unloader.getRecordBatch();
                emitBlock(DataBlock.forRecords(batch));

                // Release memory retained in VSR (batch still has a reference)
                root.clear();
            }
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException | IOException  e) {

            // These errors occur if the data stream contains bad values for vector sizes, offsets etc.
            // This may be as a result of a corrupt data stream, or a maliciously crafted message

            // Decoders work on a stream of buffers, "real" IO exceptions should not occur

            var errorMessage = "Arrow file decoding failed, content is garbled";

            log.error(errorMessage, e);
            doTargetError(new EDataCorruption(errorMessage, e));
        }
        catch (Throwable e)  {

            // Ensure unexpected errors are still reported to the Flow API

            log.error("Unexpected error in Arrow stream decoding", e);
            doTargetError(new EUnexpected(e));
        }
        finally {

            chunk.release();
        }
    }

    @Override
    protected void decodeLastChunk() {

        // No-op, current version of arrow file decoder buffers the full input
    }
}
