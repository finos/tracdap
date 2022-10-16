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

import org.finos.tracdap.common.codec.BufferDecoder;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.util.ByteSeekableChannel;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.InvalidArrowFileException;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ArrowFileDecoder extends BufferDecoder {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;

    public ArrowFileDecoder(BufferAllocator arrowAllocator) {

        this.arrowAllocator = arrowAllocator;
    }

    @Override
    public void consumeBuffer(ByteBuf buffer) {

        if (buffer.readableBytes() == 0) {
            var error = new EDataCorruption("Arrow data file is empty");
            log.error(error.getMessage(), error);
            throw error;
        }

        try (var channel = new ByteSeekableChannel(buffer);

             var reader = new ArrowFileReader(channel, arrowAllocator);
             var root = reader.getVectorSchemaRoot()) {

            emitRoot(root);

            while (reader.loadNextBatch()) {

                emitBatch();
            }

            emitEnd();
        }
        catch (InvalidArrowFileException e) {

            // A nice clean validation failure from the Arrow framework
            // E.g. missing / incorrect magic number at the start (or end) of the file

            var errorMessage = "Arrow file decoding failed, file is invalid: " + e.getMessage();

            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException | IOException  e) {

            // These errors occur if the data stream contains bad values for vector sizes, offsets etc.
            // This may be as a result of a corrupt data stream, or a maliciously crafted message

            // Decoders work on a stream of buffers, "real" IO exceptions should not occur

            var errorMessage = "Arrow file decoding failed, content is garbled";

            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (Throwable e)  {

            // Ensure unexpected errors are still reported to the Flow API

            log.error("Unexpected error in Arrow file decoding", e);
            throw new EUnexpected(e);
        }
        finally {

            buffer.release();
        }
    }

    @Override
    public void close() {

        // No-op, no resources are held outside consumeBuffer()
    }
}
