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
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;


public class ArrowStreamDecoder extends BufferDecoder {

    // Safeguard max allowed size for first (schema) message - 16 MiB should be ample
    private static final int CONTINUATION_MARKER = 0xffffffff;
    private static final int MAX_FIRST_MESSAGE_SIZE = 16 * 1024 * 1024;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;

    public ArrowStreamDecoder(BufferAllocator arrowAllocator) {

        this.arrowAllocator = arrowAllocator;
    }

    @Override
    public void consumeBuffer(ByteBuf buffer) {

        if (buffer.readableBytes() == 0) {
            var error = new EDataCorruption("Arrow data stream is empty");
            log.error(error.getMessage(), error);
            throw error;
        }

        try (var channel = new ByteSeekableChannel(buffer)) {

            // Arrow does not attempt to validate the stream before reading
            // This quick validation peeks at the start of the stream for a basic sanity check
            // It should be enough to flag e.g. if data has been sent in a totally different format

            // Make sure to do this check before setting up reader + root,
            // since that will trigger reading the initial schema message

            validateStartOfStream(channel);

            try (var reader = new ArrowStreamReader(channel, arrowAllocator);
                 var root = reader.getVectorSchemaRoot()){

                emitRoot(root);

                while (reader.loadNextBatch()) {

                    emitBatch();
                }

                emitEnd();
            }
        }
        catch (NotAnArrowStream e) {

            // A nice clean validation exception

            var errorMessage = "Arrow stream decoding failed, content does not look like an Arrow stream";

            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException | IOException  e) {

            // These errors occur if the data stream contains bad values for vector sizes, offsets etc.
            // This may be as a result of a corrupt data stream, or a maliciously crafted message

            // Decoders work on a stream of buffers, "real" IO exceptions should not occur

            var errorMessage = "Arrow stream decoding failed, content is garbled";
            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (Throwable e)  {

            // Ensure unexpected errors are still reported to the Flow API
            log.error("Unexpected error in Arrow stream decoding", e);
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

    private void validateStartOfStream(SeekableByteChannel channel) throws IOException {

        // https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format

        // Arrow streams are a series of messages followed by optional body data
        // Each message is preceded by a continuation marker and message size
        // Just sanity checking these two values should catch some serious decode failures
        // E.g. if a data stream contains a totally different format

        // Record the stream position, so it can be restored after the validation
        long pos = channel.position();

        var headerBytes = new byte[8];
        var headerBuf = ByteBuffer.wrap(headerBytes);
        headerBuf.order(ByteOrder.LITTLE_ENDIAN);

        channel.position(0);

        var prefaceLength = channel.read(headerBuf);
        var continuation = headerBuf.getInt(0);
        var messageSize = headerBuf.getInt(4);

        if (prefaceLength != 8 || continuation != CONTINUATION_MARKER ||
            messageSize <=0 || messageSize > MAX_FIRST_MESSAGE_SIZE)

            throw new NotAnArrowStream();

        // Restore original stream position
        channel.position(pos);
    }

    private static class NotAnArrowStream extends RuntimeException { }
}
