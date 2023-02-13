/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.data.util;

import org.finos.tracdap.common.exception.EUnexpected;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;


public class Bytes {

    // Currently, it is not possible to do zero-copy between Netty and gRPC
    // https://github.com/grpc/grpc-java/issues/1054

    // Incoming data has already been copied to create the ByteString
    // Outgoing data needs to be copied before giving to gRPC
    // Because writing to the wire is asynchronous and there is no way to callback for release

    public static ByteBuf fromProtoBytes(ByteString bs) {

        return Unpooled.wrappedBuffer(bs.asReadOnlyByteBuffer());
    }

    public static ByteString toProtoBytes(ByteBuf buf) {

        try {

            if (buf.nioBufferCount() == 1) {

                var nioBuffer = buf.nioBuffer();
                return ByteString.copyFrom(nioBuffer);
            }

            if (buf.nioBufferCount() > 1) {

                var nioBuffers = buf.nioBuffers();

                var byteStream = Arrays.stream(nioBuffers)
                        .map(ByteString::copyFrom)
                        .reduce(ByteString::concat);

                if (byteStream.isEmpty())
                    throw new EUnexpected();

                return byteStream.get();
            }

            if (buf.hasArray()) {

                return UnsafeByteOperations.unsafeWrap(buf.array());
            }

            var bufCopy = new byte[buf.readableBytes()];
            buf.readBytes(bufCopy);

            return UnsafeByteOperations.unsafeWrap(bufCopy);
        }
        finally {
            buf.release();
        }
    }
}
