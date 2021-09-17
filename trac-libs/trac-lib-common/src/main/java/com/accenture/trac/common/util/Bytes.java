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

package com.accenture.trac.common.util;

import com.accenture.trac.common.exception.EUnexpected;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;


public class Bytes {

    public static ByteString toProtoBytes(ByteBuf buf) {

        if (buf.nioBufferCount() == 1) {

            var nioBuffer = buf.nioBuffer();
            return UnsafeByteOperations.unsafeWrap(nioBuffer);
        }

        if (buf.nioBufferCount() > 1) {

            var nioBuffers = buf.nioBuffers();

            var byteStream = Arrays.stream(nioBuffers)
                    .map(UnsafeByteOperations::unsafeWrap)
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
}
