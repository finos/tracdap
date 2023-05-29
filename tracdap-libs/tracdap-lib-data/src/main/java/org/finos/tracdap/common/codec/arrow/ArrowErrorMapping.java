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

package org.finos.tracdap.common.codec.arrow;

import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.ETrac;
import org.finos.tracdap.common.exception.EUnexpected;

import org.apache.arrow.vector.ipc.InvalidArrowFileException;
import java.io.IOException;


class ArrowErrorMapping {

    static ETrac mapDecodingError(Throwable e) {

        if (e instanceof ETrac)
            return (ETrac) e;

        // A nice clean validation failure from the Arrow framework
        // E.g. missing / incorrect magic number at the start (or end) of the file

        if (e instanceof InvalidArrowFileException) {
            var errorMessage = "Arrow decoding failed, file is invalid: " + e.getMessage();
            return new EDataCorruption(errorMessage, e);
        }

        // These errors occur if the data stream contains bad values for vector sizes, offsets etc.
        // This may be as a result of a corrupt data stream, or a maliciously crafted message

        if (e instanceof IllegalArgumentException || e instanceof IndexOutOfBoundsException) {

            var errorMessage = "Arrow decoding failed, content is garbled";
            return new EDataCorruption(errorMessage, e);
        }

        // Decoders work on a stream of buffers, "real" IO exceptions should not occur

        if (e instanceof IOException) {

            var errorMessage = "Arrow decoding failed, there was an error processing the data stream";
            throw new EDataCorruption(errorMessage, e);
        }

        // Catch all

        throw new EUnexpected(e);
    }
}
