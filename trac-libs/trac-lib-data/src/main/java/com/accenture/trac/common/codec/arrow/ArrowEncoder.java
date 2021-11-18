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

import com.accenture.trac.common.codec.BaseEncoder;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

public abstract class ArrowEncoder extends BaseEncoder {

    // Common base encoder for both files and streams
    // Both receive a data stream and output a byte stream

    // Decoders do not share a common structure
    // This is because the file decoder requires random access to the byte stream

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;
    private VectorSchemaRoot root;
    private VectorLoader loader;
    private ArrowWriter writer;

    protected abstract ArrowWriter createWriter(VectorSchemaRoot root);

    public ArrowEncoder(BufferAllocator arrowAllocator) {

        this.arrowAllocator = arrowAllocator;
    }

    @Override
    protected void encodeSchema(Schema arrowSchema) {

        try {

            this.root = ArrowSchema.createRoot(arrowSchema, arrowAllocator);
            this.loader = new VectorLoader(root);
            this.writer = createWriter(root);

            writer.start();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);

            releaseEverything();

            throw new EUnexpected(e);
        }
    }

    @Override
    protected void encodeRecords(ArrowRecordBatch batch) {

        try (batch) {  // This will release the batch

            loader.load(batch);  // This retains data in the VSR, must be matched by root.clear()
            writer.writeBatch();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);

            releaseEverything();

            throw new EUnexpected(e);
        }
        finally {

            // Release data that was retained in VSR by the loader
            root.clear();
        }
    }

    @Override
    protected void encodeDictionary(ArrowDictionaryBatch batch) {

        throw new ETracInternal("Arrow stream dictionary encoding not supported");
    }

    @Override
    protected void encodeEos() {

        try {
            writer.end();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
        finally {

            releaseEverything();
        }
    }

    private void releaseEverything() {

        if (writer != null) {
            writer.close();
            writer = null;
        }

        if (root != null) {
            root.close();
            root = null;
        }
    }
}
