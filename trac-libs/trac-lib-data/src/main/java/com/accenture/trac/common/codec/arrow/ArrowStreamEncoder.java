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
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.util.ByteOutputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;


public class ArrowStreamEncoder extends BaseEncoder {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private BufferAllocator arrowAllocator;
    private VectorSchemaRoot root;
    private VectorLoader loader;
    private ArrowStreamWriter writer;

    public ArrowStreamEncoder(BufferAllocator arrowAllocator) {

        this.arrowAllocator = arrowAllocator;
    }

    @Override
    protected void encodeSchema(Schema arrowSchema) {

        createRoot(arrowSchema);
    }

    void createRoot(Schema arrowSchema) {

        var fields = arrowSchema.getFields();
        var vectors = new ArrayList<FieldVector>(fields.size());

        for (var field : fields)
            vectors.add(field.createVector(arrowAllocator));

        this.root = new VectorSchemaRoot(fields, vectors);
        this.loader = new VectorLoader(root);  // TODO: No compression support atm

        var out = new ByteOutputStream(outQueue::add);
        this.writer = new ArrowStreamWriter(root, /* dictionary provider = */ null, out);

        try {
            writer.start();
        }
        catch (IOException e) {

            // todo
            log.error(e.getMessage(), e);
            throw new ETracInternal(e.getMessage(), e);
        }
    }

    @Override
    protected void encodeRecords(ArrowRecordBatch batch) {

        try (batch) {  // This will release the batch

            loader.load(batch);  // This retains data in the VSR, must be matched by root.clear()
            writer.writeBatch();
        }
        catch (IOException e) {

            // todo
            log.error(e.getMessage(), e);
            throw new ETracInternal(e.getMessage(), e);
        }
        finally {

            root.clear();  // Release data that was retained in VSR by the loader
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

            // todo
            log.error(e.getMessage(), e);
            throw new ETracInternal(e.getMessage(), e);
        }
        finally {
            writer.close();
        }
    }
}
