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

package com.accenture.trac.common.codec.csv;

import com.accenture.trac.common.concurrent.flow.CommonBaseProcessor;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.metadata.SchemaDefinition;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Consumer;


public class CsvEncoder extends CommonBaseProcessor<DataBlock, ByteBuf> {

    private final SchemaDefinition schema;
    private VectorSchemaRoot root;
    private VectorLoader loader;

    private final CsvMapper mapper = null;
    private CsvGenerator generator;
    //private final ByteBufOutputStream ostream;

    public CsvEncoder(SchemaDefinition schema) {

        var allocator = new RootAllocator();

        this.schema = schema;

        var arrowSchema = (Schema) null;
        var fields = arrowSchema.getFields();
        var vectors = new ArrayList<FieldVector>(fields.size());

        for (var field : fields)
            vectors.add(field.createVector(allocator));

        this.root = new VectorSchemaRoot(fields, vectors);
        this.loader = new VectorLoader(root);  // TODO: No compression support atm
    }

    @Override
    protected void handleTargetRequest() {

    }

    @Override
    protected void handleSourceNext(DataBlock block) {

        if (block.arrowRecords != null)
            encodeBatch(block.arrowRecords);
    }

    private void encodeHeader() {

    }

    private void encodeBatch(ArrowRecordBatch batch) {

        try {

            loader.load(batch);

            var nRows = batch.getRowCount();
            var nCols = batch.getFieldVectors().size();

            for (int i = 0; i < nRows; i++) {

                generator.writeStartArray();

                for (int j = 0; j < nCols; j++) {

                    // TODO: Type mapping

                    var value = batch.getVector(j).getObject(i);
                    generator.writeString(value.toString());
                }

                generator.writeEndArray();
            }
        }
        catch (IOException e) {

            // TODO: Error
        }
        finally {

            recycler.accept(batch);
        }
    }
}
