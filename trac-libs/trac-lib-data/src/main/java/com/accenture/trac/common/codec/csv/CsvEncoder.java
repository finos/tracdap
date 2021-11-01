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
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.function.Consumer;


public class CsvEncoder extends CommonBaseProcessor<VectorSchemaRoot, ByteBuf> {

    private final Consumer<VectorSchemaRoot> recycler;
    private final CsvMapper mapper = null;
    private CsvGenerator generator;
    //private final ByteBufOutputStream ostream;

    public CsvEncoder(Consumer<VectorSchemaRoot> recycler) {
        this.recycler = recycler;
        //ostream = new ByteBufOutputStream()
    }

    @Override
    protected void handleTargetRequest() {

    }

    @Override
    protected void handleSourceNext(VectorSchemaRoot item) {
        encodeBatch(item);
    }

    private void encodeBatch(VectorSchemaRoot batch) {

        try {

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
