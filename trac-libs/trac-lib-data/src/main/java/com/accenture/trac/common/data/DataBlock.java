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

package com.accenture.trac.common.data;

import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;


public class DataBlock implements AutoCloseable {

    public final Schema arrowSchema;
    public final ArrowRecordBatch arrowRecords;
    public final ArrowDictionaryBatch arrowDictionary;

    private DataBlock(
            Schema arrowSchema,
            ArrowRecordBatch arrowRecords,
            ArrowDictionaryBatch arrowDictionary) {

        this.arrowSchema = arrowSchema;
        this.arrowRecords = arrowRecords;
        this.arrowDictionary = arrowDictionary;
    }

    public static DataBlock forSchema(Schema arrowSchema) {
        return new DataBlock(arrowSchema, null, null);
    }

    public static DataBlock forRecords(ArrowRecordBatch arrowRecords) {
        return new DataBlock(null, arrowRecords, null);
    }

    public static DataBlock forDictionary(ArrowDictionaryBatch arrowDictionary) {
        return new DataBlock(null, null, arrowDictionary);
    }

    @Override
    public void close() {

        if (arrowRecords != null)
            arrowRecords.close();

        if (arrowDictionary != null)
            arrowDictionary.close();
    }
}
