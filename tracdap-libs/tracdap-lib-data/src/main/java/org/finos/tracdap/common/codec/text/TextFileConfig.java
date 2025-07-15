/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.codec.text;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;


public class TextFileConfig {

    private final JsonFactory jsonFactory;
    private final FormatSchema formatSchema;

    private final int batchSize;
    private final boolean singleRecord;

    public TextFileConfig(JsonFactory jsonFactory, int batchSize) {
        this(jsonFactory, null, batchSize, false);
    }

    public TextFileConfig(
            JsonFactory jsonFactory,
            FormatSchema formatSchema,
            int batchSize,
            boolean singleRecord) {

        this.jsonFactory = jsonFactory;
        this.formatSchema = formatSchema;
        this.batchSize = batchSize;
        this.singleRecord = singleRecord;
    }

    public JsonFactory getJsonFactory() {
        return jsonFactory;
    }

    public boolean hasFormatSchema() {
        return formatSchema != null;
    }

    public FormatSchema getFormatSchema() {
        return formatSchema;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isSingleRecord() {
        return singleRecord;
    }
}
