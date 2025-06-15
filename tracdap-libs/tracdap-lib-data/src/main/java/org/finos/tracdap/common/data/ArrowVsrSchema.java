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

package org.finos.tracdap.common.data;

import org.apache.arrow.vector.types.pojo.*;


/**
 * Describe the full schema information needed to understand a VSR.
 *
 * <p>The physical schema describes what is held directly in the VSR.
 * The decoded schema describes any dictionary-encoded fields with their original data type.
 */
public class ArrowVsrSchema {

    private final Schema physicalSchema;
    private final Schema decodedSchema;

    public ArrowVsrSchema(Schema physicalSchema) {
        this.physicalSchema = physicalSchema;
        this.decodedSchema = physicalSchema;
    }

    public ArrowVsrSchema(Schema physicalSchema, Schema decodedSchema) {
        this.physicalSchema = physicalSchema;
        this.decodedSchema = decodedSchema;
    }

    public Schema physical() {
        return physicalSchema;
    }

    public Schema decoded() {
        return decodedSchema != null ? decodedSchema : physicalSchema;
    }
}
