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

import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Describe the full schema information needed to understand a VSR.
 *
 * <p>The physical schema describes what is held directly in the VSR.
 * The decoded schema describes any dictionary-encoded fields with their original data type.
 */
public class ArrowVsrSchema {

    private final Schema physicalSchema;
    private final Map<Long, Field> dictionaryFields;
    private final DictionaryProvider dictionaries;
    private final boolean singleRecord;

    private final Schema logicalSchema;

    public ArrowVsrSchema(Schema physicalSchema) {
        // Create empty dictionary maps
        this(physicalSchema, new HashMap<>(), new DictionaryProvider.MapDictionaryProvider(), false);
    }

    public ArrowVsrSchema(Schema physicalSchema, DictionaryProvider dictionaries) {

        this(physicalSchema, buildDictionaryFields(dictionaries), dictionaries, checkSingleRecord(physicalSchema));
    }

    public ArrowVsrSchema(Schema physicalSchema, Map<Long, Field> dictionaryFields, DictionaryProvider dictionaries) {

        this(physicalSchema, dictionaryFields, dictionaries, checkSingleRecord(physicalSchema));
    }

    public ArrowVsrSchema(
            Schema physicalSchema,
            Map<Long, Field> dictionaryFields,
            DictionaryProvider dictionaries,
            boolean singleRecord) {

        this.physicalSchema = physicalSchema;
        this.dictionaryFields = dictionaryFields;
        this.dictionaries = dictionaries;
        this.singleRecord = singleRecord;

        this.logicalSchema = buildLogicalSchema(physicalSchema);
    }

    private static boolean checkSingleRecord(Schema physicalSchema) {

        var schemaMetadata = physicalSchema.getCustomMetadata();

        if (schemaMetadata == null)
            return false;

        var singleRecordFlag = schemaMetadata.get("trac.schema.singleRecord");

        if (singleRecordFlag == null)
            return false;

        return Boolean.TRUE.toString().equalsIgnoreCase(singleRecordFlag);
    }

    public Schema physical() {
        return physicalSchema;
    }

    public Map<Long, Field> dictionaryFields() {
        return dictionaryFields;
    }

    public DictionaryProvider dictionaries() {
        return dictionaries;
    }

    public boolean isSingleRecord() {
        return singleRecord;
    }

    public Schema logical() {
        return logicalSchema != null ? logicalSchema : physicalSchema;
    }

    private static Map<Long, Field> buildDictionaryFields(DictionaryProvider dictionaries) {

        var dictionaryFields = new HashMap<Long, Field>();

        if (dictionaries != null) {
            for (var dictionaryId : dictionaries.getDictionaryIds()) {
                var dictionary = dictionaries.lookup(dictionaryId);
                dictionaryFields.put(dictionaryId, dictionary.getVector().getField());
            }
        }

        return dictionaryFields;
    }

    private Schema buildLogicalSchema(Schema physicalSchema) {

        var logicalFields = new ArrayList<Field>();

        for (int i = 0; i < physicalSchema.getFields().size(); i++) {
            var logicalField = buildLogicalField(physicalSchema.getFields().get(i));
            logicalFields.add(logicalField);
        }

        return new Schema(logicalFields, physicalSchema.getCustomMetadata());
    }

    private Field buildLogicalField(Field physicalField) {

        if (physicalField.getDictionary() != null) {

            var dictionaryId = physicalField.getDictionary().getId();
            var dictionaryField = dictionaryFields.get(dictionaryId);

            // Take name and nullability from the physical field
            // These are not encoded in the dictionary field type, which can be shared between fields

            var fieldType = new FieldType(physicalField.isNullable(), dictionaryField.getType(), null);
            return new Field(physicalField.getName(), fieldType, dictionaryField.getChildren());
        }

        if (physicalField.getChildren() != null) {

            var logicalChildren = physicalField.getChildren()
                    .stream()
                    .map(this::buildLogicalField)
                    .collect(Collectors.toList());

            for (int i = 0; i < logicalChildren.size(); i++) {

                var physicalChild = physicalField.getChildren().get(i);
                var logicalChild = logicalChildren.get(i);

                if (logicalChild != physicalChild) {

                    return new Field(
                            physicalField.getName(),
                            physicalField.getFieldType(),
                            logicalChildren);
                }
            }
        }

        return physicalField;
    }
}
