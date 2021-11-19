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

package com.accenture.trac.common.validation.fixed;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.validation.core.ValidationContext;
import com.accenture.trac.metadata.*;
import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.stream.Collectors;


public class SchemaValidator {

    private static final List<BasicType> ALLOWED_BUSINESS_KEY_TYPES = List.of(
            BasicType.STRING, BasicType.INTEGER, BasicType.DATE);

    private static final List<BasicType> ALLOWED_CATEGORICAL_TYPES = List.of(
            BasicType.STRING, BasicType.INTEGER);

    private static final Descriptors.Descriptor SCHEMA_DEFINITION;
    private static final Descriptors.FieldDescriptor SD_SCHEMA_TYPE;
    private static final Descriptors.OneofDescriptor SD_SCHEMA_TYPE_DEFINITION;
    private static final Descriptors.FieldDescriptor SD_TABLE;

    private static final Descriptors.Descriptor TABLE_SCHEMA;
    private static final Descriptors.FieldDescriptor TS_FIELDS;

    private static final Descriptors.Descriptor FIELD_SCHEMA;
    private static final Descriptors.FieldDescriptor FS_FIELD_NAME;
    private static final Descriptors.FieldDescriptor FS_FIELD_ORDER;
    private static final Descriptors.FieldDescriptor FS_FIELD_TYPE;


    static {

        SCHEMA_DEFINITION = SchemaDefinition.getDescriptor();
        SD_SCHEMA_TYPE = field(SCHEMA_DEFINITION, SchemaDefinition.SCHEMATYPE_FIELD_NUMBER);
        SD_TABLE = field(SCHEMA_DEFINITION, SchemaDefinition.TABLE_FIELD_NUMBER);
        SD_SCHEMA_TYPE_DEFINITION = SD_TABLE.getContainingOneof();

        TABLE_SCHEMA = TableSchema.getDescriptor();
        TS_FIELDS = field(TABLE_SCHEMA, TableSchema.FIELDS_FIELD_NUMBER);

        FIELD_SCHEMA = FieldSchema.getDescriptor();
        FS_FIELD_NAME = field(FIELD_SCHEMA, FieldSchema.FIELDNAME_FIELD_NUMBER);
        FS_FIELD_ORDER = field(FIELD_SCHEMA, FieldSchema.FIELDORDER_FIELD_NUMBER);
        FS_FIELD_TYPE = field(FIELD_SCHEMA, FieldSchema.FIELDTYPE_FIELD_NUMBER);
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
    }


    public static ValidationContext schema(SchemaDefinition schema, ValidationContext ctx) {

        ctx = ctx.push(SD_SCHEMA_TYPE)
                .apply(Validation::required)
                .apply(Validation::recognizedEnum, SchemaType.class)
                .pop();

        ctx = ctx.pushOneOf(SD_SCHEMA_TYPE_DEFINITION)
                .apply(Validation::required)
                .pop();

        if (schema.getSchemaType() == SchemaType.TABLE) {

            return ctx.push(SD_TABLE)
                    .apply(SchemaValidator::tableSchema, TableSchema.class)
                    .pop();
        }
        else {

            // TABLE is the only schema type available at present
            throw new EUnexpected();
        }
    }

    public static ValidationContext tableSchema(TableSchema table, ValidationContext ctx) {

        ctx = ctx.push(TS_FIELDS)
                .apply(Validation::listNotEmpty, List.class)
                .applyList(SchemaValidator::fieldSchema, FieldSchema.class)
                .pop();

        // Check for duplicate field names, including duplicates with different case

        var names = table.getFieldsList().stream()
                .map(FieldSchema::getFieldName)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        if (names.size() != table.getFieldsCount()) {

            var err = "Table schema contains duplicate field names";
            ctx = ctx.error(err);
        }

        // If fields orders are all zero, ordering will be inferred from the list order

        var allZeroOrder = table.getFieldsList().stream()
                .map(FieldSchema::getFieldOrder)
                .allMatch(order -> order == 0);

        if (!allZeroOrder) {

            // If field orders are specified, they must for a contiguous set of indices starting at zero
            // Fields may be specified out-of-order, so long as the entire set forms a contiguous range
            // I.e. 0, 1, 3, 4, 2 is allowed, but 0, 1, 3, 4, 5 is not

            var orders = table.getFieldsList().stream()
                    .map(FieldSchema::getFieldOrder)
                    .collect(Collectors.toSet());

            for (var fieldOrder = 0; fieldOrder < table.getFieldsCount(); fieldOrder++) {

                if (!orders.contains(fieldOrder)) {
                    var err = "Field orders must form a contiguous set of indices starting at zero";
                    ctx = ctx.error(err);
                }
            }
        }

        return ctx;
    }

    public static ValidationContext fieldSchema(FieldSchema field, ValidationContext ctx) {

        ctx = ctx.push(FS_FIELD_NAME)
                .apply(Validation::required)
                .apply(Validation::identifier)
                .apply(Validation::notTracReserved)
                .pop();

        // Do not apply Validation::required, which fails for zero-valued integers
        ctx = ctx.push(FS_FIELD_ORDER)
                .apply(Validation::notNegative, Integer.class)
                .pop();

        ctx = ctx.push(FS_FIELD_TYPE)
                .apply(Validation::required)
                .apply(Validation::recognizedEnum, BasicType.class)
                .apply(TypeSystemValidator::primitive, BasicType.class)
                .pop();

        if (field.getBusinessKey() && !ALLOWED_BUSINESS_KEY_TYPES.contains(field.getFieldType())) {

            var err = String.format("Schema field [%s] cannot be a business key because it has type [%s]",
                    ctx.fieldName(), field.getFieldType());

            ctx = ctx.error(err);
        }

        if (field.getCategorical() && !ALLOWED_CATEGORICAL_TYPES.contains(field.getFieldType())) {

            var err = String.format("Schema field [%s] cannot be categorical because it has type [%s]",
                    ctx.fieldName(), field.getFieldType());

            ctx = ctx.error(err);
        }

        // No validation applied to label or format code

        return ctx;
    }
}
