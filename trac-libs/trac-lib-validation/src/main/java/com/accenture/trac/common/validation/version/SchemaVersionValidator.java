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

package com.accenture.trac.common.validation.version;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.validation.core.ValidationContext;
import com.accenture.trac.common.validation.fixed.SchemaValidator;
import com.accenture.trac.metadata.*;
import com.google.protobuf.Descriptors;

public class SchemaVersionValidator {

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

    public static ValidationContext schema(SchemaDefinition current, SchemaDefinition prior, ValidationContext ctx) {

        ctx = ctx.push(SD_SCHEMA_TYPE)
                .apply(CommonValidators::exactMatch, SchemaType.class)
                .pop();

        if (current.getSchemaType() == SchemaType.TABLE) {

            return ctx.push(SD_TABLE)
                    .apply(SchemaVersionValidator::tableSchema, TableSchema.class)
                    .pop();
        }
        else {

            // TABLE is the only schema type available at present
            throw new EUnexpected();
        }
    }

    public static ValidationContext tableSchema(TableSchema current, TableSchema prior, ValidationContext ctx) {

        // todo
        return ctx;
    }

    public static ValidationContext existingField(FieldSchema current, FieldSchema prior, ValidationContext ctx) {

        // This is a fairly strict field comparison, which insists on exact match
        // for field name case and field order. It may be possible to relax these
        // restrictions, if TRAC can provide an easy way to handle data when these
        // things change.

        // Field label and format code are allowed to change freely between schema versions.

        // Fields are matched by name before comparing
        if (!current.getFieldName().equalsIgnoreCase(prior.getFieldName()))
            throw new EUnexpected();

        if (!current.getFieldName().equals(prior.getFieldName())) {

            ctx = ctx.error(String.format(
                    "Field name case changed for [%s]: prior = [%s], new = [%s]",
                    prior.getFieldName(), prior.getFieldName(), current.getFieldName()));
        }

        if (current.getFieldOrder() != prior.getFieldOrder()) {

            ctx = ctx.error(String.format(
                    "Field order changed for [%s]: prior = [%d], new = [%d]",
                    prior.getFieldName(), prior.getFieldOrder(), current.getFieldOrder()));
        }

        if (current.getFieldType() != prior.getFieldType()) {

            ctx = ctx.error(String.format(
                    "Field type changed for [%s]: prior = [%s], new = [%s]",
                    prior.getFieldName(), prior.getFieldType(), current.getFieldType()));
        }

        if (current.getBusinessKey() != prior.getBusinessKey()) {

            ctx = ctx.error(String.format(
                    "Business key flag changed for [%s]: prior = [%s], new = [%s]",
                    prior.getFieldName(), prior.getBusinessKey(), current.getBusinessKey()));
        }

        if (current.getCategorical() != prior.getCategorical()) {

            ctx = ctx.error(String.format(
                    "Categorical flag changed for [%s]: prior = [%s], new = [%s]",
                    prior.getFieldName(), prior.getCategorical(), current.getCategorical()));
        }

        return ctx;
    }

    public static ValidationContext newField(FieldSchema newField, ValidationContext ctx) {

        if (newField.getBusinessKey()) {

            ctx = ctx.error(String.format(
                    "New field [%s] is marked as a business key",
                    newField.getFieldName()));
        }

        return ctx;
    }
}
