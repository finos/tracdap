/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.validation.version;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.metadata.*;
import com.google.protobuf.Descriptors;

import java.util.function.Function;
import java.util.stream.Collectors;

public class SchemaVersionValidator {

    private static final Descriptors.Descriptor SCHEMA_DEFINITION;
    private static final Descriptors.FieldDescriptor SD_SCHEMA_TYPE;
    private static final Descriptors.FieldDescriptor SD_TABLE;

    static {

        SCHEMA_DEFINITION = SchemaDefinition.getDescriptor();
        SD_SCHEMA_TYPE = field(SCHEMA_DEFINITION, SchemaDefinition.SCHEMATYPE_FIELD_NUMBER);
        SD_TABLE = field(SCHEMA_DEFINITION, SchemaDefinition.TABLE_FIELD_NUMBER);
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

        var priorFields = prior
                .getFieldsList().stream()
                .collect(Collectors.toMap(f -> f.getFieldName().toLowerCase(), Function.identity()));

        var currentFields = current
                .getFieldsList().stream()
                .collect(Collectors.toMap(f -> f.getFieldName().toLowerCase(), Function.identity()));

        var newFields = current.getFieldsList().stream()
                .filter(f -> !priorFields.containsKey(f.getFieldName().toLowerCase()))
                .collect(Collectors.toList());

        var existingFields = current.getFieldsList().stream()
                .filter(f -> priorFields.containsKey(f.getFieldName().toLowerCase()))
                .collect(Collectors.toList());

        var removedFields = prior.getFieldsList().stream()
                .filter(f -> !currentFields.containsKey(f.getFieldName().toLowerCase()))
                .collect(Collectors.toList());

        for (var field : existingFields) {

            var priorField = priorFields.get(field.getFieldName().toLowerCase());
            ctx = existingField(field, priorField, ctx);
        }

        for (var field : newFields)
            ctx = newField(field, ctx);

        for (var field : removedFields)
            ctx = removedField(field, ctx);

        return ctx;
    }

    private static ValidationContext existingField(FieldSchema current, FieldSchema prior, ValidationContext ctx) {

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

    private static ValidationContext newField(FieldSchema newField, ValidationContext ctx) {

        if (newField.getBusinessKey()) {

            ctx = ctx.error(String.format(
                    "New field [%s] is marked as a business key",
                    newField.getFieldName()));
        }

        return ctx;
    }

    private static ValidationContext removedField(FieldSchema removedField, ValidationContext ctx) {

        return ctx.error(String.format(
                "Field [%s] from the prior schema version has been removed",
                removedField.getFieldName()));
    }
}
