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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.core.ValidatorUtils;
import org.finos.tracdap.metadata.*;
import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Validator(type = ValidationType.STATIC)
public class SchemaValidator {

    private static final Map<SchemaDefinition.SchemaDetailsCase, SchemaType> SCHEMA_TYPE_CASE_MAPPING = Map.ofEntries(
            Map.entry(SchemaDefinition.SchemaDetailsCase.TABLE, SchemaType.TABLE_SCHEMA));

    private static final List<BasicType> ALLOWED_BUSINESS_KEY_TYPES = List.of(
            BasicType.STRING, BasicType.INTEGER, BasicType.DATE);

    private static final List<BasicType> ALLOWED_CATEGORICAL_TYPES = List.of(BasicType.STRING);

    private static final Map<String, SchemaDefinition> NO_NAMED_TYPES = Map.of();
    private static final Map<String, EnumValues> NO_NAMED_ENUMS = Map.of();

    private static final Descriptors.Descriptor SCHEMA_DEFINITION;
    private static final Descriptors.FieldDescriptor SD_SCHEMA_TYPE;
    private static final Descriptors.FieldDescriptor SD_FIELDS;
    private static final Descriptors.FieldDescriptor SD_NAMED_TYPES;
    private static final Descriptors.FieldDescriptor SD_NAMED_ENUMS;
    private static final Descriptors.OneofDescriptor SD_SCHEMA_DETAILS;
    private static final Descriptors.FieldDescriptor SD_TABLE;

    private static final Descriptors.Descriptor TABLE_SCHEMA;
    private static final Descriptors.FieldDescriptor TS_FIELDS;

    private static final Descriptors.Descriptor FIELD_SCHEMA;
    private static final Descriptors.FieldDescriptor FS_FIELD_NAME;
    private static final Descriptors.FieldDescriptor FS_FIELD_ORDER;
    private static final Descriptors.FieldDescriptor FS_FIELD_TYPE;
    private static final Descriptors.FieldDescriptor FS_LABEL;
    private static final Descriptors.FieldDescriptor FS_DEFAULT_VALUE;
    private static final Descriptors.FieldDescriptor FS_NAMED_TYPE;
    private static final Descriptors.FieldDescriptor FS_NAMED_ENUM;
    private static final Descriptors.FieldDescriptor FS_CHILDREN;

    private static final Descriptors.Descriptor ENUM_VALUES;
    private static final Descriptors.FieldDescriptor EV_VALUES;


    static {

        SCHEMA_DEFINITION = SchemaDefinition.getDescriptor();
        SD_SCHEMA_TYPE = ValidatorUtils.field(SCHEMA_DEFINITION, SchemaDefinition.SCHEMATYPE_FIELD_NUMBER);
        SD_FIELDS = ValidatorUtils.field(SCHEMA_DEFINITION, SchemaDefinition.FIELDS_FIELD_NUMBER);
        SD_NAMED_TYPES = ValidatorUtils.field(SCHEMA_DEFINITION, SchemaDefinition.NAMEDTYPES_FIELD_NUMBER);
        SD_NAMED_ENUMS = ValidatorUtils.field(SCHEMA_DEFINITION, SchemaDefinition.NAMEDENUMS_FIELD_NUMBER);
        SD_TABLE = ValidatorUtils.field(SCHEMA_DEFINITION, SchemaDefinition.TABLE_FIELD_NUMBER);
        SD_SCHEMA_DETAILS = SD_TABLE.getContainingOneof();

        TABLE_SCHEMA = TableSchema.getDescriptor();
        TS_FIELDS = ValidatorUtils.field(TABLE_SCHEMA, TableSchema.FIELDS_FIELD_NUMBER);

        FIELD_SCHEMA = FieldSchema.getDescriptor();
        FS_FIELD_NAME = ValidatorUtils.field(FIELD_SCHEMA, FieldSchema.FIELDNAME_FIELD_NUMBER);
        FS_FIELD_ORDER = ValidatorUtils.field(FIELD_SCHEMA, FieldSchema.FIELDORDER_FIELD_NUMBER);
        FS_FIELD_TYPE = ValidatorUtils.field(FIELD_SCHEMA, FieldSchema.FIELDTYPE_FIELD_NUMBER);
        FS_LABEL = ValidatorUtils.field(FIELD_SCHEMA, FieldSchema.LABEL_FIELD_NUMBER);
        FS_DEFAULT_VALUE = ValidatorUtils.field(FIELD_SCHEMA, FieldSchema.DEFAULTVALUE_FIELD_NUMBER);
        FS_NAMED_TYPE = ValidatorUtils.field(FIELD_SCHEMA, FieldSchema.NAMEDTYPE_FIELD_NUMBER);
        FS_NAMED_ENUM = ValidatorUtils.field(FIELD_SCHEMA, FieldSchema.NAMEDENUM_FIELD_NUMBER);
        FS_CHILDREN = ValidatorUtils.field(FIELD_SCHEMA, FieldSchema.CHILDREN_FIELD_NUMBER);

        ENUM_VALUES = EnumValues.getDescriptor();
        EV_VALUES = ValidatorUtils.field(ENUM_VALUES, EnumValues.VALUES_FIELD_NUMBER);
    }

    public static ValidationContext dynamicSchema(SchemaDefinition schema, ValidationContext ctx) {

        ctx = ctx.push(SD_SCHEMA_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, SchemaType.class)
                .pop();

        // For dynamic schemas, schema details must not be included

        ctx = ctx.pushOneOf(SD_SCHEMA_DETAILS)
                .apply(CommonValidators::omitted)
                .pop();

        ctx = ctx.pushRepeated(SD_FIELDS)
                .apply(CommonValidators::omitted)
                .pop();

        ctx = ctx.pushMap(SD_NAMED_TYPES)
                .apply(CommonValidators::omitted)
                .pop();

        ctx = ctx.pushMap(SD_NAMED_ENUMS)
                .apply(CommonValidators::omitted)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext schema(SchemaDefinition schema, ValidationContext ctx) {

        return schema(schema, null, ctx);
    }

    private static ValidationContext schema(SchemaDefinition schema, SchemaDefinition rootSchema, ValidationContext ctx) {

        ctx = ctx.push(SD_SCHEMA_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, SchemaType.class)
                .pop();

        var parentSchema = rootSchema != null ? rootSchema : schema;
        var allowNamedTypes = rootSchema == null && schema.getSchemaType() != SchemaType.TABLE;
        var allowNamedEnums = rootSchema == null;

        if (allowNamedTypes) {
            ctx = ctx.pushMap(SD_NAMED_TYPES)
                    .applyMapKeys(CommonValidators::qualifiedIdentifier)
                    .applyMapKeys(CommonValidators::notTracReserved)
                    .applyMapValues(SchemaValidator::schema, SchemaDefinition.class, parentSchema)
                    .pop();
        }
        else {
            ctx = ctx.pushMap(SD_NAMED_TYPES)
                    .applyMapKeys(CommonValidators::omitted)
                    .pop();
        }

        if (allowNamedEnums) {
            ctx = ctx.pushMap(SD_NAMED_ENUMS)
                    .applyMapKeys(CommonValidators::qualifiedIdentifier)
                    .applyMapKeys(CommonValidators::notTracReserved)
                    .applyMapValues(SchemaValidator::enumValues, EnumValues.class)
                    .pop();
        }
        else {
            ctx = ctx.pushMap(SD_NAMED_ENUMS)
                    .applyMapKeys(CommonValidators::omitted)
                    .pop();
        }

        if (schema.getSchemaType() == SchemaType.TABLE_SCHEMA) {

            // Otherwise apply the regular validator

            ctx = ctx.pushOneOf(SD_SCHEMA_DETAILS)
                    .apply(CommonValidators::required)
                    .apply(SchemaValidator::schemaMatchesType)
                    .apply(SchemaValidator::tableSchema, TableSchema.class, parentSchema)
                    .applyRegistered()
                    .pop();
        }

        else {

            ctx = ctx.pushRepeated(SD_FIELDS)
                    .apply(CommonValidators::listNotEmpty)
                    .applyRepeated(SchemaValidator::fieldSchema, FieldSchema.class, parentSchema)
                    .pop();
        }

        return ctx;
    }

    public static ValidationContext schemaMatchesType(ValidationContext ctx) {

        var schemaDef = (SchemaDefinition) ctx.parentMsg();
        var schemaDetails = schemaDef.getSchemaDetailsCase();

        var schemaType = schemaDef.getSchemaType();
        var definitionType = SCHEMA_TYPE_CASE_MAPPING.getOrDefault(schemaDetails, SchemaType.UNRECOGNIZED);

        if (schemaType != definitionType) {

            var err = String.format("Schema has type [%s] but contains definition type [%s]",
                    schemaType, definitionType);

            return ctx.error(err);
        }

        return ctx;
    }

    @Validator
    public static ValidationContext tableSchema(TableSchema table, ValidationContext ctx) {

        ctx = ctx.pushRepeated(TS_FIELDS)
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated(SchemaValidator::fieldSchema, FieldSchema.class)
                .pop();

        return fieldNamesAndOrdering(table.getFieldsList(), ctx);
    }

    public static ValidationContext tableSchema(TableSchema table, SchemaDefinition root, ValidationContext ctx) {

        ctx = ctx.pushRepeated(TS_FIELDS)
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated(SchemaValidator::fieldSchema, FieldSchema.class, root)
                .pop();

        return fieldNamesAndOrdering(table.getFieldsList(), ctx);
    }

    private static ValidationContext fieldNamesAndOrdering(List<FieldSchema> fields, ValidationContext ctx) {

        // Check for duplicate field names, including duplicates with different case

        var names = fields.stream()
                .map(FieldSchema::getFieldName)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        if (names.size() != fields.size()) {

            var err = "Table schema contains duplicate field names";
            ctx = ctx.error(err);
        }

        // If fields orders are all zero, ordering will be inferred from the list order

        var allZeroOrder = fields.stream()
                .map(FieldSchema::getFieldOrder)
                .allMatch(order -> order == 0);

        if (!allZeroOrder) {

            // If field orders are specified, they must for a contiguous set of indices starting at zero
            // Fields may be specified out-of-order, so long as the entire set forms a contiguous range
            // I.e. 0, 1, 3, 4, 2 is allowed, but 0, 1, 3, 4, 5 is not

            var orders = fields.stream()
                    .map(FieldSchema::getFieldOrder)
                    .collect(Collectors.toSet());

            for (var fieldOrder = 0; fieldOrder < fields.size(); fieldOrder++) {

                if (!orders.contains(fieldOrder)) {
                    var err = "Field orders must form a contiguous set of indices starting at zero";
                    ctx = ctx.error(err);
                }
            }
        }

        return ctx;
    }

    @Validator
    public static ValidationContext fieldSchema(FieldSchema field, ValidationContext ctx) {

        return fieldSchema(field, NO_NAMED_TYPES, NO_NAMED_ENUMS, SchemaType.TABLE_SCHEMA, ctx);
    }

    public static ValidationContext fieldSchema(FieldSchema field, SchemaDefinition root, ValidationContext ctx) {

        if (root != null)
            return fieldSchema(field, root.getNamedTypesMap(), root.getNamedEnumsMap(), root.getSchemaType(), ctx);
        else
            return fieldSchema(field, NO_NAMED_TYPES, NO_NAMED_ENUMS, SchemaType.TABLE_SCHEMA, ctx);
    }

    public static ValidationContext fieldSchema(
            FieldSchema field,
            Map<String, SchemaDefinition> namedTypes,
            Map<String, EnumValues> namedEnums,
            SchemaType schemaType,
            ValidationContext ctx) {

        ctx = ctx.push(FS_FIELD_NAME)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .apply(CommonValidators::notTracReserved)
                .pop();

        // Do not apply Validation::required, which fails for zero-valued integers
        ctx = ctx.push(FS_FIELD_ORDER)
                .apply(CommonValidators::notNegative, Integer.class)
                .pop();

        ctx = ctx.push(FS_FIELD_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::recognizedEnum, BasicType.class)
                .applyIf(schemaType == SchemaType.TABLE_SCHEMA, CommonValidators::primitiveType, BasicType.class)
                .pop();

        ctx = ctx.push(FS_LABEL)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::labelLengthLimit)
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

        // Business key fields should always be marked not null
        // We could remove this check in validation, and normalize instead

        if (field.getBusinessKey() && !field.getNotNull()) {

            var err = String.format(
                    "Schema field [%s] cannot have notNull == false because it is a business key",
                    ctx.fieldName());

            ctx = ctx.error(err);
        }

        if (TypeSystem.isPrimitive(field.getFieldType())) {

            if (field.getChildrenCount() > 0) {

                var err = String.format(
                        "Schema field [%s] cannot have children because it is primitive type [%s]",
                        ctx.fieldName(), field.getFieldType());

                ctx = ctx.error(err);
            }
        }
        else if (field.getFieldType() == BasicType.ARRAY) {

            if (field.getChildrenCount() != 1) {

                var err = String.format(
                        "Schema field [%s] mast have exactly 1 child because it is type [%s]",
                        ctx.fieldName(), field.getFieldType());

                ctx = ctx.error(err);
            }
        }
        else if (field.getFieldType() == BasicType.MAP) {

            if (field.getChildrenCount() != 2) {

                var err = String.format(
                        "Schema field [%s] mast have exactly 2 children because it is type [%s]",
                        ctx.fieldName(), field.getFieldType());

                ctx = ctx.error(err);
            }
            else if (field.getChildren(0).getFieldType() != BasicType.STRING) {

                var err = String.format(
                        "Schema field [%s] is type [%s], it mast define a child of type STRING as the key",
                        ctx.fieldName(), field.getFieldType());

                ctx = ctx.error(err);
            }
        }
        else if (field.getFieldType() == BasicType.STRUCT) {

            if (field.getChildrenCount() == 0 && ! field.hasNamedType()) {

                var err = String.format(
                        "Schema field [%s] mast define at least one child or have a named type because it is type [%s]",
                        ctx.fieldName(), field.getFieldType());

                ctx = ctx.error(err);
            }
        }

        if (field.hasNamedType()) {

            if (field.getFieldType() != BasicType.STRUCT) {

                var err = String.format(
                        "Schema field [%s] cannot use named type [%s] because it is not a STRUCT field",
                        ctx.fieldName(), field.getFieldType());

                ctx = ctx.error(err);
            }
            else if (field.getChildrenCount() > 1) {

                var err = String.format(
                        "Schema field [%s] cannot use named type [%s] because it defines its own children",
                        ctx.fieldName(), field.getFieldType());

                ctx = ctx.error(err);
            }
            else if (!namedTypes.containsKey(field.getNamedType())) {

                var err = String.format(
                        "Schema field [%s] refers to unknown type [%s]]",
                        ctx.fieldName(), field.getNamedType());

                ctx = ctx.error(err);
            }
        }

        if (field.hasNamedEnum()) {

            if (!field.getCategorical()) {

                var err = String.format(
                        "Schema field [%s] cannot use named enum [%s] because it is not a categorical field",
                        ctx.fieldName(), field.getNamedEnum());

                ctx = ctx.error(err);
            }
            else if (!namedEnums.containsKey(field.getNamedEnum())) {

                var err = String.format(
                        "Schema field [%s] refers to unknown enum [%s]]",
                        ctx.fieldName(), field.getNamedEnum());

                ctx = ctx.error(err);
            }
        }

        // No validation applied to label or format code

        return ctx;
    }

    private static ValidationContext enumValues(EnumValues enumValues, ValidationContext ctx) {

        ctx.pushRepeated(EV_VALUES)
                .apply(CommonValidators::listNotEmpty)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .pop();

        return ctx;
    }
}
