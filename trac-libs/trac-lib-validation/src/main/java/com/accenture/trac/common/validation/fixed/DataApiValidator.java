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

import com.accenture.trac.api.DataReadRequest;
import com.accenture.trac.api.DataWriteRequest;
import com.accenture.trac.api.FileReadRequest;
import com.accenture.trac.api.FileWriteRequest;
import com.accenture.trac.metadata.ObjectType;
import com.accenture.trac.metadata.SchemaDefinition;
import com.accenture.trac.metadata.TagSelector;
import com.accenture.trac.metadata.TagUpdate;
import com.accenture.trac.common.validation.core.ValidationContext;
import com.google.protobuf.Descriptors;


public class DataApiValidator {

    private static final Descriptors.Descriptor DATA_WRITE_REQUEST;
    private static final Descriptors.FieldDescriptor DWR_TENANT;
    private static final Descriptors.FieldDescriptor DWR_TAG_UPDATES;
    private static final Descriptors.FieldDescriptor DWR_PRIOR_VERSION;
    private static final Descriptors.FieldDescriptor DWR_SCHEMA_ID;
    private static final Descriptors.FieldDescriptor DWR_SCHEMA;
    private static final Descriptors.OneofDescriptor DWR_SCHEMA_DEFINITION;
    private static final Descriptors.FieldDescriptor DWR_FORMAT;

    private static final Descriptors.Descriptor DATA_READ_REQUEST;
    private static final Descriptors.FieldDescriptor DRR_TENANT;
    private static final Descriptors.FieldDescriptor DRR_SELECTOR;
    private static final Descriptors.FieldDescriptor DRR_FORMAT;

    private static final Descriptors.Descriptor FILE_WRITE_REQUEST;
    private static final Descriptors.FieldDescriptor FWR_TENANT;
    private static final Descriptors.FieldDescriptor FWR_TAG_UPDATES;
    private static final Descriptors.FieldDescriptor FWR_PRIOR_VERSION;
    private static final Descriptors.FieldDescriptor FWR_NAME;
    private static final Descriptors.FieldDescriptor FWR_MIME_TYPE;
    private static final Descriptors.FieldDescriptor FWR_SIZE;

    private static final Descriptors.Descriptor FILE_READ_REQUEST;
    private static final Descriptors.FieldDescriptor FRR_TENANT;
    private static final Descriptors.FieldDescriptor FRR_SELECTOR;

    static {

        DATA_WRITE_REQUEST = DataWriteRequest.getDescriptor();
        DWR_TENANT = field(DATA_WRITE_REQUEST, DataWriteRequest.TENANT_FIELD_NUMBER);
        DWR_TAG_UPDATES = field(DATA_WRITE_REQUEST, DataWriteRequest.TAGUPDATES_FIELD_NUMBER);
        DWR_PRIOR_VERSION = field(DATA_WRITE_REQUEST, DataWriteRequest.PRIORVERSION_FIELD_NUMBER);
        DWR_SCHEMA_ID = field(DATA_WRITE_REQUEST, DataWriteRequest.SCHEMAID_FIELD_NUMBER);
        DWR_SCHEMA = field(DATA_WRITE_REQUEST, DataWriteRequest.SCHEMA_FIELD_NUMBER);
        DWR_SCHEMA_DEFINITION = DWR_SCHEMA.getContainingOneof();
        DWR_FORMAT = field(DATA_WRITE_REQUEST, DataWriteRequest.FORMAT_FIELD_NUMBER);

        DATA_READ_REQUEST = DataReadRequest.getDescriptor();
        DRR_TENANT = field(DATA_READ_REQUEST, DataReadRequest.TENANT_FIELD_NUMBER);
        DRR_SELECTOR = field(DATA_READ_REQUEST, DataReadRequest.SELECTOR_FIELD_NUMBER);
        DRR_FORMAT = field(DATA_READ_REQUEST, DataReadRequest.FORMAT_FIELD_NUMBER);

        FILE_WRITE_REQUEST = FileWriteRequest.getDescriptor();
        FWR_TENANT = field(FILE_WRITE_REQUEST, FileWriteRequest.TENANT_FIELD_NUMBER);
        FWR_TAG_UPDATES = field(FILE_WRITE_REQUEST, FileWriteRequest.TAGUPDATES_FIELD_NUMBER);
        FWR_PRIOR_VERSION = field(FILE_WRITE_REQUEST, FileWriteRequest.PRIORVERSION_FIELD_NUMBER);
        FWR_NAME = field(FILE_WRITE_REQUEST, FileWriteRequest.NAME_FIELD_NUMBER);
        FWR_MIME_TYPE = field(FILE_WRITE_REQUEST, FileWriteRequest.MIMETYPE_FIELD_NUMBER);
        FWR_SIZE = field(FILE_WRITE_REQUEST, FileWriteRequest.SIZE_FIELD_NUMBER);

        FILE_READ_REQUEST = FileReadRequest.getDescriptor();
        FRR_TENANT = field(FILE_READ_REQUEST, FileReadRequest.TENANT_FIELD_NUMBER);
        FRR_SELECTOR = field(FILE_READ_REQUEST, FileReadRequest.SELECTOR_FIELD_NUMBER);
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
    }

    public static ValidationContext validateCreateDataset(DataWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(DWR_PRIOR_VERSION)
                .apply(Validation::omitted)
                .pop();

        return createOrUpdateDataset(msg, ctx);
    }

    public static ValidationContext validateUpdateDataset(DataWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(DWR_PRIOR_VERSION)
                .apply(Validation::required)
                .apply(MetadataValidator::validateTagSelector, TagSelector.class)
                .apply(Validation.selectorType(ObjectType.DATA), TagSelector.class)
                .pop();

        return createOrUpdateDataset(msg, ctx);
    }

    private static ValidationContext createOrUpdateDataset(DataWriteRequest msg, ValidationContext ctx) {

        // Restricting tenant keys to be valid identifiers is a very strong restriction!
        // This could be loosened a bit, perhaps natural language and UTF is ok?
        // Easier to loosen later hence leaving the strict restriction for now

        ctx = ctx.push(DWR_TENANT)
                .apply(Validation::required)
                .apply(Validation::identifier)
                .pop();

        ctx = ctx.push(DWR_TAG_UPDATES)
                .applyList(MetadataValidator::validateTagUpdate, TagUpdate.class)
                .pop();

        ctx = ctx.pushOneOf(DWR_SCHEMA_DEFINITION)
                .apply(Validation::required)
                .applyIf(MetadataValidator::validateTagSelector, TagSelector.class, msg.hasField(DWR_SCHEMA_ID))
                .applyIf(Validation.selectorType(ObjectType.SCHEMA), TagSelector.class, msg.hasField(DWR_SCHEMA_ID))
                .applyIf(Validation::fixedObjectVersion, TagSelector.class, msg.hasField(DWR_SCHEMA_ID))
                .applyIf(SchemaValidator::schema, SchemaDefinition.class, msg.hasField(DWR_SCHEMA))
                .pop();

        ctx = ctx.push(DWR_FORMAT)
                .apply(Validation::required)
                .apply(Validation::mimeType)
                .pop();

        return ctx;
    }

    public static ValidationContext validateReadDataset(DataReadRequest msg, ValidationContext ctx) {

        ctx = ctx.push(DRR_TENANT)
                .apply(Validation::required)
                .apply(Validation::identifier)
                .pop();

        ctx = ctx.push(DRR_SELECTOR)
                .apply(Validation::required)
                .apply(MetadataValidator::validateTagSelector, TagSelector.class)
                .apply(Validation.selectorType(ObjectType.DATA), TagSelector.class)
                .pop();

        ctx = ctx.push(DRR_FORMAT)
                .apply(Validation::required)
                .apply(Validation::mimeType)
                .pop();

        return ctx;
    }

    public static ValidationContext validateCreateFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(FWR_PRIOR_VERSION)
                .apply(Validation::omitted)
                .pop();

        return createOrUpdateFile(msg, ctx);
    }

    public static ValidationContext validateUpdateFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(FWR_PRIOR_VERSION)
                .apply(Validation::required)
                .apply(MetadataValidator::validateTagSelector, TagSelector.class)
                .apply(Validation.selectorType(ObjectType.FILE), TagSelector.class)
                .pop();

        return createOrUpdateFile(msg, ctx);
    }

    private static ValidationContext createOrUpdateFile(FileWriteRequest msg, ValidationContext ctx) {

        // Restricting tenant keys to be valid identifiers is a very strong restriction!
        // This could be loosened a bit, perhaps natural language and UTF is ok?
        // Easier to loosen later hence leaving the strict restriction for now

        ctx = ctx.push(FWR_TENANT)
                .apply(Validation::required)
                .apply(Validation::identifier)
                .pop();

        ctx = ctx.push(FWR_TAG_UPDATES)
                .applyList(MetadataValidator::validateTagUpdate, TagUpdate.class)
                .pop();

        ctx = ctx.push(FWR_NAME)
                .apply(Validation::required)
                .apply(Validation::fileName)
                .pop();

        ctx = ctx.push(FWR_MIME_TYPE)
                .apply(Validation::required)
                .apply(Validation::mimeType)
                .pop();

        ctx = ctx.push(FWR_SIZE)
                .apply(Validation::optional)
                .apply(Validation::notNegative, Long.class)
                .pop();

        return ctx;
    }

    public static ValidationContext validateReadFile(FileReadRequest msg, ValidationContext ctx) {

        ctx = ctx.push(FRR_TENANT)
                .apply(Validation::required)
                .apply(Validation::identifier)
                .pop();

        ctx = ctx.push(FRR_SELECTOR)
                .apply(Validation::required)
                .apply(MetadataValidator::validateTagSelector, TagSelector.class)
                .apply(Validation.selectorType(ObjectType.FILE), TagSelector.class)
                .pop();

        return ctx;
    }
}
