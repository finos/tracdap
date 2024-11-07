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

package org.finos.tracdap.common.validation.api;

import com.google.protobuf.ByteString;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.core.ValidatorUtils;
import org.finos.tracdap.common.validation.static_.CommonValidators;
import org.finos.tracdap.common.validation.static_.ObjectIdValidator;
import org.finos.tracdap.common.validation.static_.SchemaValidator;
import org.finos.tracdap.common.validation.static_.TagUpdateValidator;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.SchemaDefinition;
import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.metadata.TagUpdate;
import org.finos.tracdap.common.validation.core.ValidationContext;
import com.google.protobuf.Descriptors;


@Validator(type = ValidationType.STATIC, serviceFile = Data.class, serviceName = TracDataApiGrpc.SERVICE_NAME)
public class DataApiValidator {

    private static final Descriptors.Descriptor DATA_WRITE_REQUEST;
    private static final Descriptors.FieldDescriptor DWR_TENANT;
    private static final Descriptors.FieldDescriptor DWR_TAG_UPDATES;
    private static final Descriptors.FieldDescriptor DWR_PRIOR_VERSION;
    private static final Descriptors.FieldDescriptor DWR_SCHEMA_ID;
    private static final Descriptors.FieldDescriptor DWR_SCHEMA;
    private static final Descriptors.OneofDescriptor DWR_SCHEMA_SPECIFIER;
    private static final Descriptors.FieldDescriptor DWR_FORMAT;
    private static final Descriptors.FieldDescriptor DWR_CONTENT;

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
    private static final Descriptors.FieldDescriptor FWR_CONTENT;

    private static final Descriptors.Descriptor FILE_READ_REQUEST;
    private static final Descriptors.FieldDescriptor FRR_TENANT;
    private static final Descriptors.FieldDescriptor FRR_SELECTOR;

    private static final Descriptors.Descriptor DOWNLOAD_REQUEST;
    private static final Descriptors.FieldDescriptor DR_TENANT;
    private static final Descriptors.FieldDescriptor DR_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor DR_OBJECT_ID;
    private static final Descriptors.FieldDescriptor DR_OBJECT_VERSION;

    static {

        DATA_WRITE_REQUEST = DataWriteRequest.getDescriptor();
        DWR_TENANT = ValidatorUtils.field(DATA_WRITE_REQUEST, DataWriteRequest.TENANT_FIELD_NUMBER);
        DWR_TAG_UPDATES = ValidatorUtils.field(DATA_WRITE_REQUEST, DataWriteRequest.TAGUPDATES_FIELD_NUMBER);
        DWR_PRIOR_VERSION = ValidatorUtils.field(DATA_WRITE_REQUEST, DataWriteRequest.PRIORVERSION_FIELD_NUMBER);
        DWR_SCHEMA_ID = ValidatorUtils.field(DATA_WRITE_REQUEST, DataWriteRequest.SCHEMAID_FIELD_NUMBER);
        DWR_SCHEMA = ValidatorUtils.field(DATA_WRITE_REQUEST, DataWriteRequest.SCHEMA_FIELD_NUMBER);
        DWR_SCHEMA_SPECIFIER = DWR_SCHEMA.getContainingOneof();
        DWR_FORMAT = ValidatorUtils.field(DATA_WRITE_REQUEST, DataWriteRequest.FORMAT_FIELD_NUMBER);
        DWR_CONTENT = ValidatorUtils.field(DATA_WRITE_REQUEST, DataWriteRequest.CONTENT_FIELD_NUMBER);

        DATA_READ_REQUEST = DataReadRequest.getDescriptor();
        DRR_TENANT = ValidatorUtils.field(DATA_READ_REQUEST, DataReadRequest.TENANT_FIELD_NUMBER);
        DRR_SELECTOR = ValidatorUtils.field(DATA_READ_REQUEST, DataReadRequest.SELECTOR_FIELD_NUMBER);
        DRR_FORMAT = ValidatorUtils.field(DATA_READ_REQUEST, DataReadRequest.FORMAT_FIELD_NUMBER);

        FILE_WRITE_REQUEST = FileWriteRequest.getDescriptor();
        FWR_TENANT = ValidatorUtils.field(FILE_WRITE_REQUEST, FileWriteRequest.TENANT_FIELD_NUMBER);
        FWR_TAG_UPDATES = ValidatorUtils.field(FILE_WRITE_REQUEST, FileWriteRequest.TAGUPDATES_FIELD_NUMBER);
        FWR_PRIOR_VERSION = ValidatorUtils.field(FILE_WRITE_REQUEST, FileWriteRequest.PRIORVERSION_FIELD_NUMBER);
        FWR_NAME = ValidatorUtils.field(FILE_WRITE_REQUEST, FileWriteRequest.NAME_FIELD_NUMBER);
        FWR_MIME_TYPE = ValidatorUtils.field(FILE_WRITE_REQUEST, FileWriteRequest.MIMETYPE_FIELD_NUMBER);
        FWR_SIZE = ValidatorUtils.field(FILE_WRITE_REQUEST, FileWriteRequest.SIZE_FIELD_NUMBER);
        FWR_CONTENT = ValidatorUtils.field(FILE_WRITE_REQUEST, FileWriteRequest.CONTENT_FIELD_NUMBER);

        FILE_READ_REQUEST = FileReadRequest.getDescriptor();
        FRR_TENANT = ValidatorUtils.field(FILE_READ_REQUEST, FileReadRequest.TENANT_FIELD_NUMBER);
        FRR_SELECTOR = ValidatorUtils.field(FILE_READ_REQUEST, FileReadRequest.SELECTOR_FIELD_NUMBER);

        DOWNLOAD_REQUEST = DownloadRequest.getDescriptor();
        DR_TENANT = ValidatorUtils.field(DOWNLOAD_REQUEST, DownloadRequest.TENANT_FIELD_NUMBER);
        DR_OBJECT_TYPE = ValidatorUtils.field(DOWNLOAD_REQUEST, DownloadRequest.OBJECTTYPE_FIELD_NUMBER);
        DR_OBJECT_ID = ValidatorUtils.field(DOWNLOAD_REQUEST, DownloadRequest.OBJECTID_FIELD_NUMBER);
        DR_OBJECT_VERSION = ValidatorUtils.field(DOWNLOAD_REQUEST, DownloadRequest.OBJECTVERSION_FIELD_NUMBER);
    }

    @Validator(method = "createDataset")
    public static ValidationContext createDataset(DataWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(DWR_PRIOR_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        return createOrUpdateDataset(msg, ctx);
    }

    @Validator(method = "createSmallDataset")
    public static ValidationContext createSmallDataset(DataWriteRequest msg, ValidationContext ctx) {

        ctx = createDataset(msg, ctx);

        ctx.push(DWR_CONTENT)
                .apply(CommonValidators::required)
                .apply(DataApiValidator::contentNotEmpty, ByteString.class)
                .pop();

        return ctx;
    }

    @Validator(method = "updateDataset")
    public static ValidationContext updateDataset(DataWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(DWR_PRIOR_VERSION)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.DATA)
                .pop();

        return createOrUpdateDataset(msg, ctx);
    }

    @Validator(method = "updateSmallDataset")
    public static ValidationContext updateSmallDataset(DataWriteRequest msg, ValidationContext ctx) {

        ctx = updateDataset(msg, ctx);

        ctx.push(DWR_CONTENT)
                .apply(CommonValidators::required)
                .apply(DataApiValidator::contentNotEmpty, ByteString.class)
                .pop();

        return ctx;
    }

    private static ValidationContext createOrUpdateDataset(DataWriteRequest msg, ValidationContext ctx) {

        // Restricting tenant keys to be valid identifiers is a very strong restriction!
        // This could be loosened a bit, perhaps natural language and UTF is ok?
        // Easier to loosen later hence leaving the strict restriction for now

        ctx = ctx.push(DWR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.pushRepeated(DWR_TAG_UPDATES)
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                .applyRepeated(TagUpdateValidator::reservedAttrs, TagUpdate.class, false)
                .pop();

        ctx = ctx.pushOneOf(DWR_SCHEMA_SPECIFIER)
                .apply(CommonValidators::required)
                .applyOneOf(DWR_SCHEMA_ID, ObjectIdValidator::tagSelector, TagSelector.class)
                .applyOneOf(DWR_SCHEMA_ID, ObjectIdValidator::selectorType, TagSelector.class, ObjectType.SCHEMA)
                .applyOneOf(DWR_SCHEMA_ID, ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .applyOneOf(DWR_SCHEMA, SchemaValidator::schema, SchemaDefinition.class)
                .pop();

        ctx = ctx.push(DWR_FORMAT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::mimeType)
                .pop();

        return ctx;
    }

    @Validator(method = "readDataset")
    public static ValidationContext readDataset(DataReadRequest msg, ValidationContext ctx) {

        ctx = ctx.push(DRR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(DRR_SELECTOR)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.DATA)
                .pop();

        ctx = ctx.push(DRR_FORMAT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::mimeType)
                .pop();

        return ctx;
    }

    @Validator(method = "readSmallDataset")
    public static ValidationContext readSmallDataset(DataReadRequest msg, ValidationContext ctx) {

        return readDataset(msg, ctx);
    }

    @Validator(method = "createFile")
    public static ValidationContext createFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(FWR_PRIOR_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        return createOrUpdateFile(msg, ctx);
    }

    @Validator(method = "createSmallFile")
    public static ValidationContext createSmallFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = createFile(msg, ctx);

        ctx.push(FWR_CONTENT)
                .apply(CommonValidators::required)
                .apply(DataApiValidator::contentNotEmpty, ByteString.class)
                .pop();

        return ctx;
    }

    @Validator(method = "updateFile")
    public static ValidationContext updateFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(FWR_PRIOR_VERSION)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.FILE)
                .pop();

        return createOrUpdateFile(msg, ctx);
    }

    @Validator(method = "updateSmallFile")
    public static ValidationContext updateSmallFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = updateFile(msg, ctx);

        ctx.push(FWR_CONTENT)
                .apply(CommonValidators::required)
                .apply(DataApiValidator::contentNotEmpty, ByteString.class)
                .pop();

        return ctx;
    }

    private static ValidationContext createOrUpdateFile(FileWriteRequest msg, ValidationContext ctx) {

        // Restricting tenant keys to be valid identifiers is a very strong restriction!
        // This could be loosened a bit, perhaps natural language and UTF is ok?
        // Easier to loosen later hence leaving the strict restriction for now

        ctx = ctx.push(FWR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.pushRepeated(FWR_TAG_UPDATES)
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                .applyRepeated(TagUpdateValidator::reservedAttrs, TagUpdate.class, false)
                .pop();

        ctx = ctx.push(FWR_NAME)
                .apply(CommonValidators::required)
                .apply(CommonValidators::fileName)
                .apply(CommonValidators::notTracReserved)
                .pop();

        ctx = ctx.push(FWR_MIME_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::mimeType)
                .pop();

        ctx = ctx.push(FWR_SIZE)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::notNegative, Long.class)
                .pop();

        return ctx;
    }

    @Validator(method = "readFile")
    public static ValidationContext readFile(FileReadRequest msg, ValidationContext ctx) {

        ctx = ctx.push(FRR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(FRR_SELECTOR)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.FILE)
                .pop();

        return ctx;
    }

    @Validator(method = "readSmallFile")
    public static ValidationContext readSmallFile(FileReadRequest msg, ValidationContext ctx) {

        return readFile(msg, ctx);
    }

    private static ValidationContext contentNotEmpty(ByteString content, ValidationContext ctx) {

        if (content.isEmpty()) {

            return ctx.error("Content cannot be empty");
        }

        return ctx;
    }

    @Validator(method = "downloadFile")
    public static ValidationContext downloadFile(DownloadRequest msg, ValidationContext ctx) {

        ctx = ctx.push(DR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(DR_OBJECT_ID)
                .apply(CommonValidators::required)
                .apply(CommonValidators::uuid)
                .pop();

        ctx = ctx.push(DR_OBJECT_VERSION)
                .apply(CommonValidators::required)
                .apply(CommonValidators::positive, Integer.class)
                .pop();

        return ctx;
    }

    @Validator(method = "downloadLatestFile")
    public static ValidationContext downloadLatestFile(DownloadRequest msg, ValidationContext ctx) {

        ctx = ctx.push(DR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(DR_OBJECT_ID)
                .apply(CommonValidators::required)
                .apply(CommonValidators::uuid)
                .pop();

        ctx = ctx.push(DR_OBJECT_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        return ctx;
    }
}
