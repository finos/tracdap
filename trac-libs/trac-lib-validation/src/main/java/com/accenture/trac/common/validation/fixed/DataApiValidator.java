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

import com.accenture.trac.api.FileReadRequest;
import com.accenture.trac.api.FileWriteRequest;
import com.accenture.trac.metadata.ObjectType;
import com.accenture.trac.metadata.TagSelector;
import com.accenture.trac.metadata.TagUpdate;
import com.accenture.trac.common.validation.core.ValidationContext;
import com.google.protobuf.Descriptors;


public class DataApiValidator {

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
                .applyTypedList(MetadataValidator::validateTagUpdate, TagUpdate.class)
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
