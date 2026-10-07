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

import org.finos.tracdap.common.validation.ValidationConstants;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.regex.Pattern;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class CaptureValidator {

    private static final Pattern PATH_EXTENSION = Pattern.compile(".*\\.([^./\\\\]+)\\Z");

    private static final Descriptors.Descriptor CAPTURE_SOURCE;
    private static final Descriptors.OneofDescriptor CS_SOURCE;
    private static final Descriptors.FieldDescriptor CS_LOCATION;
    private static final Descriptors.FieldDescriptor CS_SCHEMA_SOURCE;
    private static final Descriptors.OneofDescriptor CS_SCHEMA_SPECIFIER;
    private static final Descriptors.FieldDescriptor CS_SCHEMA_ID;
    private static final Descriptors.FieldDescriptor CS_SCHEMA;
    private static final Descriptors.FieldDescriptor CS_DATA_ATTRS;
    private static final Descriptors.FieldDescriptor CS_FILE_ATTRS;

    private static final Descriptors.Descriptor EXTERNAL_LOCATION;
    private static final Descriptors.FieldDescriptor EL_STORAGE_KEY;
    private static final Descriptors.FieldDescriptor EL_STORAGE_PATH;

    static {

        CAPTURE_SOURCE = CaptureSource.getDescriptor();
        CS_LOCATION = field(CAPTURE_SOURCE, CaptureSource.LOCATION_FIELD_NUMBER);
        CS_SOURCE = CS_LOCATION.getContainingOneof();
        CS_SCHEMA_SOURCE = field(CAPTURE_SOURCE, CaptureSource.SCHEMASOURCE_FIELD_NUMBER);
        CS_SCHEMA_ID = field(CAPTURE_SOURCE, CaptureSource.SCHEMAID_FIELD_NUMBER);
        CS_SCHEMA = field(CAPTURE_SOURCE, CaptureSource.SCHEMA_FIELD_NUMBER);
        CS_SCHEMA_SPECIFIER = CS_SCHEMA_ID.getContainingOneof();
        CS_DATA_ATTRS = field(CAPTURE_SOURCE, CaptureSource.DATAATTRS_FIELD_NUMBER);
        CS_FILE_ATTRS = field(CAPTURE_SOURCE, CaptureSource.FILEATTRS_FIELD_NUMBER);

        EXTERNAL_LOCATION = ExternalLocation.getDescriptor();
        EL_STORAGE_KEY = field(EXTERNAL_LOCATION, ExternalLocation.STORAGEKEY_FIELD_NUMBER);
        EL_STORAGE_PATH = field(EXTERNAL_LOCATION, ExternalLocation.STORAGEPATH_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext captureSource(CaptureSource msg, ValidationContext ctx) {

        ctx = ctx.pushOneOf(CS_SOURCE)
                .apply(CommonValidators::required)
                .applyOneOf(CS_LOCATION, CaptureValidator::externalLocation, ExternalLocation.class)
                .pop();

        ctx = ctx.push(CS_SCHEMA_SOURCE)
                .apply(CommonValidators::recognizedEnum, CaptureSchemaSource.class)
                .pop();

        if (msg.getSchemaSource() == CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED) {

            ctx = ctx.pushOneOf(CS_SCHEMA_SPECIFIER)
                    .apply(CommonValidators::required)
                    .applyOneOf(CS_SCHEMA_ID, ObjectIdValidator::tagSelector, TagSelector.class)
                    .applyOneOf(CS_SCHEMA_ID, ObjectIdValidator::selectorType, TagSelector.class, ObjectType.SCHEMA)
                    .pop();

            if (msg.hasSchema()) {
                ctx = ctx.push(CS_SCHEMA)
                        .error("An inline schema is not supported for captures, declare the schema with a SCHEMA object (schemaId)")
                        .pop();
            }
        }

        else if (msg.getSchemaSource() == CaptureSchemaSource.CAPTURE_SCHEMA_FILE) {

            if (msg.hasSchemaId() || msg.hasSchema()) {
                ctx = ctx.pushOneOf(CS_SCHEMA_SPECIFIER)
                        .error("A capture that uses the file's own schema cannot also declare a schema")
                        .pop();
            }

            if (msg.hasLocation() && "csv".equals(fileExtension(msg.getLocation().getStoragePath()))) {
                ctx = ctx.push(CS_SCHEMA_SOURCE)
                        .error("A CSV file has no schema of its own, a capture of a CSV file needs a declared schema")
                        .pop();
            }
        }

        ctx = ctx.pushRepeated(CS_DATA_ATTRS)
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                .applyRepeated(TagUpdateValidator::reservedAttrs, TagUpdate.class, false)
                .pop();

        ctx = ctx.pushRepeated(CS_FILE_ATTRS)
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                .applyRepeated(TagUpdateValidator::reservedAttrs, TagUpdate.class, false)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext externalLocation(ExternalLocation msg, ValidationContext ctx) {

        ctx = ctx.push(EL_STORAGE_KEY)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(EL_STORAGE_PATH)
                .apply(CommonValidators::required)
                .apply(CommonValidators::relativePath)
                .apply(CaptureValidator::captureFileName)
                .pop();

        return ctx;
    }

    private static ValidationContext captureFileName(String storagePath, ValidationContext ctx) {

        var segments = storagePath.split(ValidationConstants.PATH_SEPARATORS.pattern());
        var fileName = segments[segments.length - 1];

        ctx = CommonValidators.fileName(fileName, ctx);

        if (ctx.failed())
            return ctx;

        var extension = fileExtension(storagePath);

        if (extension == null || !ValidationConstants.CAPTURE_FORMATS.containsKey(extension)) {

            var err = String.format(
                    "File [%s] cannot be captured, the file name must end in one of %s",
                    fileName, ValidationConstants.CAPTURE_FORMATS.keySet().stream().sorted().map(e -> "." + e).toList());

            return ctx.error(err);
        }

        return ctx;
    }

    public static String fileExtension(String storagePath) {

        var match = PATH_EXTENSION.matcher(storagePath);

        return match.matches() ? match.group(1).toLowerCase() : null;
    }
}
