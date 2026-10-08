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
import org.finos.tracdap.metadata.ExternalLocation;

import com.google.protobuf.Descriptors;

import java.util.regex.Pattern;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class ExternalLocationValidator {

    private static final Pattern PATH_EXTENSION = Pattern.compile(".*\\.([^./\\\\]+)\\Z");

    private static final Descriptors.Descriptor EXTERNAL_LOCATION;
    private static final Descriptors.FieldDescriptor EL_STORAGE_KEY;
    private static final Descriptors.FieldDescriptor EL_STORAGE_PATH;
    private static final Descriptors.FieldDescriptor EL_PROTOCOL;
    private static final Descriptors.FieldDescriptor EL_LOCATION_DETAILS;

    static {

        EXTERNAL_LOCATION = ExternalLocation.getDescriptor();
        EL_STORAGE_KEY = field(EXTERNAL_LOCATION, ExternalLocation.STORAGEKEY_FIELD_NUMBER);
        EL_STORAGE_PATH = field(EXTERNAL_LOCATION, ExternalLocation.STORAGEPATH_FIELD_NUMBER);
        EL_PROTOCOL = field(EXTERNAL_LOCATION, ExternalLocation.PROTOCOL_FIELD_NUMBER);
        EL_LOCATION_DETAILS = field(EXTERNAL_LOCATION, ExternalLocation.LOCATIONDETAILS_FIELD_NUMBER);
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
                .apply(ExternalLocationValidator::dataFileName)
                .pop();

        return ctx;
    }

    // Protocol and location details are recorded by the platform when a job is submitted
    public static ValidationContext locationDetailsMustBeEmpty(ExternalLocation msg, ValidationContext ctx) {

        if (!msg.getProtocol().isEmpty()) {
            ctx = ctx.push(EL_PROTOCOL)
                    .error("The protocol of an external location is set by the platform and cannot be supplied")
                    .pop();
        }

        if (msg.getLocationDetailsCount() > 0) {
            ctx = ctx.pushMap(EL_LOCATION_DETAILS)
                    .error("The location details of an external location are set by the platform and cannot be supplied")
                    .pop();
        }

        return ctx;
    }

    private static ValidationContext dataFileName(String storagePath, ValidationContext ctx) {

        var segments = storagePath.split(ValidationConstants.PATH_SEPARATORS.pattern());
        var fileName = segments[segments.length - 1];

        ctx = CommonValidators.fileName(fileName, ctx);

        if (ctx.failed())
            return ctx;

        var extension = fileExtension(storagePath);

        if (extension == null || !ValidationConstants.EXTERNAL_FILE_FORMATS.containsKey(extension)) {

            var err = String.format(
                    "File [%s] is not a supported data file, the file name must end in one of %s",
                    fileName, ValidationConstants.EXTERNAL_FILE_FORMATS.keySet().stream().sorted().map(e -> "." + e).toList());

            return ctx.error(err);
        }

        return ctx;
    }

    public static String fileExtension(String storagePath) {

        var match = PATH_EXTENSION.matcher(storagePath);

        return match.matches() ? match.group(1).toLowerCase() : null;
    }
}
