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

package org.finos.tracdap.common.validation.version;

import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.FileDefinition;
import org.finos.tracdap.common.validation.core.ValidationContext;
import com.google.protobuf.*;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.VERSION)
public class FileVersionValidator {

    private static final Descriptors.Descriptor FILE_DEF;
    private static final Descriptors.FieldDescriptor FILE_NAME;
    private static final Descriptors.FieldDescriptor FILE_EXTENSION;
    private static final Descriptors.FieldDescriptor FILE_MIME_TYPE;
    private static final Descriptors.FieldDescriptor FILE_STORAGE_ID;

    static {
        FILE_DEF = FileDefinition.getDescriptor();
        FILE_NAME = field(FILE_DEF, FileDefinition.NAME_FIELD_NUMBER);
        FILE_EXTENSION = field(FILE_DEF, FileDefinition.EXTENSION_FIELD_NUMBER);
        FILE_MIME_TYPE = field(FILE_DEF, FileDefinition.MIMETYPE_FIELD_NUMBER);
        FILE_STORAGE_ID = field(FILE_DEF, FileDefinition.STORAGEID_FIELD_NUMBER);
    }


    @Validator
    public static ValidationContext fileVersion(FileDefinition current, FileDefinition prior, ValidationContext ctx) {

        ctx = ctx.push(FILE_NAME)
                .apply(FileVersionValidator::sameExtension, String.class)
                .pop();

        ctx = ctx.push(FILE_EXTENSION)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(FILE_MIME_TYPE)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(FILE_STORAGE_ID)
                .apply(CommonValidators::exactMatch)
                .pop();

        return ctx;
    }

    static ValidationContext sameExtension(String currentName, String priorName, ValidationContext ctx) {

        var priorExt = priorName.contains(".")
                ? priorName.substring(priorName.lastIndexOf('.'))
                : "";

        var currentExt = currentName.contains(".")
                ? currentName.substring(currentName.lastIndexOf('.'))
                : "";

        if (!priorExt.equals(currentExt)) {

            var err = String.format(
                    "File extension in field [%s] changed between versions: prior = [%s], new = [%s]",
                    ctx.fieldName(), priorExt, currentExt);

            return ctx.error(err);
        }

        return ctx;
    }
}
