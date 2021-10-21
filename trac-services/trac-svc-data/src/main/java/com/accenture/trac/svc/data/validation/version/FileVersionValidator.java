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

package com.accenture.trac.svc.data.validation.version;

import com.accenture.trac.metadata.FileDefinition;
import com.accenture.trac.svc.data.validation.core.ValidationContext;
import com.google.protobuf.*;

import java.util.Objects;


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

    public static ValidationContext fileVersion(FileDefinition current, FileDefinition prior, ValidationContext ctx) {

        ctx = ctx.push(FILE_NAME)
                .apply(FileVersionValidator::sameExtension, String.class)
                .pop();

        ctx = ctx.push(FILE_EXTENSION)
                .apply(FileVersionValidator::exactMatch)
                .pop();

        ctx = ctx.push(FILE_MIME_TYPE)
                .apply(FileVersionValidator::exactMatch)
                .pop();

        ctx = ctx.push(FILE_STORAGE_ID)
                .apply(FileVersionValidator::exactMatch)
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
                    "File extension in field [%s] must not change between versions: prior = [%s], current = [%s]",
                    ctx.fieldName(), priorExt, currentExt);

            return ctx.error(err);
        }

        return ctx;
    }

    static ValidationContext exactMatch(Object current, Object prior, ValidationContext ctx) {

        var equal = Objects.equals(prior, current);

        if (!equal) {

            var err = String.format(
                    "Value of field [%s] must not change between versions: prior = [%s], current = [%s]",
                    ctx.fieldName(), prior.toString(), current.toString());

            return ctx.error(err);
        }

        return ctx;
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
    }
}
