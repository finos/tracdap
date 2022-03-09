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

import com.accenture.trac.common.validation.core.ValidationContext;
import org.finos.tracdap.metadata.FileDefinition;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagSelector;
import com.google.protobuf.Descriptors;

import java.util.regex.Pattern;


public class FileValidator {

    private static final Pattern EXT_PATTERN = Pattern.compile(".*\\.([^./\\\\]+)");

    private static final Descriptors.Descriptor FILE_DEF;
    private static final Descriptors.FieldDescriptor FD_NAME;
    private static final Descriptors.FieldDescriptor FD_EXTENSION;
    private static final Descriptors.FieldDescriptor FD_MIME_TYPE;
    private static final Descriptors.FieldDescriptor FD_SIZE;
    private static final Descriptors.FieldDescriptor FD_STORAGE_ID;

    static {
        FILE_DEF = FileDefinition.getDescriptor();
        FD_NAME = field(FILE_DEF, FileDefinition.NAME_FIELD_NUMBER);
        FD_EXTENSION = field(FILE_DEF, FileDefinition.EXTENSION_FIELD_NUMBER);
        FD_MIME_TYPE = field(FILE_DEF, FileDefinition.MIMETYPE_FIELD_NUMBER);
        FD_SIZE = field(FILE_DEF, FileDefinition.SIZE_FIELD_NUMBER);
        FD_STORAGE_ID = field(FILE_DEF, FileDefinition.STORAGEID_FIELD_NUMBER);
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
    }

    public static ValidationContext file(FileDefinition msg, ValidationContext ctx) {

        ctx = ctx.push(FD_NAME)
                .apply(Validation::required)
                .apply(Validation::fileName)
                .pop();

        ctx = ctx.push(FD_EXTENSION)
                .applyWith(FileValidator::extensionMatchesName, String.class, msg.getName())
                .pop();

        ctx = ctx.push(FD_MIME_TYPE)
                .apply(Validation::required)
                .apply(Validation::mimeType)
                .pop();

        ctx = ctx.push(FD_SIZE)
                .apply(Validation::optional)
                .apply(Validation::notNegative, Long.class)
                .pop();

        ctx = ctx.push(FD_STORAGE_ID)
                .apply(Validation::required)
                .apply(MetadataValidator::validateTagSelector, TagSelector.class)
                .apply(Validation.selectorType(ObjectType.STORAGE), TagSelector.class)
                .apply(FileValidator::selectorForLatest, TagSelector.class)
                .pop();

        return ctx;
    }

    private static ValidationContext extensionMatchesName(String extension, String fileName, ValidationContext ctx) {

        var nameExtMatch = EXT_PATTERN.matcher(fileName);

        if (nameExtMatch.matches()) {

            var nameExt = nameExtMatch.group(1);

            if (!nameExt.equals(extension)) {

                var err = String.format(
                        "Extension does not match file name: extension = [%s], name ends with [%s]",
                        extension, nameExtMatch.group());

                ctx = ctx.error(err);
            }
        }

        return ctx;
    }

    private static ValidationContext selectorForLatest(TagSelector selector, ValidationContext ctx) {

        if (!selector.getLatestObject() || !selector.getLatestTag()) {

            ctx = ctx.error("File storage selector must refer to the latest object and tag version of the storage object");
        }

        return ctx;
    }
}
