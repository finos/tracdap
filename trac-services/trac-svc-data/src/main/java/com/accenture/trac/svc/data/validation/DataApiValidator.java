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

package com.accenture.trac.svc.data.validation;

import com.accenture.trac.api.FileReadRequest;
import com.accenture.trac.api.FileWriteRequest;
import com.accenture.trac.api.TracDataApiGrpc;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;


public class DataApiValidator {

    private static final String CREATE_FILE_METHOD = TracDataApiGrpc.getCreateFileMethod().getFullMethodName();
    private static final String UPDATE_FILE_METHOD = TracDataApiGrpc.getUpdateFileMethod().getFullMethodName();

    public static ValidationContext validateCreateFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = required(msg, "tenant", ctx);
        ctx = validIdentifier(msg, "tenant", ctx);

        ctx = omitted(msg, "priorVersion", ctx);

        ctx = createOrUpdateFile(msg, ctx);

        return ctx;
    }

    public static ValidationContext validateUpdateFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = required(msg, "tenant", ctx);
        ctx = validIdentifier(msg, "tenant", ctx);

        ctx = required(msg, "priorVersion", ctx);

        ctx = createOrUpdateFile(msg, ctx);

        return ctx;
    }

    private static ValidationContext createOrUpdateFile(FileWriteRequest msg, ValidationContext ctx) {

        // Todo: tag updates

        ctx = required(msg, "name", ctx);
        ctx = validFileName(msg, "name", ctx);

        ctx = required(msg, "mimeType", ctx);
        ctx = validMimeType(msg, "mimeType", ctx);

        ctx = optional(msg, "size", ctx);
        ctx = nonNegative(msg, "size", ctx);

        return ctx;
    }

    public static ValidationContext validateReadFile(FileReadRequest msg, ValidationContext ctx) {

        ctx = required(msg, "tenant", ctx);
        ctx = validIdentifier(msg, "tenant", ctx);

        ctx = required(msg, "selector", ctx);

        return ctx;
    }

    private static ValidationContext required(Message msg, String field, ValidationContext ctx) {

        var fd = msg.getDescriptorForType().findFieldByName(field);

        if (!msg.hasField(fd)) {
            var err = String.format("Missing required field: [%s]", field);
            return ctx.error(err);
        }

        if (fd.getType() == Descriptors.FieldDescriptor.Type.STRING) {

            var value = (String) msg.getField(fd);

            if (value.isEmpty()) {
                var err = String.format("Missing required field: [%s]", field);
                return ctx.error(err);
            }
        }

        return ctx;
    }

    private static ValidationContext omitted(Message msg, String field, ValidationContext ctx) {

        var fd = msg.getDescriptorForType().findFieldByName(field);

        if (msg.hasField(fd)) {
            var err = String.format("Field must not be provided: [%s]", field);
            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext optional(Message msg, String field, ValidationContext ctx) {

        return ctx;
    }

    private static ValidationContext validIdentifier(Message msg, String field, ValidationContext ctx) {

        msg.getField(msg.getDescriptorForType().findFieldByName(field));
        return ctx;
    }

    private static ValidationContext validFileName(Message msg, String field, ValidationContext ctx) {

        msg.getField(msg.getDescriptorForType().findFieldByName(field));
        return ctx;
    }

    private static ValidationContext validMimeType(Message msg, String field, ValidationContext ctx) {

        msg.getField(msg.getDescriptorForType().findFieldByName(field));
        return ctx;
    }

    private static ValidationContext nonNegative(Message msg, String field, ValidationContext ctx) {

        return ctx;
    }
}
