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
import com.google.protobuf.Message;


public class DataApiValidator {

    public static ValidationContext validateCreateFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(msg, "tenant")
                .apply(Validation::required)
                .apply(Validation::identifier)
                .pop();

        ctx = ctx.push(msg, "priorVersion")
                .apply(Validation::omitted)
                .pop();

        return createOrUpdateFile(msg, ctx);
    }

    public static ValidationContext validateUpdateFile(FileWriteRequest msg, ValidationContext ctx) {

        ctx = ctx.push(msg, "tenant")
                .apply(Validation::required)
                .apply(Validation::identifier)
                .pop();

        ctx = ctx.push(msg, "priorVersion")
                .apply(Validation::required)
                .pop();

        return createOrUpdateFile(msg, ctx);
    }

    private static ValidationContext createOrUpdateFile(FileWriteRequest msg, ValidationContext ctx) {

        // Todo: tag updates

        ctx = ctx.push(msg, "name")
                .apply(Validation::required)
                //.apply(Validation::fileName)
                .pop();

        ctx = ctx.push(msg, "mimeType")
                .apply(Validation::required)
                .apply(Validation::mimeType)
                .pop();

        ctx = ctx.push(msg, "size")
                .apply(Validation::optional)
                .apply(Validation::nonNegative)
                .pop();

        return ctx;
    }

    public static ValidationContext validateReadFile(FileReadRequest msg, ValidationContext ctx) {

        ctx = ctx.push(msg, "tenant")
                .apply(Validation::required)
                .apply(Validation::identifier)
                .pop();

        ctx = ctx.push(msg, "selector")
                .apply(Validation::required)
                .pop();

        return ctx;
    }
}
