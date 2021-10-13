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

import com.accenture.trac.api.FileWriteRequest;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;


public class Validator {

    public ValidationResult validateObject(Message msg) {

        return ValidationResult.pass();
    }

    public ValidationResult validateApiCall(Message msg, MethodDescriptor<?, ?> method) {

        var ctx = ValidationContext.newContext();
        return validateApiCall(msg, method, ctx);
    }

    public ValidationResult validateVersion(Message msg, Message original) {

        return ValidationResult.pass();
    }

    private ValidationResult validateApiCall(Message msg, MethodDescriptor<?, ?> method, ValidationContext ctx) {

        var methodName = method.getBareMethodName();

        if (methodName == null)
            throw new EUnexpected();

        if (methodName.equals("createFile") && msg instanceof FileWriteRequest) {
            ctx = DataApiValidator.validateCreateFile((FileWriteRequest) msg, ctx);
            return ValidationResult.forContext(ctx);
        }

        if (methodName.equals("updateFile") && msg instanceof FileWriteRequest) {
            ctx = DataApiValidator.validateUpdateFile((FileWriteRequest) msg, ctx);
            return ValidationResult.forContext(ctx);
        }

        throw new ETracInternal("Missing required validators");
    }

}
