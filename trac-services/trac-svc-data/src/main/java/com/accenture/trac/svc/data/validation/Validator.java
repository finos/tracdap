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
import com.accenture.trac.common.exception.EInputValidation;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.svc.data.validation.core.ValidationContext;
import com.accenture.trac.svc.data.validation.core.ValidationResult;
import com.accenture.trac.svc.data.validation.fixed.DataApiValidator;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Validator {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public ValidationResult validateObject(Message msg) {

        return ValidationResult.pass();
    }

    public void validateApiCall(Message msg, Descriptors.MethodDescriptor method) {

        var ctx = ValidationContext.forApiCall(method, msg);
        validateApiCall(msg, method, ctx);
    }

    public void validateVersion(Message msg, Message original) {

        ValidationResult.pass();
    }

    private void validateApiCall(Message msg, Descriptors.MethodDescriptor method, ValidationContext ctx) {

        var methodName = method.getName();

        if (methodName == null)
            throw new EUnexpected();

        log.info("VALIDATION START: [{}]", methodName);

        if (methodName.equals("createFile") && msg instanceof FileWriteRequest)
            ctx = DataApiValidator.validateCreateFile((FileWriteRequest) msg, ctx);

        else if (methodName.equals("updateFile") && msg instanceof FileWriteRequest)
            ctx = DataApiValidator.validateUpdateFile((FileWriteRequest) msg, ctx);

        else if (methodName.equals("readFile") && msg instanceof FileReadRequest)
            ctx = DataApiValidator.validateReadFile((FileReadRequest) msg, ctx);

        else
            throw new ETracInternal("Missing required validators");

        var result = ValidationResult.forContext(ctx);

        if (!result.ok()) {

            log.error("VALIDATION FAILED: [{}]", methodName);

            for (var failure: result.failures())
                log.error(failure.message());

            throw new EInputValidation(result.failureMessage());
        }

        log.info("VALIDATION SUCCEEDED: [{}]", methodName);
    }

}
