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

import com.accenture.trac.api.Data;
import com.accenture.trac.api.FileReadRequest;
import com.accenture.trac.api.FileWriteRequest;
import com.accenture.trac.svc.data.validation.core.ValidationFunction;
import com.accenture.trac.svc.data.validation.core.ValidationKey;
import com.accenture.trac.svc.data.validation.fixed.DataApiValidator;
import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.Map;

public class ValidatorBuilder {

    public static Map<ValidationKey, ValidationFunction<?>> buildValidatorMap() {

        // Quick static implementation
        // Can be replaced with annotations / reflection when more validators are added
        // Should use a single static initialization in that case

        var validatorMap = new HashMap<ValidationKey, ValidationFunction<?>>();

        var dataProto = Data.getDescriptor();

        addValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateCreateFile,
                dataProto, "TracDataApi", "createFile");

        addValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateUpdateFile,
                dataProto, "TracDataApi", "updateFile");

        addValidator(validatorMap, FileReadRequest.class, DataApiValidator::validateReadFile,
                dataProto, "TracDataApi", "readFile");

        return validatorMap;
    }

    private static <T> void addValidator(
            Map<ValidationKey, ValidationFunction<?>> validatorMap,
            Class<T> targetClass, ValidationFunction.Typed<T> validator,
            Descriptors.FileDescriptor file, String serviceName, String methodName) {

        var method = file.findServiceByName(serviceName).findMethodByName(methodName);
        var requestType = method.getInputType();

        var key = ValidationKey.fixed(requestType, method);
        var func = new ValidationFunction<T>(validator, targetClass);

        validatorMap.put(key, func);
    }

}
