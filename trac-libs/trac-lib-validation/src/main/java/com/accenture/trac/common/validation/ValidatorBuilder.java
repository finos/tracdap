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

package com.accenture.trac.common.validation;

import com.accenture.trac.api.*;
import com.accenture.trac.common.validation.version.SchemaVersionValidator;
import com.accenture.trac.metadata.FileDefinition;
import com.accenture.trac.common.validation.core.ValidationFunction;
import com.accenture.trac.common.validation.core.ValidationKey;
import com.accenture.trac.common.validation.fixed.DataApiValidator;
import com.accenture.trac.common.validation.version.FileVersionValidator;
import com.accenture.trac.metadata.SchemaDefinition;
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

        addValidator(validatorMap, DataWriteRequest.class, DataApiValidator::validateCreateDataset,
                dataProto, "TracDataApi", "createDataset");

        addValidator(validatorMap, DataWriteRequest.class, DataApiValidator::validateCreateDataset,
                dataProto, "TracDataApi", "createSmallDataset");

        addValidator(validatorMap, DataWriteRequest.class, DataApiValidator::validateUpdateDataset,
                dataProto, "TracDataApi", "updateDataset");

        addValidator(validatorMap, DataWriteRequest.class, DataApiValidator::validateUpdateDataset,
                dataProto, "TracDataApi", "updateSmallDataset");

        addValidator(validatorMap, DataReadRequest.class, DataApiValidator::validateReadDataset,
                dataProto, "TracDataApi", "readDataset");

        addValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateCreateFile,
                dataProto, "TracDataApi", "createFile");

        addValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateCreateFile,
                dataProto, "TracDataApi", "createSmallFile");

        addValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateUpdateFile,
                dataProto, "TracDataApi", "updateFile");

        addValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateUpdateFile,
                dataProto, "TracDataApi", "updateSmallFile");

        addValidator(validatorMap, FileReadRequest.class, DataApiValidator::validateReadFile,
                dataProto, "TracDataApi", "readFile");

        addVersionValidator(validatorMap, FileDefinition.class, FileVersionValidator::fileVersion,
                FileDefinition.getDescriptor());

        addVersionValidator(validatorMap, SchemaDefinition.class, SchemaVersionValidator::schema,
                SchemaDefinition.getDescriptor());

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

    private static <T> void addVersionValidator(
            Map<ValidationKey, ValidationFunction<?>> validatorMap,
            Class<T> targetClass, ValidationFunction.Version<T> validator,
            Descriptors.Descriptor messageType) {

        var key = ValidationKey.version(messageType);
        var func = new ValidationFunction<T>(validator, targetClass);

        validatorMap.put(key, func);

    }

}
