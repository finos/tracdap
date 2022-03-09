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
import com.accenture.trac.common.validation.fixed.FileValidator;
import com.accenture.trac.common.validation.fixed.SchemaValidator;
import com.accenture.trac.common.validation.version.DataVersionValidator;
import com.accenture.trac.common.validation.version.SchemaVersionValidator;
import com.accenture.trac.metadata.DataDefinition;
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

        addObjectValidator(validatorMap, SchemaDefinition.class, SchemaValidator::schema, SchemaDefinition.getDescriptor());
        addObjectValidator(validatorMap, FileDefinition.class, FileValidator::file, FileDefinition.getDescriptor());

        addMethodValidator(validatorMap, DataWriteRequest.class, DataApiValidator::validateCreateDataset,
                dataProto, "TracDataApi", "createDataset");

        addMethodValidator(validatorMap, DataWriteRequest.class, DataApiValidator::validateCreateDataset,
                dataProto, "TracDataApi", "createSmallDataset");

        addMethodValidator(validatorMap, DataWriteRequest.class, DataApiValidator::validateUpdateDataset,
                dataProto, "TracDataApi", "updateDataset");

        addMethodValidator(validatorMap, DataWriteRequest.class, DataApiValidator::validateUpdateDataset,
                dataProto, "TracDataApi", "updateSmallDataset");

        addMethodValidator(validatorMap, DataReadRequest.class, DataApiValidator::validateReadDataset,
                dataProto, "TracDataApi", "readDataset");

        addMethodValidator(validatorMap, DataReadRequest.class, DataApiValidator::validateReadDataset,
                dataProto, "TracDataApi", "readSmallDataset");

        addMethodValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateCreateFile,
                dataProto, "TracDataApi", "createFile");

        addMethodValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateCreateFile,
                dataProto, "TracDataApi", "createSmallFile");

        addMethodValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateUpdateFile,
                dataProto, "TracDataApi", "updateFile");

        addMethodValidator(validatorMap, FileWriteRequest.class, DataApiValidator::validateUpdateFile,
                dataProto, "TracDataApi", "updateSmallFile");

        addMethodValidator(validatorMap, FileReadRequest.class, DataApiValidator::validateReadFile,
                dataProto, "TracDataApi", "readFile");

        addMethodValidator(validatorMap, FileReadRequest.class, DataApiValidator::validateReadFile,
                dataProto, "TracDataApi", "readSmallFile");

        addVersionValidator(validatorMap, FileDefinition.class, FileVersionValidator::fileVersion,
                FileDefinition.getDescriptor());

        addVersionValidator(validatorMap, SchemaDefinition.class, SchemaVersionValidator::schema,
                SchemaDefinition.getDescriptor());

        addVersionValidator(validatorMap, DataDefinition.class, DataVersionValidator::data,
                DataDefinition.getDescriptor());

        return validatorMap;
    }

    private static <T> void addObjectValidator(
            Map<ValidationKey, ValidationFunction<?>> validatorMap,
            Class<T> targetClass, ValidationFunction.Typed<T> validator,
            Descriptors.Descriptor messageType) {

        var key = ValidationKey.fixedObject(messageType);
        var func = new ValidationFunction<>(validator, targetClass);

        validatorMap.put(key, func);
    }


    private static <T> void addMethodValidator(
            Map<ValidationKey, ValidationFunction<?>> validatorMap,
            Class<T> targetClass, ValidationFunction.Typed<T> validator,
            Descriptors.FileDescriptor file, String serviceName, String methodName) {

        var method = file.findServiceByName(serviceName).findMethodByName(methodName);
        var requestType = method.getInputType();

        var key = ValidationKey.fixedMethod(requestType, method);
        var func = new ValidationFunction<>(validator, targetClass);

        validatorMap.put(key, func);
    }

    private static <T> void addVersionValidator(
            Map<ValidationKey, ValidationFunction<?>> validatorMap,
            Class<T> targetClass, ValidationFunction.Version<T> validator,
            Descriptors.Descriptor messageType) {

        var key = ValidationKey.version(messageType);
        var func = new ValidationFunction<>(validator, targetClass);

        validatorMap.put(key, func);

    }

}
