/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.validation.core;

import com.google.common.reflect.ClassPath;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.validation.fixed.MetadataValidator;
import org.finos.tracdap.common.validation.version.SchemaVersionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


public class ValidatorBuilder {

    private static final Logger log = LoggerFactory.getLogger(ValidatorBuilder.class);

    private static final String GET_DESCRIPTOR_METHOD = "getDescriptor";

    public static Map<ValidationKey, ValidationFunction<?>> buildValidatorMap() {

        var fixedValidatorPackage = MetadataValidator.class.getPackage();
        var fixedValidators = scanPackage(fixedValidatorPackage);

        var versionValidatorPackage = SchemaVersionValidator.class.getPackage();
        var versionValidators = scanPackage(versionValidatorPackage);

        var allValidators = new HashMap<ValidationKey, ValidationFunction<?>>();
        allValidators.putAll(fixedValidators);
        allValidators.putAll(versionValidators);

        return allValidators;
    }

    public static Map<ValidationKey, ValidationFunction<?>> scanPackage(Package package_) {

        try {

            var validatorMap = new HashMap<ValidationKey, ValidationFunction<?>>();

            var classPath = ClassPath.from(ValidatorBuilder.class.getClassLoader());
            var classes = classPath.getTopLevelClasses(package_.getName());

            for (var classInfo : classes) {

                var class_ = classInfo.load();

                if (!class_.isAnnotationPresent(Validator.class))
                    continue;

                var validators = scanClass(class_);
                validatorMap.putAll(validators);
            }

            return validatorMap;
        }
        catch (IOException e) {

            log.error("Failed to set up validation: Error scanning validation classes", e);
            throw new EUnexpected(e);
        }
    }

    public static Map<ValidationKey, ValidationFunction<?>> scanClass(Class<?> class_) {

        var validatorMap = new HashMap<ValidationKey, ValidationFunction<?>>();

        if (!class_.isAnnotationPresent(Validator.class))
            throw new EUnexpected();

        var classAnnotation = class_.getAnnotation(Validator.class);

        for (var method : class_.getDeclaredMethods()) {

            if (!method.isAnnotationPresent(Validator.class))
                continue;

            var methodAnnotation = method.getAnnotation(Validator.class);
            var methodParams = method.getParameterTypes();

            // First parameter of a validation func must be a message of the type being validated
            // There must also be a validation ctx, so at least 2 params for any validation
            if (methodParams.length < 2 || !Message.class.isAssignableFrom(methodParams[0]))
                throw new EUnexpected();

            var validationType = methodAnnotation.type() != ValidationType.UNDEFINED
                    ? methodAnnotation.type()
                    : classAnnotation.type();

            var objectType =
                    methodAnnotation.object() != Message.class ? methodAnnotation.object() :
                    classAnnotation.object() != Message.class ? classAnnotation.object() :
                    methodParams[0];

            var serviceFile = methodAnnotation.serviceFile() != Object.class
                    ? methodAnnotation.serviceFile()
                    : classAnnotation.serviceFile();

            var serviceName = !methodAnnotation.serviceName().isEmpty()
                    ? methodAnnotation.serviceName()
                    : classAnnotation.serviceName();

            var methodName = !methodAnnotation.method().isEmpty()
                    ? methodAnnotation.method()
                    : classAnnotation.method();


            Map.Entry<ValidationKey, ValidationFunction<?>> validator;

            if (validationType == ValidationType.FIXED && methodName.isEmpty())
                validator = buildObjectValidator(objectType, method);

            else if (validationType == ValidationType.FIXED)
                validator = buildMethodValidator(serviceFile, serviceName, methodName, objectType, method);

            else if (validationType == ValidationType.VERSION)
                validator = buildVersionValidator(objectType, method);

            else
                throw new EUnexpected();

            validatorMap.put(validator.getKey(), validator.getValue());
        }

        return validatorMap;
    }

    private static Map.Entry<ValidationKey, ValidationFunction<?>> buildObjectValidator(
            Class<?> objectType,
            Method validationFunc) {

        try {

            var descriptorFunc = objectType.getMethod(GET_DESCRIPTOR_METHOD);
            var descriptor = (Descriptors.Descriptor) descriptorFunc.invoke(null);

            var key = ValidationKey.forObject(descriptor);
            var func = ValidationFunction.makeTyped(validationFunc, objectType);

            return Map.entry(key, func);

        }
        catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {

            log.error("Failed to set up validation: Descriptor lookup failed", e);
            throw new EUnexpected(e);
        }
    }

    private static Map.Entry<ValidationKey, ValidationFunction<?>> buildMethodValidator(
            Class<?> serviceFile, String serviceName, String methodName,
            Class<?> objectType, Method validationFunc) {

        try {

            var fileDescriptorFunc = serviceFile.getMethod(GET_DESCRIPTOR_METHOD);
            var fileDescriptor = (Descriptors.FileDescriptor) fileDescriptorFunc.invoke(null);
            var serviceDescriptor = fileDescriptor.findServiceByName(serviceName);
            var serviceMethod = serviceDescriptor.findMethodByName(methodName);

            // Sanity check - make sure we get the same descriptor on the objectType
            var descriptorFunc = objectType.getMethod(GET_DESCRIPTOR_METHOD);
            var descriptor = (Descriptors.Descriptor) descriptorFunc.invoke(null);

            if (!descriptor.equals(serviceMethod.getInputType()))
                throw new EUnexpected();

            var key = ValidationKey.forMethod(serviceMethod.getInputType(), serviceMethod);
            var func = ValidationFunction.makeTyped(validationFunc, objectType);

            return Map.entry(key, func);
        }
        catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {

            log.error("Failed to set up validation: Descriptor lookup failed", e);
            throw new EUnexpected(e);
        }
    }

    private static Map.Entry<ValidationKey, ValidationFunction<?>> buildVersionValidator(
            Class<?> objectType,
            Method validationFunc) {

        try {

            var descriptorFunc = objectType.getMethod(GET_DESCRIPTOR_METHOD);
            var descriptor = (Descriptors.Descriptor) descriptorFunc.invoke(null);

            var key = ValidationKey.forVersion(descriptor);
            var func = ValidationFunction.makeVersion(validationFunc, objectType);

            return Map.entry(key, func);
        }
        catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {

            log.error("Failed to set up validation: Descriptor lookup failed", e);
            throw new EUnexpected(e);
        }
    }
}
