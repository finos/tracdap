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

import com.accenture.trac.common.exception.EInputValidation;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.exception.EVersionValidation;
import com.accenture.trac.metadata.ObjectDefinition;
import com.accenture.trac.metadata.TagHeader;
import com.accenture.trac.common.validation.core.*;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class Validator {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<ValidationKey, ValidationFunction<?>> validators;

    public Validator() {
        this.validators = ValidatorBuilder.buildValidatorMap();
    }

    public <TMsg extends Message>
    void validateFixedMethod(TMsg message, Descriptors.MethodDescriptor method) {

        var ctx = ValidationContext.forMethod(message, method);
        topLevelValidation(message, ctx);
    }

    public <TMsg extends Message>
    void validateFixedObject(TMsg message) {

        var ctx = ValidationContext.forMessage(message);
        topLevelValidation(message, ctx);
    }

    public <TMsg extends Message>
    void validateVersion(TMsg current, TMsg prior) {

        var ctx = ValidationContext.forVersion(current, prior);
        topLevelValidation(current, ctx);
    }

    public <TMsg extends Message>
    void validateReferential(TMsg message, Map<TagHeader, ObjectDefinition> references) {

    }

    private <TMsg extends Message>
    void topLevelValidation(TMsg msg, ValidationContext ctx) {

        var key = ctx.key();

        log.info("VALIDATION START: [{}]", key.displayName());

        var resultCtx = registeredValidation(msg, ctx);
        var result = ValidationResult.forContext(resultCtx);

        if (!result.ok()) {

            log.error("VALIDATION FAILED: [{}]", key.displayName());

            for (var failure: result.failures())
                log.error(failure.message());

            switch (ctx.validationType()) {

                case FIXED: throw new EInputValidation(result.failureMessage());
                case VERSION: throw new EVersionValidation(result.failureMessage());

                default:
                    throw new EUnexpected();
            }


        }

        log.info("VALIDATION SUCCEEDED: [{}]", key.displayName());
    }

    private <TMsg extends Message>
    ValidationContext registeredValidation(TMsg msg, ValidationContext ctx) {

        var key = ctx.key();
        var validator = validators.get(key);

        if (validator == null) {
            log.error("Required validator is not registered: [{}]", key.displayName());
            throw new EUnexpected();
        }

        if (validator.isBasic())
            return ctx.apply(validator.basic());

        if (!validator.targetClass().isInstance(msg))
            throw new EUnexpected();

        if (validator.isTyped()) {

            @SuppressWarnings("unchecked")
            var typedValidator = (ValidationFunction.Typed<TMsg>) validator.typed();

            @SuppressWarnings("unchecked")
            var typedTarget = (Class<TMsg>) msg.getClass();

            return ctx.apply(typedValidator, typedTarget);
        }

        if (validator.isVersion()) {

            @SuppressWarnings("unchecked")
            var typedValidator = (ValidationFunction.Version<TMsg>) validator.version();

            @SuppressWarnings("unchecked")
            var typedTarget = (Class<TMsg>) msg.getClass();

            return ctx.apply(typedValidator, typedTarget);
        }

        throw new EUnexpected();
    }
}
