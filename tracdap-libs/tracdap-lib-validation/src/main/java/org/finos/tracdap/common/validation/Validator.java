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

package org.finos.tracdap.common.validation;

import org.finos.tracdap.common.exception.EConsistencyValidation;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.exception.EVersionValidation;
import org.finos.tracdap.common.validation.core.*;
import org.finos.tracdap.common.validation.core.impl.ValidationResult;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.config.PlatformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Validator {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public Validator() {
        //this.validators = ValidatorBuilder.buildValidatorMap();
    }

    public <TMsg extends Message>
    void validateFixedMethod(TMsg message, Descriptors.MethodDescriptor method) {

        var ctx = ValidationContext.forMethod(message, method);
        doValidation(ctx);
    }

    public <TMsg extends Message>
    void validateFixedObject(TMsg message) {

        var ctx = ValidationContext.forMessage(message);
        doValidation(ctx);
    }

    public <TMsg extends Message>
    void validateVersion(TMsg current, TMsg prior) {

        var ctx = ValidationContext.forVersion(current, prior);
        doValidation(ctx);
    }

    public <TMsg extends Message>
    void validateConsistency(TMsg message, MetadataBundle metadata, PlatformConfig resources) {

        var ctx = ValidationContext.forConsistency(message, metadata, resources);
        doValidation(ctx);
    }

    private void doValidation(ValidationContext ctx) {

        var key = ctx.key();

        log.info("VALIDATION START: [{}]", key.displayName());

        var resultCtx = ctx.applyRegistered();
        var result = ValidationResult.forContext(resultCtx);

        if (!result.ok()) {

            for (var failure: result.failures())
                log.error(failure.locationAndMessage());

            log.error("VALIDATION FAILED: [{}]", key.displayName());

            var details = result.errorDetails();

            switch (ctx.validationType()) {

                case STATIC: throw new EInputValidation(details.getMessage(), details);
                case VERSION: throw new EVersionValidation(details.getMessage(), details);
                case CONSISTENCY: throw new EConsistencyValidation(details.getMessage(), details);

                default:
                    throw new EUnexpected();
            }


        }

        log.info("VALIDATION SUCCEEDED: [{}]", key.displayName());
    }
}
