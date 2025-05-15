/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.validation.static_;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class ResultValidator {

    private static final Descriptors.Descriptor RESULT_DEFINITION;
    private static final Descriptors.FieldDescriptor RD_JOB_ID;
    private static final Descriptors.FieldDescriptor RD_STATUS_CODE;
    private static final Descriptors.FieldDescriptor RD_STATUS_MESSAGE;
    private static final Descriptors.FieldDescriptor RD_LOG_FILE_ID;
    private static final Descriptors.FieldDescriptor RD_OUTPUTS;

    static {

        RESULT_DEFINITION = ResultDefinition.getDescriptor();
        RD_JOB_ID = field(RESULT_DEFINITION, ResultDefinition.JOBID_FIELD_NUMBER);
        RD_STATUS_CODE = field(RESULT_DEFINITION, ResultDefinition.STATUSCODE_FIELD_NUMBER);
        RD_STATUS_MESSAGE = field(RESULT_DEFINITION, ResultDefinition.STATUSMESSAGE_FIELD_NUMBER);
        RD_LOG_FILE_ID = field(RESULT_DEFINITION, ResultDefinition.LOGFILEID_FIELD_NUMBER);
        RD_OUTPUTS = field(RESULT_DEFINITION, ResultDefinition.OUTPUTS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext job(ResultDefinition msg, ValidationContext ctx) {

        ctx = ctx.push(RD_JOB_ID)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.JOB)
                .apply(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.push(RD_STATUS_CODE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, JobStatusCode.class)
                .pop();

        ctx = ctx.push(RD_STATUS_MESSAGE)
                .apply(CommonValidators::optional)
                .pop();

        ctx = ctx.push(RD_LOG_FILE_ID)
                .apply(CommonValidators::optional)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.FILE)
                .apply(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.pushMap(RD_OUTPUTS)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .pop();

        return ctx;
    }
}
