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

package org.finos.tracdap.common.validation.version;

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.CustomDefinition;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.VERSION)
public class CustomVersionValidator {

    private static final Descriptors.Descriptor CUSTOM_DEF;
    private static final Descriptors.FieldDescriptor CD_CUSTOM_SCHEMA_TYPE;
    private static final Descriptors.FieldDescriptor CD_CUSTOM_SCHEMA_VERSION;

    static {
        CUSTOM_DEF = CustomDefinition.getDescriptor();
        CD_CUSTOM_SCHEMA_TYPE = field(CUSTOM_DEF, CustomDefinition.CUSTOMSCHEMATYPE_FIELD_NUMBER);
        CD_CUSTOM_SCHEMA_VERSION = field(CUSTOM_DEF, CustomDefinition.CUSTOMSCHEMAVERSION_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext customVersion(CustomDefinition current, CustomDefinition prior, ValidationContext ctx) {

        ctx = ctx.push(CD_CUSTOM_SCHEMA_TYPE)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(CD_CUSTOM_SCHEMA_VERSION)
                .apply(CommonValidators::equalOrGreater, Integer.class)
                .pop();

        return ctx;
    }
}
