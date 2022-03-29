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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.CustomDefinition;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class CustomValidator {

    private static final Descriptors.Descriptor CUSTOM_DEF;
    private static final Descriptors.FieldDescriptor CD_CUSTOM_SCHEMA_TYPE;
    private static final Descriptors.FieldDescriptor CD_CUSTOM_SCHEMA_VERSION;
    private static final Descriptors.FieldDescriptor CD_CUSTOM_DATA;

    static {
        CUSTOM_DEF = CustomDefinition.getDescriptor();
        CD_CUSTOM_SCHEMA_TYPE = field(CUSTOM_DEF, CustomDefinition.CUSTOMSCHEMATYPE_FIELD_NUMBER);
        CD_CUSTOM_SCHEMA_VERSION = field(CUSTOM_DEF, CustomDefinition.CUSTOMSCHEMAVERSION_FIELD_NUMBER);
        CD_CUSTOM_DATA = field(CUSTOM_DEF, CustomDefinition.CUSTOMDATA_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext custom(CustomDefinition msg, ValidationContext ctx) {

        ctx = ctx.push(CD_CUSTOM_SCHEMA_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .apply(CommonValidators::notTracReserved)
                .pop();

        ctx = ctx.push(CD_CUSTOM_SCHEMA_VERSION)
                .apply(CommonValidators::required)
                .apply(CommonValidators::positive, Integer.class)
                .pop();

        ctx = ctx.push(CD_CUSTOM_DATA)
                .apply(CommonValidators::required)
                .apply(CommonValidators::bytesNotEmpty, ByteString.class)
                .pop();

        return ctx;
    }
}
