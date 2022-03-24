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

import com.google.protobuf.Descriptors;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.core.ValidatorUtils;
import org.finos.tracdap.metadata.*;


@Validator(type = ValidationType.STATIC)
public class TagUpdateValidator {

    // Rough implementation that provides only a few validation points
    // Full implementation will be comprehensive across the metadata model
    // Requires generic handling of all message, enum and primitive types, as well as the TRAC type system

    private static final Descriptors.Descriptor TAG_UPDATE;
    private static final Descriptors.FieldDescriptor TU_OPERATION;
    private static final Descriptors.FieldDescriptor TU_ATTR_NAME;
    private static final Descriptors.FieldDescriptor TU_VALUE;
    static {

        TAG_UPDATE = TagUpdate.getDescriptor();
        TU_ATTR_NAME = ValidatorUtils.field(TAG_UPDATE, TagUpdate.ATTRNAME_FIELD_NUMBER);
        TU_OPERATION = ValidatorUtils.field(TAG_UPDATE, TagUpdate.OPERATION_FIELD_NUMBER);
        TU_VALUE = ValidatorUtils.field(TAG_UPDATE, TagUpdate.VALUE_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext tagUpdate(TagUpdate msg, ValidationContext ctx) {

        ctx = ctx.push(TU_OPERATION)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::recognizedEnum, TagOperation.class)
                .pop();

        // attrName is not needed fot the CLEAR_ALL_ATTR operation
        if (msg.getOperation() == TagOperation.CLEAR_ALL_ATTR) {

            ctx = ctx.push(TU_ATTR_NAME)
                    .apply(CommonValidators::omitted)
                    .pop();
        }
        else {

            ctx = ctx.push(TU_ATTR_NAME)
                    .apply(CommonValidators::required)
                    .apply(CommonValidators::identifier)
                    .apply(CommonValidators::notTracReserved)
                    .pop();
        }

        // value is not needed for CLEAR_ALL_ATTR or DELETE_ATTR
        if (msg.getOperation() == TagOperation.CLEAR_ALL_ATTR || msg.getOperation() == TagOperation.DELETE_ATTR) {

            ctx = ctx.push(TU_VALUE)
                    .apply(CommonValidators::omitted)
                    .pop();
        }
        else {

            ctx = ctx.push(TU_VALUE)
                    .apply(CommonValidators::required)
                    .apply(TypeSystemValidator::value, Value.class)
                    .pop();
        }

        return ctx;
    }

}
