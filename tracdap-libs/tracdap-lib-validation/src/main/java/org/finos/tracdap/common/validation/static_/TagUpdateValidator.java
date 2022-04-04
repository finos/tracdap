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
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.core.ValidatorUtils;
import org.finos.tracdap.metadata.*;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class TagUpdateValidator {

    private static final Descriptors.Descriptor TAG_UPDATE;
    private static final Descriptors.FieldDescriptor TU_OPERATION;
    private static final Descriptors.FieldDescriptor TU_ATTR_NAME;
    private static final Descriptors.FieldDescriptor TU_VALUE;

    private static final Descriptors.Descriptor VALUE;
    private static final Descriptors.OneofDescriptor V_VALUE;

    static {

        TAG_UPDATE = TagUpdate.getDescriptor();
        TU_ATTR_NAME = ValidatorUtils.field(TAG_UPDATE, TagUpdate.ATTRNAME_FIELD_NUMBER);
        TU_OPERATION = ValidatorUtils.field(TAG_UPDATE, TagUpdate.OPERATION_FIELD_NUMBER);
        TU_VALUE = ValidatorUtils.field(TAG_UPDATE, TagUpdate.VALUE_FIELD_NUMBER);

        VALUE = Value.getDescriptor();
        V_VALUE = field(VALUE, Value.BOOLEANVALUE_FIELD_NUMBER).getContainingOneof();
    }

    @Validator
    public static ValidationContext tagUpdate(TagUpdate msg, ValidationContext ctx) {

        ctx = ctx.push(TU_OPERATION)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::recognizedEnum, TagOperation.class)
                .pop();

        var nameRequired = msg.getOperation() != TagOperation.CLEAR_ALL_ATTR;
        var valueRequired = nameRequired && msg.getOperation() != TagOperation.DELETE_ATTR;

        var nameRequiredQualifier = String.format("%s == %s",
                TU_OPERATION.getName(), TagOperation.CLEAR_ALL_ATTR.name());
        var valueRequiredQualifier = String.format("%s == %s or %s == %s",
                TU_OPERATION.getName(), TagOperation.DELETE_ATTR.name(),
                TU_OPERATION.getName(), TagOperation.CLEAR_ALL_ATTR.name());

        ctx = ctx.push(TU_ATTR_NAME)
                .apply(CommonValidators.ifAndOnlyIf(nameRequired, nameRequiredQualifier, true))
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(TU_VALUE)
                .apply(CommonValidators.ifAndOnlyIf(valueRequired, valueRequiredQualifier, true))
                .apply(TypeSystemValidator::value, Value.class)
                .apply(TagUpdateValidator::notNull, Value.class)
                .apply(TagUpdateValidator::allowedValueTypes, Value.class)
                .pop();

        return ctx;
    }

    public static ValidationContext reservedAttrs(TagUpdate msg, boolean allowReserved, ValidationContext ctx) {

        if (allowReserved)
            return ctx;

        var isReserved = MetadataConstants.TRAC_RESERVED_IDENTIFIER.matcher(msg.getAttrName());

        if (isReserved.matches()) {

            var err = String.format(
                    "Attribute name [%s] is a reserved identifier",
                    msg.getAttrName());

            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext notNull(Value msg, ValidationContext ctx) {

        if (!msg.hasOneof(V_VALUE))
            return ctx.error("Null values are not allowed for tag updates");

        return ctx;
    }

    private static ValidationContext allowedValueTypes(Value msg, ValidationContext ctx) {

        if (TypeSystem.isPrimitive(msg))
            return ctx;

        if (TypeSystem.basicType(msg) == BasicType.ARRAY) {

            if (TypeSystem.isPrimitive(msg.getType().getArrayType()))
                return ctx;

            return ctx.error("Nested array types are now allowed for tag updates");
        }

        var err = String.format("Value type [%s] is not allowed for tag updates", TypeSystem.basicType(msg));
        return ctx.error(err);
    }
}
