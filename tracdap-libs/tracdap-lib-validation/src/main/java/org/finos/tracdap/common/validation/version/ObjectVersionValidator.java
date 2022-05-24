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

package org.finos.tracdap.common.validation.version;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.ObjectDefinition;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.VERSION)
public class ObjectVersionValidator {

    private static final Descriptors.Descriptor OBJECT_DEFINITION;
    private static final Descriptors.FieldDescriptor OD_OBJECT_TYPE;
    private static final Descriptors.OneofDescriptor OD_DEFINITION;

    static {

        OBJECT_DEFINITION = ObjectDefinition.getDescriptor();
        OD_OBJECT_TYPE = field(OBJECT_DEFINITION, ObjectDefinition.OBJECTTYPE_FIELD_NUMBER);
        OD_DEFINITION = field(OBJECT_DEFINITION, ObjectDefinition.DATA_FIELD_NUMBER).getContainingOneof();
    }

    @Validator
    public static ValidationContext objectVersion(ObjectDefinition current, ObjectDefinition prior, ValidationContext ctx) {

        ctx = ctx.push(OD_OBJECT_TYPE)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.pushOneOf(OD_DEFINITION)
                .apply(CommonValidators::sameOneOf)
                .applyRegistered()
                .pop();

        return ctx;
    }
}
