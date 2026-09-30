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

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.HashMap;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class IdentityValidator {

    private static final Descriptors.Descriptor IDENTITY_DEFINITION;
    private static final Descriptors.FieldDescriptor ID_PROPERTIES;
    private static final Descriptors.FieldDescriptor ID_SECRETS;

    static {
        IDENTITY_DEFINITION = IdentityDefinition.getDescriptor();
        ID_PROPERTIES = field(IDENTITY_DEFINITION, IdentityDefinition.PROPERTIES_FIELD_NUMBER);
        ID_SECRETS = field(IDENTITY_DEFINITION, IdentityDefinition.SECRETS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext identityDefinition(IdentityDefinition msg, ValidationContext ctx) {

        var propertyKeys = new HashMap<String, String>();

        ctx = ctx.pushMap(ID_PROPERTIES)
                .applyMapKeys(CommonValidators::required)
                .applyMapKeys(CommonValidators::propertyKey)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(propertyKeys, ID_PROPERTIES.getName()))
                .applyMapValues(CommonValidators::required)
                .pop();

        // Secret values can be blank, so a masked identity can be sent back on update
        ctx = ctx.pushMap(ID_SECRETS)
                .applyMapKeys(CommonValidators::required)
                .applyMapKeys(CommonValidators::propertyKey)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(propertyKeys, ID_SECRETS.getName()))
                .pop();

        return ctx;
    }
}
