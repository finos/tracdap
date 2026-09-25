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
public class CredentialValidator {

    private static final Descriptors.Descriptor CREDENTIAL_DEFINITION;
    private static final Descriptors.FieldDescriptor CD_PROPERTIES;
    private static final Descriptors.FieldDescriptor CD_SECRETS;

    static {
        CREDENTIAL_DEFINITION = CredentialDefinition.getDescriptor();
        CD_PROPERTIES = field(CREDENTIAL_DEFINITION, CredentialDefinition.PROPERTIES_FIELD_NUMBER);
        CD_SECRETS = field(CREDENTIAL_DEFINITION, CredentialDefinition.SECRETS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext credentialDefinition(CredentialDefinition msg, ValidationContext ctx) {

        var propertyKeys = new HashMap<String, String>();

        ctx = ctx.pushMap(CD_PROPERTIES)
                .applyMapKeys(CommonValidators::required)
                .applyMapKeys(CommonValidators::propertyKey)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(propertyKeys, CD_PROPERTIES.getName()))
                .applyMapValues(CommonValidators::required)
                .pop();

        ctx = ctx.pushMap(CD_SECRETS)
                .applyMapKeys(CommonValidators::required)
                .applyMapKeys(CommonValidators::propertyKey)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(propertyKeys, CD_SECRETS.getName()))
                .applyMapValues(CommonValidators::required)
                .pop();

        return ctx;
    }
}
