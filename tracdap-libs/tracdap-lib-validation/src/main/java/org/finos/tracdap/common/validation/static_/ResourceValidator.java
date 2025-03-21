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
public class ResourceValidator {

    private static final Descriptors.Descriptor RESOURCE_DEFINITION;
    private static final Descriptors.FieldDescriptor RD_RESOURCE_TYPE;
    private static final Descriptors.FieldDescriptor RD_PROTOCOL;
    private static final Descriptors.FieldDescriptor RD_SUB_PROTOCOL;
    private static final Descriptors.FieldDescriptor RD_PUBLIC_PROPERTIES;
    private static final Descriptors.FieldDescriptor RD_PROPERTIES;
    private static final Descriptors.FieldDescriptor RD_SECRETS;

    static {
        RESOURCE_DEFINITION = ResourceDefinition.getDescriptor();
        RD_RESOURCE_TYPE = field(RESOURCE_DEFINITION, ResourceDefinition.RESOURCETYPE_FIELD_NUMBER);
        RD_PROTOCOL = field(RESOURCE_DEFINITION, ResourceDefinition.PROTOCOL_FIELD_NUMBER);
        RD_SUB_PROTOCOL = field(RESOURCE_DEFINITION, ResourceDefinition.SUBPROTOCOL_FIELD_NUMBER);
        RD_PUBLIC_PROPERTIES = field(RESOURCE_DEFINITION, ResourceDefinition.PUBLICPROPERTIES_FIELD_NUMBER);
        RD_PROPERTIES = field(RESOURCE_DEFINITION, ResourceDefinition.PROPERTIES_FIELD_NUMBER);
        RD_SECRETS = field(RESOURCE_DEFINITION, ResourceDefinition.SECRETS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext resourceDefinition(ResourceDefinition msg, ValidationContext ctx) {

        ctx = ctx.push(RD_RESOURCE_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, ResourceType.class)
                .pop();

        ctx = ctx.push(RD_PROTOCOL)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(RD_SUB_PROTOCOL)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::identifier)
                .pop();

        var propertyKeys = new HashMap<String, String>();

        ctx = ctx.pushMap(RD_PUBLIC_PROPERTIES)
                .applyMapKeys(CommonValidators::required)
                .applyMapKeys(CommonValidators::propertyKey)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(propertyKeys, RD_PUBLIC_PROPERTIES.getName()))
                .applyMapValues(CommonValidators::required)
                .pop();

        ctx = ctx.pushMap(RD_PROPERTIES)
                .applyMapKeys(CommonValidators::required)
                .applyMapKeys(CommonValidators::propertyKey)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(propertyKeys, RD_PROPERTIES.getName()))
                .applyMapValues(CommonValidators::required)
                .pop();

        ctx = ctx.pushMap(RD_SECRETS)
                .applyMapKeys(CommonValidators::required)
                .applyMapKeys(CommonValidators::propertyKey)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(propertyKeys, RD_SECRETS.getName()))
                .applyMapValues(CommonValidators::required)
                .pop();

        return ctx;
    }
}
