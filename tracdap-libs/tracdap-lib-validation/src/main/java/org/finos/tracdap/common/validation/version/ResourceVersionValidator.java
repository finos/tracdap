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
import org.finos.tracdap.metadata.ResourceDefinition;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.VERSION)
public class ResourceVersionValidator {

    private static final Descriptors.Descriptor RESOURCE_DEFINITION;
    private static final Descriptors.FieldDescriptor RD_RESOURCE_TYPE;

    static {
        RESOURCE_DEFINITION = ResourceDefinition.getDescriptor();
        RD_RESOURCE_TYPE = field(RESOURCE_DEFINITION, ResourceDefinition.RESOURCETYPE_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext resourceVersion(ResourceDefinition current, ResourceDefinition prior, ValidationContext ctx) {

        // Resource versions must have the same resource type
        // Everything else can change, i.e. migrating to a different protocol is allowed

        ctx = ctx.push(RD_RESOURCE_TYPE)
                .apply(CommonValidators::exactMatch)
                .pop();

        return ctx;
    }
}
