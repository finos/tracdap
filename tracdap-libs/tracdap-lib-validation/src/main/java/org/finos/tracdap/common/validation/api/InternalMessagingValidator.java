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

package org.finos.tracdap.common.validation.api;

import org.finos.tracdap.api.internal.ConfigUpdate;
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.InternalMessagingApiGrpc;
import org.finos.tracdap.api.internal.InternalMessagingProto;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.CommonValidators;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC, serviceFile = InternalMessagingProto.class, serviceName = InternalMessagingApiGrpc.SERVICE_NAME)
public class InternalMessagingValidator {

    private static final Descriptors.Descriptor CONFIG_UPDATE;
    private static final Descriptors.FieldDescriptor CU_TENANT;
    private static final Descriptors.FieldDescriptor CU_UPDATE_TYPE;
    private static final Descriptors.FieldDescriptor CU_CONFIG_ENTRY;

    static {

        CONFIG_UPDATE = ConfigUpdate.getDescriptor();
        CU_TENANT = field(CONFIG_UPDATE, ConfigUpdate.TENANT_FIELD_NUMBER);
        CU_UPDATE_TYPE = field(CONFIG_UPDATE, ConfigUpdate.UPDATETYPE_FIELD_NUMBER);
        CU_CONFIG_ENTRY = field(CONFIG_UPDATE, ConfigUpdate.CONFIGENTRY_FIELD_NUMBER);

    }

    @Validator(method = "configUpdate")
    public static ValidationContext configUpdate(ConfigUpdate msg, ValidationContext ctx) {

        ctx = ctx.push(CU_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(CU_UPDATE_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, ConfigUpdateType.class)
                .pop();

        ctx = ctx.push(CU_CONFIG_ENTRY)
                .apply(CommonValidators::required)
                .applyRegistered()
                .pop();

        return ctx;
    }
}
