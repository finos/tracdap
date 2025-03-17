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

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.CommonValidators;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC, serviceFile = AdminServiceProto.class, serviceName = TracAdminApiGrpc.SERVICE_NAME)
public class AdminApiValidator {

    private static final Descriptors.Descriptor CONFIG_WRITE_REQUEST;
    private static final Descriptors.FieldDescriptor CWR_TENANT;
    private static final Descriptors.FieldDescriptor CWR_CONFIG_CLASS;
    private static final Descriptors.FieldDescriptor CWR_CONFIG_KEY;
    private static final Descriptors.FieldDescriptor CWR_PRIOR_VERSION;
    private static final Descriptors.FieldDescriptor CWR_DEFINITION;

    private static final Descriptors.Descriptor CONFIG_READ_REQUEST;
    private static final Descriptors.FieldDescriptor CRR_TENANT;
    private static final Descriptors.FieldDescriptor CRR_ENTRY;

    static {

        CONFIG_WRITE_REQUEST = ConfigWriteRequest.getDescriptor();
        CWR_TENANT = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.TENANT_FIELD_NUMBER);
        CWR_CONFIG_CLASS = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.CONFIGCLASS_FIELD_NUMBER);
        CWR_CONFIG_KEY = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.CONFIGKEY_FIELD_NUMBER);
        CWR_PRIOR_VERSION = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.PRIORVERSION_FIELD_NUMBER);
        CWR_DEFINITION = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.DEFINITION_FIELD_NUMBER);

        CONFIG_READ_REQUEST = ConfigReadRequest.getDescriptor();
        CRR_TENANT = field(CONFIG_READ_REQUEST, ConfigReadRequest.TENANT_FIELD_NUMBER);
        CRR_ENTRY = field(CONFIG_READ_REQUEST, ConfigReadRequest.ENTRY_FIELD_NUMBER);
    }

    @Validator(method = "createConfigObject")
    public static ValidationContext createConfigObject(ConfigWriteRequest msg, ValidationContext ctx) {

        ctx = commonWriteRequest(ctx);

        // No prior version for create calls
        ctx = ctx.push(CWR_PRIOR_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        ctx = ctx.push(CWR_DEFINITION)
                .apply(CommonValidators::required)
                // TODO: restrict object types
                .applyRegistered()
                .pop();

        return ctx;
    }

    @Validator(method = "updateConfigObject")
    public static ValidationContext updateConfigObject(ConfigWriteRequest msg, ValidationContext ctx) {

        ctx = commonWriteRequest(ctx);

        // No prior version for create calls
        ctx = ctx.push(CWR_PRIOR_VERSION)
                .apply(CommonValidators::required)
                // TODO: Match config key / class
                .pop();

        ctx = ctx.push(CWR_DEFINITION)
                .apply(CommonValidators::required)
                // TODO: restrict object types
                // TODO: Object types matches prior in version validator
                .applyRegistered()
                .pop();

        return ctx;
    }

    @Validator(method = "deleteConfigObject")
    public static ValidationContext deleteConfigObject(ConfigWriteRequest msg, ValidationContext ctx) {

        ctx = commonWriteRequest(ctx);

        // No prior version for create calls
        ctx = ctx.push(CWR_PRIOR_VERSION)
                .apply(CommonValidators::required)
                // TODO: Match config key / class
                .pop();

        // Details must be blank for a delete request
        ctx = ctx.push(CWR_DEFINITION)
                .apply(CommonValidators::omitted)
                .pop();

        return ctx;
    }

    private static ValidationContext commonWriteRequest(ValidationContext ctx) {

        ctx = ctx.push(CWR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(CWR_CONFIG_CLASS)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(CWR_CONFIG_KEY)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        return ctx;
    }

    @Validator(method = "readConfigObject")
    public static ValidationContext readConfigObject(ConfigReadRequest msg, ValidationContext ctx) {

        // No prior version for create calls
        ctx = ctx.push(CRR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(CRR_ENTRY)
                .apply(CommonValidators::required)
                .applyRegistered()
                .pop();

        return ctx;
    }
}
