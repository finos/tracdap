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
import org.finos.tracdap.common.validation.ValidationConstants;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.CommonValidators;
import org.finos.tracdap.common.validation.static_.ConfigValidator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC, serviceFile = AdminServiceProto.class, serviceName = TracAdminApiGrpc.SERVICE_NAME)
public class AdminApiValidator {

    private static final Descriptors.Descriptor CONFIG_WRITE_REQUEST;
    private static final Descriptors.FieldDescriptor CWR_TENANT;
    private static final Descriptors.FieldDescriptor CWR_CONFIG_CLASS;
    private static final Descriptors.FieldDescriptor CWR_CONFIG_KEY;
    private static final Descriptors.FieldDescriptor CWR_PRIOR_ENTRY;
    private static final Descriptors.FieldDescriptor CWR_DEFINITION;

    private static final Descriptors.Descriptor CONFIG_READ_REQUEST;
    private static final Descriptors.FieldDescriptor CRR_TENANT;
    private static final Descriptors.FieldDescriptor CRR_ENTRY;

    private static final Descriptors.Descriptor CONFIG_READ_BATCH_REQUEST;
    private static final Descriptors.FieldDescriptor CRB_TENANT;
    private static final Descriptors.FieldDescriptor CRB_ENTRIES;

    private static final Descriptors.Descriptor CONFIG_LIST_REQUEST;
    private static final Descriptors.FieldDescriptor CLR_TENANT;
    private static final Descriptors.FieldDescriptor CLR_CONFIG_CLASS;
    private static final Descriptors.FieldDescriptor CLR_INCLUDE_DELETED;
    private static final Descriptors.FieldDescriptor CLR_CONFIG_TYPE;
    private static final Descriptors.FieldDescriptor CLR_RESOURCE_TYPE;

    static {

        CONFIG_WRITE_REQUEST = ConfigWriteRequest.getDescriptor();
        CWR_TENANT = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.TENANT_FIELD_NUMBER);
        CWR_CONFIG_CLASS = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.CONFIGCLASS_FIELD_NUMBER);
        CWR_CONFIG_KEY = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.CONFIGKEY_FIELD_NUMBER);
        CWR_PRIOR_ENTRY = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.PRIORENTRY_FIELD_NUMBER);
        CWR_DEFINITION = field(CONFIG_WRITE_REQUEST, ConfigWriteRequest.DEFINITION_FIELD_NUMBER);

        CONFIG_READ_REQUEST = ConfigReadRequest.getDescriptor();
        CRR_TENANT = field(CONFIG_READ_REQUEST, ConfigReadRequest.TENANT_FIELD_NUMBER);
        CRR_ENTRY = field(CONFIG_READ_REQUEST, ConfigReadRequest.ENTRY_FIELD_NUMBER);

        CONFIG_READ_BATCH_REQUEST = ConfigReadBatchRequest.getDescriptor();
        CRB_TENANT = field(CONFIG_READ_BATCH_REQUEST, ConfigReadBatchRequest.TENANT_FIELD_NUMBER);
        CRB_ENTRIES = field(CONFIG_READ_BATCH_REQUEST, ConfigReadBatchRequest.ENTRIES_FIELD_NUMBER);

        CONFIG_LIST_REQUEST = ConfigListRequest.getDescriptor();
        CLR_TENANT = field(CONFIG_LIST_REQUEST, ConfigListRequest.TENANT_FIELD_NUMBER);
        CLR_CONFIG_CLASS = field(CONFIG_LIST_REQUEST, ConfigListRequest.CONFIGCLASS_FIELD_NUMBER);
        CLR_INCLUDE_DELETED = field(CONFIG_LIST_REQUEST, ConfigListRequest.INCLUDEDELETED_FIELD_NUMBER);
        CLR_CONFIG_TYPE = field(CONFIG_LIST_REQUEST, ConfigListRequest.CONFIGTYPE_FIELD_NUMBER);
        CLR_RESOURCE_TYPE = field(CONFIG_LIST_REQUEST, ConfigListRequest.RESOURCETYPE_FIELD_NUMBER);
    }

    @Validator(method = "createConfigObject")
    public static ValidationContext createConfigObject(ConfigWriteRequest msg, ValidationContext ctx) {

        ctx = commonWriteRequest(ctx);

        // No prior version for create calls
        ctx = ctx.push(CWR_PRIOR_ENTRY)
                .apply(CommonValidators::omitted)
                .pop();

        ctx = ctx.push(CWR_DEFINITION)
                .apply(CommonValidators::required)
                .apply(AdminApiValidator::configObjectType, ObjectDefinition.class)
                .applyRegistered()
                .pop();

        return ctx;
    }

    @Validator(method = "updateConfigObject")
    public static ValidationContext updateConfigObject(ConfigWriteRequest msg, ValidationContext ctx) {

        ctx = commonWriteRequest(ctx);

        ctx = ctx.push(CWR_PRIOR_ENTRY)
                .apply(CommonValidators::required)
                .applyRegistered()
                .apply(AdminApiValidator::sameClassAndKey, ConfigEntry.class, msg)
                .pop();

        var priorSelector = msg.getPriorEntry().getDetails().getObjectSelector();
        var priorSelectorAvailable =
                msg.getPriorEntry().hasDetails() &&
                msg.getPriorEntry().getDetails().hasObjectSelector();

        ctx = ctx.push(CWR_DEFINITION)
                .apply(CommonValidators::required)
                .applyRegistered()
                .apply(AdminApiValidator::configObjectType, ObjectDefinition.class)
                .applyIf(priorSelectorAvailable, AdminApiValidator::sameObjectType, ObjectDefinition.class, priorSelector)
                .pop();


        return ctx;
    }

    @Validator(method = "deleteConfigObject")
    public static ValidationContext deleteConfigObject(ConfigWriteRequest msg, ValidationContext ctx) {

        ctx = commonWriteRequest(ctx);

        ctx = ctx.push(CWR_PRIOR_ENTRY)
                .apply(CommonValidators::required)
                .applyRegistered()
                .apply(AdminApiValidator::sameClassAndKey, ConfigEntry.class, msg)
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
                .apply(CommonValidators::configKey)
                .pop();

        ctx = ctx.push(CWR_CONFIG_KEY)
                .apply(CommonValidators::required)
                .apply(CommonValidators::configKey)
                .pop();

        return ctx;
    }

    @Validator(method = "readConfigObject")
    public static ValidationContext readConfigObject(ConfigReadRequest msg, ValidationContext ctx) {

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

    @Validator(method = "readConfigBatch")
    public static ValidationContext readConfigBatch(ConfigReadBatchRequest msg, ValidationContext ctx) {

        ctx = ctx.push(CRB_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.pushRepeated(CRB_ENTRIES)
                .apply(CommonValidators::required)
                .applyRepeated(ConfigValidator::configEntry, ConfigEntry.class)
                .pop();

        return ctx;
    }

    @Validator(method = "listConfigEntries")
    public static ValidationContext listConfigEntries(ConfigListRequest msg, ValidationContext ctx) {

        ctx = ctx.push(CLR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(CLR_CONFIG_CLASS)
                .apply(CommonValidators::required)
                .apply(CommonValidators::configKey)
                .pop();

        ctx = ctx.push(CLR_INCLUDE_DELETED)
                .apply(CommonValidators::optional)
                .pop();

        ctx = ctx.push(CLR_CONFIG_TYPE)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::nonZeroEnum, ConfigType.class)
                .pop();

        ctx = ctx.push(CLR_RESOURCE_TYPE)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::nonZeroEnum, ResourceType.class)
                .pop();

        if (msg.hasConfigType() && msg.hasResourceType()) {
            ctx = ctx.error("Both configType and resourceType are set (these are mutually exclusive options)");
        }

        return ctx;
    }

    private static ValidationContext configObjectType(ObjectDefinition definition, ValidationContext ctx) {

        if (!ValidationConstants.CONFIG_OBJECT_TYPES.contains(definition.getObjectType())) {
            var err = String.format("Object type [%s] is not a config object", definition.getObjectType().name());
            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext sameClassAndKey(ConfigEntry priorEntry, ConfigWriteRequest writeRequest, ValidationContext ctx) {

        if (!priorEntry.getConfigClass().equals(writeRequest.getConfigClass())) {

            var error = String.format(
                    "Prior config class [%s] does not match supplied config class [%s]",
                    priorEntry.getConfigClass(), writeRequest.getConfigClass());

            ctx = ctx.error(error);
        }

        if (!priorEntry.getConfigKey().equals(writeRequest.getConfigKey())) {

            var error = String.format(
                    "Prior config key [%s] does not match supplied config key [%s]",
                    priorEntry.getConfigKey(), writeRequest.getConfigKey());

            ctx = ctx.error(error);
        }

        return ctx;
    }

    private static ValidationContext sameObjectType(ObjectDefinition definition, TagSelector priorSelector, ValidationContext ctx) {

        if (definition.getObjectType() != priorSelector.getObjectType()) {

            var error = String.format(
                    "Prior object type [%s] does not match supplied object type [%s]",
                    priorSelector.getObjectType(), definition.getObjectType());

            return ctx.error(error);
        }

        return ctx;
    }
}
