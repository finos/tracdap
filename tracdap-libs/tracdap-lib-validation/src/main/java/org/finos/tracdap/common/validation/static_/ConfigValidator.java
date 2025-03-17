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
import org.finos.tracdap.metadata.ConfigDetails;
import org.finos.tracdap.metadata.ConfigEntry;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.metadata.ObjectType;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class ConfigValidator {

    private static final Descriptors.Descriptor CONFIG_ENTRY;
    private static final Descriptors.FieldDescriptor CE_CONFIG_CLASS;
    private static final Descriptors.FieldDescriptor CE_CONFIG_KEY;
    private static final Descriptors.FieldDescriptor CE_CONFIG_VERSION;
    private static final Descriptors.FieldDescriptor CE_CONFIG_TIMESTAMP;
    private static final Descriptors.FieldDescriptor CE_IS_LATEST_CONFIG;
    private static final Descriptors.FieldDescriptor CE_CONFIG_DELETED;
    private static final Descriptors.FieldDescriptor CE_DETAILS;

    private static final Descriptors.Descriptor CONFIG_DETAILS;
    private static final Descriptors.FieldDescriptor CD_OBJECT_SELECTOR;
    private static final Descriptors.FieldDescriptor CD_OBJECT_TYPE;

    static {
        CONFIG_ENTRY = ConfigEntry.getDescriptor();
        CE_CONFIG_CLASS = field(CONFIG_ENTRY, ConfigEntry.CONFIGCLASS_FIELD_NUMBER);
        CE_CONFIG_KEY = field(CONFIG_ENTRY, ConfigEntry.CONFIGKEY_FIELD_NUMBER);
        CE_CONFIG_VERSION = field(CONFIG_ENTRY, ConfigEntry.CONFIGVERSION_FIELD_NUMBER);
        CE_CONFIG_TIMESTAMP = field(CONFIG_ENTRY, ConfigEntry.CONFIGTIMESTAMP_FIELD_NUMBER);
        CE_IS_LATEST_CONFIG = field(CONFIG_ENTRY, ConfigEntry.ISLATESTCONFIG_FIELD_NUMBER);
        CE_CONFIG_DELETED = field(CONFIG_ENTRY, ConfigEntry.CONFIGDELETED_FIELD_NUMBER);
        CE_DETAILS = field(CONFIG_ENTRY, ConfigEntry.DETAILS_FIELD_NUMBER);

        CONFIG_DETAILS = ConfigDetails.getDescriptor();
        CD_OBJECT_SELECTOR = field(CONFIG_DETAILS, ConfigDetails.OBJECTSELECTOR_FIELD_NUMBER);
        CD_OBJECT_TYPE = field(CONFIG_DETAILS, ConfigDetails.OBJECTTYPE_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext configEntry(ConfigEntry msg, ValidationContext ctx) {

        ctx = ctx.push(CE_CONFIG_CLASS)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(CE_CONFIG_KEY)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(CE_CONFIG_VERSION)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::positive, Integer.class)
                .pop();

        ctx = ctx.push(CE_CONFIG_TIMESTAMP)
                .apply(CommonValidators::optional)
                .applyRegistered()
                .pop();

        ctx = ctx.push(CE_IS_LATEST_CONFIG)
                .apply(CommonValidators::optional)
                .pop();

        ctx = ctx.push(CE_CONFIG_DELETED)
                .apply(CommonValidators::optional)
                .pop();

        ctx = ctx.push(CE_DETAILS)
                .apply(CommonValidators::optional)
                .applyRegistered()
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext configDetails(ConfigDetails msg, ValidationContext ctx) {

        ctx = ctx.push(CD_OBJECT_SELECTOR)
                .apply(CommonValidators::optional)
                .applyRegistered()
                .pop();

        // TODO: Not restricting selector type = object type, bc selector type can be CONFIG
        ctx = ctx.push(CD_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::recognizedEnum, ObjectType.class)
                .pop();

        return ctx;
    }
}
