/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.validation.version;

import com.accenture.trac.common.validation.core.ValidationContext;
import com.accenture.trac.metadata.DataDefinition;
import com.accenture.trac.metadata.SchemaDefinition;
import com.accenture.trac.metadata.TagSelector;
import com.google.protobuf.Descriptors;


public class DataVersionValidator {

    private static final Descriptors.Descriptor DATA_DEFINITION;
    private static final Descriptors.FieldDescriptor DD_STORAGE_ID;
    private static final Descriptors.OneofDescriptor DD_SCHEMA_DEFINITION;
    private static final Descriptors.FieldDescriptor DD_SCHEMA_ID;

    static {

        DATA_DEFINITION = DataDefinition.getDescriptor();
        DD_STORAGE_ID = field(DATA_DEFINITION, DataDefinition.STORAGEID_FIELD_NUMBER);
        DD_SCHEMA_ID = field(DATA_DEFINITION, DataDefinition.SCHEMAID_FIELD_NUMBER);
        DD_SCHEMA_DEFINITION = DD_SCHEMA_ID.getContainingOneof();
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
    }

    public static ValidationContext data(DataDefinition current, DataDefinition prior, ValidationContext ctx) {

        ctx = ctx.pushOneOf(DD_SCHEMA_DEFINITION)
                .apply(CommonValidators::sameOneOf)
                .applyIf(CommonValidators::equalOrLaterVersion, TagSelector.class, prior.hasSchemaId())
                .applyIf(SchemaVersionValidator::schema, SchemaDefinition.class, prior.hasSchema())
                .pop();

        ctx = ctx.push(DD_STORAGE_ID)
                .apply(CommonValidators::exactMatch, TagSelector.class)
                .pop();

        return ctx;
    }
}
