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

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.DataDefinition;
import org.finos.tracdap.metadata.SchemaDefinition;
import org.finos.tracdap.metadata.TagSelector;
import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.VERSION)
public class DataVersionValidator {

    private static final Descriptors.Descriptor DATA_DEFINITION;
    private static final Descriptors.OneofDescriptor DD_SCHEMA_DEFINITION;
    private static final Descriptors.FieldDescriptor DD_SCHEMA_ID;
    private static final Descriptors.FieldDescriptor DD_SCHEMA;
    private static final Descriptors.FieldDescriptor DD_STORAGE_ID;

    static {

        DATA_DEFINITION = DataDefinition.getDescriptor();
        DD_SCHEMA_DEFINITION = field(DATA_DEFINITION, DataDefinition.SCHEMAID_FIELD_NUMBER).getContainingOneof();
        DD_SCHEMA_ID = field(DATA_DEFINITION, DataDefinition.SCHEMAID_FIELD_NUMBER);
        DD_SCHEMA = field(DATA_DEFINITION, DataDefinition.SCHEMA_FIELD_NUMBER);
        DD_STORAGE_ID = field(DATA_DEFINITION, DataDefinition.STORAGEID_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext data(DataDefinition current, DataDefinition prior, ValidationContext ctx) {

        ctx = ctx.pushOneOf(DD_SCHEMA_DEFINITION)
                .apply(CommonValidators::sameOneOf)
                .applyOneOf(DD_SCHEMA_ID, CommonValidators::equalOrLaterVersion, TagSelector.class)
                .applyOneOf(DD_SCHEMA, SchemaVersionValidator::schema, SchemaDefinition.class)
                .pop();

        ctx = ctx.push(DD_STORAGE_ID)
                .apply(CommonValidators::exactMatch, TagSelector.class)
                .pop();

        return ctx;
    }
}
