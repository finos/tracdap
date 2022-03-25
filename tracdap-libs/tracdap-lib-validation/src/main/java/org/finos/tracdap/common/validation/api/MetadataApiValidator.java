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

package org.finos.tracdap.common.validation.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.CommonValidators;
import org.finos.tracdap.common.validation.static_.ObjectIdValidator;
import org.finos.tracdap.common.validation.static_.SearchValidator;
import org.finos.tracdap.metadata.SearchParameters;
import org.finos.tracdap.metadata.TagSelector;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC, serviceFile = Metadata.class, serviceName = TracMetadataApiGrpc.SERVICE_NAME)
public class MetadataApiValidator {

    private static final Descriptors.Descriptor METADATA_WRITE_REQUEST;
    private static final Descriptors.FieldDescriptor MWR_TENANT;

    private static final Descriptors.Descriptor METADATA_READ_REQUEST;
    private static final Descriptors.FieldDescriptor MRR_TENANT;
    private static final Descriptors.FieldDescriptor MRR_SELECTOR;

    private static final Descriptors.Descriptor METADATA_SEARCH_REQUEST;
    private static final Descriptors.FieldDescriptor MSR_TENANT;
    private static final Descriptors.FieldDescriptor MSR_SEARCH_PARAMS;

    private static final Descriptors.Descriptor BATCH_READ_REQUEST;
    private static final Descriptors.FieldDescriptor BRR_TENANT;
    private static final Descriptors.FieldDescriptor BRR_SELECTORS;

    static {

        METADATA_WRITE_REQUEST = MetadataWriteRequest.getDescriptor();
        MWR_TENANT = field(METADATA_WRITE_REQUEST, MetadataWriteRequest.TENANT_FIELD_NUMBER);

        METADATA_READ_REQUEST = MetadataReadRequest.getDescriptor();
        MRR_TENANT = field(METADATA_READ_REQUEST, MetadataReadRequest.TENANT_FIELD_NUMBER);
        MRR_SELECTOR = field(METADATA_READ_REQUEST, MetadataReadRequest.SELECTOR_FIELD_NUMBER);

        METADATA_SEARCH_REQUEST = MetadataSearchRequest.getDescriptor();
        MSR_TENANT = field(METADATA_SEARCH_REQUEST, MetadataSearchRequest.TENANT_FIELD_NUMBER);
        MSR_SEARCH_PARAMS = field(METADATA_SEARCH_REQUEST, MetadataSearchRequest.SEARCHPARAMS_FIELD_NUMBER);

        BATCH_READ_REQUEST = MetadataBatchRequest.getDescriptor();
        BRR_TENANT = field(BATCH_READ_REQUEST, MetadataBatchRequest.TENANT_FIELD_NUMBER);
        BRR_SELECTORS = field(BATCH_READ_REQUEST, MetadataBatchRequest.SELECTOR_FIELD_NUMBER);
    }

    @Validator(method = "readObject")
    public static ValidationContext readObject(MetadataReadRequest msg, ValidationContext ctx) {

        ctx = ctx.push(MRR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(MRR_SELECTOR)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .pop();

        return ctx;
    }

    @Validator(method = "search")
    public static ValidationContext search(MetadataSearchRequest msg, ValidationContext ctx) {

        ctx = ctx.push(MSR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(MSR_SEARCH_PARAMS)
                .apply(CommonValidators::required)
                .apply(SearchValidator::searchParameters, SearchParameters.class)
                .pop();

        return ctx;
    }

    @Validator(method = "readBatch")
    public static ValidationContext readBatch(MetadataBatchRequest msg, ValidationContext ctx) {

        ctx = ctx.push(BRR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.pushRepeated(BRR_SELECTORS)
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated(ObjectIdValidator::tagSelector, TagSelector.class)
                .pop();

        return ctx;
    }
}
