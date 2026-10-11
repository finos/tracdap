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

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class PlacementValidator {

    private static final Descriptors.Descriptor PLACEMENT_TARGET;
    private static final Descriptors.OneofDescriptor PT_SOURCE;
    private static final Descriptors.FieldDescriptor PT_DATA_ID;
    private static final Descriptors.FieldDescriptor PT_LOCATION;

    private static final Descriptors.Descriptor PLACEMENT_RECORD;
    private static final Descriptors.FieldDescriptor PR_DATA_ID;
    private static final Descriptors.FieldDescriptor PR_LOCATION;
    private static final Descriptors.FieldDescriptor PR_FORMAT;
    private static final Descriptors.FieldDescriptor PR_MIME_TYPE;
    private static final Descriptors.FieldDescriptor PR_SIZE;
    private static final Descriptors.FieldDescriptor PR_CONTENT_HASH;

    static {

        PLACEMENT_TARGET = PlacementTarget.getDescriptor();
        PT_DATA_ID = field(PLACEMENT_TARGET, PlacementTarget.DATAID_FIELD_NUMBER);
        PT_SOURCE = PT_DATA_ID.getContainingOneof();
        PT_LOCATION = field(PLACEMENT_TARGET, PlacementTarget.LOCATION_FIELD_NUMBER);

        PLACEMENT_RECORD = PlacementRecord.getDescriptor();
        PR_DATA_ID = field(PLACEMENT_RECORD, PlacementRecord.DATAID_FIELD_NUMBER);
        PR_LOCATION = field(PLACEMENT_RECORD, PlacementRecord.LOCATION_FIELD_NUMBER);
        PR_FORMAT = field(PLACEMENT_RECORD, PlacementRecord.FORMAT_FIELD_NUMBER);
        PR_MIME_TYPE = field(PLACEMENT_RECORD, PlacementRecord.MIMETYPE_FIELD_NUMBER);
        PR_SIZE = field(PLACEMENT_RECORD, PlacementRecord.SIZE_FIELD_NUMBER);
        PR_CONTENT_HASH = field(PLACEMENT_RECORD, PlacementRecord.CONTENTHASH_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext placementTarget(PlacementTarget msg, ValidationContext ctx) {

        ctx = ctx.pushOneOf(PT_SOURCE)
                .apply(CommonValidators::required)
                .applyOneOf(PT_DATA_ID, ObjectIdValidator::tagSelector, TagSelector.class)
                .applyOneOf(PT_DATA_ID, ObjectIdValidator::selectorType, TagSelector.class, ObjectType.DATA)
                .pop();

        ctx = ctx.push(PT_LOCATION)
                .apply(CommonValidators::required)
                .apply(ExternalLocationValidator::externalLocation, ExternalLocation.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext placementRecord(PlacementRecord msg, ValidationContext ctx) {

        ctx = ctx.push(PR_DATA_ID)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.DATA)
                .apply(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.push(PR_LOCATION)
                .apply(CommonValidators::required)
                .apply(ExternalLocationValidator::externalLocation, ExternalLocation.class)
                .pop();

        ctx = ctx.push(PR_FORMAT)
                .apply(CommonValidators::required)
                .pop();

        ctx = ctx.push(PR_MIME_TYPE)
                .apply(CommonValidators::required)
                .pop();

        ctx = ctx.push(PR_SIZE)
                .apply(CommonValidators::notNegative, Long.class)
                .pop();

        ctx = ctx.push(PR_CONTENT_HASH)
                .apply(CommonValidators::required)
                .pop();

        return ctx;
    }
}
