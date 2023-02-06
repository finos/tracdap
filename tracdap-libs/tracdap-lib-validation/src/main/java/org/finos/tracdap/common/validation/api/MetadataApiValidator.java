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
import org.finos.tracdap.common.validation.core.ValidationFunction;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.*;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.HashSet;
import java.util.Set;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC, serviceFile = Metadata.class, serviceName = TracMetadataApiGrpc.SERVICE_NAME)
public class MetadataApiValidator {

    public static final boolean PUBLIC_API = false;
    public static final boolean TRUSTED_API = true;

    private static final Descriptors.Descriptor METADATA_WRITE_REQUEST;
    private static final Descriptors.FieldDescriptor MWR_TENANT;
    private static final Descriptors.FieldDescriptor MWR_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor MWR_PRIOR_VERSION;
    private static final Descriptors.FieldDescriptor MWR_DEFINITION;
    private static final Descriptors.FieldDescriptor MWR_ATTRS;

    private static final Descriptors.Descriptor METADATA_READ_REQUEST;
    private static final Descriptors.FieldDescriptor MRR_TENANT;
    private static final Descriptors.FieldDescriptor MRR_SELECTOR;

    private static final Descriptors.Descriptor BATCH_READ_REQUEST;
    private static final Descriptors.FieldDescriptor BRR_TENANT;
    private static final Descriptors.FieldDescriptor BRR_SELECTORS;

    private static final Descriptors.Descriptor BATCH_WRITE_REQUEST;
    private static final Descriptors.FieldDescriptor BWR_TENANT;
    private static final Descriptors.FieldDescriptor BWR_REQUESTS;

    private static final Descriptors.Descriptor METADATA_SEARCH_REQUEST;
    private static final Descriptors.FieldDescriptor MSR_TENANT;
    private static final Descriptors.FieldDescriptor MSR_SEARCH_PARAMS;

    private static final Descriptors.Descriptor METADATA_GET_REQUEST;
    private static final Descriptors.FieldDescriptor MGR_TENANT;
    private static final Descriptors.FieldDescriptor MGR_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor MGR_OBJECT_ID;
    private static final Descriptors.FieldDescriptor MGR_OBJECT_VERSION;
    private static final Descriptors.FieldDescriptor MGR_TAG_VERSION;

    private static final Descriptors.Descriptor UNIVERSAL_METADATA_WRITE_BATCH_REQUEST;
    private static final Descriptors.FieldDescriptor UMWBR_TENANT;

    static {

        METADATA_WRITE_REQUEST = MetadataWriteRequest.getDescriptor();
        MWR_TENANT = field(METADATA_WRITE_REQUEST, MetadataWriteRequest.TENANT_FIELD_NUMBER);
        MWR_OBJECT_TYPE = field(METADATA_WRITE_REQUEST, MetadataWriteRequest.OBJECTTYPE_FIELD_NUMBER);
        MWR_PRIOR_VERSION = field(METADATA_WRITE_REQUEST, MetadataWriteRequest.PRIORVERSION_FIELD_NUMBER);
        MWR_DEFINITION = field(METADATA_WRITE_REQUEST, MetadataWriteRequest.DEFINITION_FIELD_NUMBER);
        MWR_ATTRS = field(METADATA_WRITE_REQUEST, MetadataWriteRequest.TAGUPDATES_FIELD_NUMBER);

        METADATA_READ_REQUEST = MetadataReadRequest.getDescriptor();
        MRR_TENANT = field(METADATA_READ_REQUEST, MetadataReadRequest.TENANT_FIELD_NUMBER);
        MRR_SELECTOR = field(METADATA_READ_REQUEST, MetadataReadRequest.SELECTOR_FIELD_NUMBER);

        BATCH_READ_REQUEST = MetadataBatchRequest.getDescriptor();
        BRR_TENANT = field(BATCH_READ_REQUEST, MetadataBatchRequest.TENANT_FIELD_NUMBER);
        BRR_SELECTORS = field(BATCH_READ_REQUEST, MetadataBatchRequest.SELECTOR_FIELD_NUMBER);

        BATCH_WRITE_REQUEST = MetadataWriteBatchRequest.getDescriptor();
        BWR_TENANT = field(BATCH_WRITE_REQUEST, MetadataWriteBatchRequest.TENANT_FIELD_NUMBER);
        BWR_REQUESTS = field(BATCH_WRITE_REQUEST, MetadataWriteBatchRequest.REQUESTS_FIELD_NUMBER);

        METADATA_SEARCH_REQUEST = MetadataSearchRequest.getDescriptor();
        MSR_TENANT = field(METADATA_SEARCH_REQUEST, MetadataSearchRequest.TENANT_FIELD_NUMBER);
        MSR_SEARCH_PARAMS = field(METADATA_SEARCH_REQUEST, MetadataSearchRequest.SEARCHPARAMS_FIELD_NUMBER);

        METADATA_GET_REQUEST = MetadataGetRequest.getDescriptor();
        MGR_TENANT = field(METADATA_GET_REQUEST, MetadataGetRequest.TENANT_FIELD_NUMBER);
        MGR_OBJECT_TYPE = field(METADATA_GET_REQUEST, MetadataGetRequest.OBJECTTYPE_FIELD_NUMBER);
        MGR_OBJECT_ID = field(METADATA_GET_REQUEST, MetadataGetRequest.OBJECTID_FIELD_NUMBER);
        MGR_OBJECT_VERSION = field(METADATA_GET_REQUEST, MetadataGetRequest.OBJECTVERSION_FIELD_NUMBER);
        MGR_TAG_VERSION = field(METADATA_GET_REQUEST, MetadataGetRequest.TAGVERSION_FIELD_NUMBER);

        UNIVERSAL_METADATA_WRITE_BATCH_REQUEST = UniversalMetadataWriteBatchRequest.getDescriptor();
        UMWBR_TENANT = field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.TENANT_FIELD_NUMBER);
    }

    @Validator(method = "writeBatch")
    public static ValidationContext writeBatch(UniversalMetadataWriteBatchRequest msg, ValidationContext ctx) {
        return writeBatch(msg, ctx, MetadataApiValidator.PUBLIC_API);
    }

    public static ValidationContext writeBatch(UniversalMetadataWriteBatchRequest msg, ValidationContext ctx, boolean apiTrust) {
        ctx = ctx.push(UMWBR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        var knownObjectIds = new HashSet<String>();
        var objectIdCheck = uniqueObjectIdCheck(knownObjectIds);

        ctx = ctx.pushRepeated(field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.CREATEOBJECTS_FIELD_NUMBER))
                .applyRepeated((m, c) -> createObject(m, c, apiTrust, false), MetadataWriteRequest.class)
                .applyRepeated(objectIdCheck, MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.PREALLOCATEOBJECTS_FIELD_NUMBER))
                .applyRepeated((m, c) -> createPreallocatedObject(m, c, false), MetadataWriteRequest.class)
                .applyRepeated(objectIdCheck, MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.UPDATEOBJECTS_FIELD_NUMBER))
                .applyRepeated((m, c) -> updateObject(m, c, apiTrust, false), MetadataWriteRequest.class)
                .applyRepeated(objectIdCheck, MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.UPDATETAGS_FIELD_NUMBER))
                .applyRepeated((m, c) -> updateTag(m, c, apiTrust, false), MetadataWriteRequest.class)
                .applyRepeated(objectIdCheck, MetadataWriteRequest.class)
                .pop();

        return ctx;
    }

    @Validator(method = "createObject")
    public static ValidationContext createObject(MetadataWriteRequest msg, ValidationContext ctx) {

        return createObject(msg, ctx, PUBLIC_API, true);
    }

    static ValidationContext createObject(MetadataWriteRequest msg, ValidationContext ctx, boolean apiTrust, boolean tenantRequired) {

        ctx = createOrUpdate(ctx, false, apiTrust, tenantRequired);

        ctx = ctx.push(MWR_PRIOR_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        ctx = ctx.push(MWR_DEFINITION)
                .apply(CommonValidators::required)
                .apply(ObjectValidator::objectType, ObjectDefinition.class, msg.getObjectType())
                .applyRegistered()
                .pop();

        return ctx;
    }

    @Validator(method = "updateObject")
    public static ValidationContext updateObject(MetadataWriteRequest msg, ValidationContext ctx) {

        return updateObject(msg, ctx, PUBLIC_API, true);
    }

    static ValidationContext updateObject(MetadataWriteRequest msg, ValidationContext ctx, boolean apiTrust, boolean tenantRequired) {

        ctx = createOrUpdate(ctx, true, apiTrust, tenantRequired);

        ctx = ctx.push(MWR_PRIOR_VERSION)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, msg.getObjectType())
                .apply(ObjectIdValidator::explicitObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.push(MWR_DEFINITION)
                .apply(CommonValidators::required)
                .apply(ObjectValidator::objectType, ObjectDefinition.class, msg.getObjectType())
                .applyRegistered()
                .pop();

        return ctx;
    }

    @Validator(method = "updateTag")
    public static ValidationContext updateTag(MetadataWriteRequest msg, ValidationContext ctx) {

        return updateTag(msg, ctx, PUBLIC_API, true);
    }

    static ValidationContext updateTag(MetadataWriteRequest msg, ValidationContext ctx, boolean apiTrust, boolean tenantRequired) {

        ctx = createOrUpdate(ctx, false, apiTrust, tenantRequired);

        ctx = ctx.push(MWR_PRIOR_VERSION)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, msg.getObjectType())
                .apply(ObjectIdValidator::explicitObjectVersion, TagSelector.class)
                .apply(ObjectIdValidator::explicitTagVersion, TagSelector.class)
                .pop();

        ctx = ctx.push(MWR_DEFINITION)
                .apply(CommonValidators::omitted)
                .pop();

        return ctx;
    }

    static ValidationContext preallocateId(MetadataWriteRequest msg, ValidationContext ctx) {

        return preallocateId(msg, ctx, true);
    }

    private static ValidationContext preallocateId(MetadataWriteRequest msg, ValidationContext ctx, boolean tenantRequired) {
        ctx = createOrUpdate(ctx, false, TRUSTED_API, tenantRequired);

        ctx = ctx.push(MWR_PRIOR_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        ctx = ctx.push(MWR_DEFINITION)
                .apply(CommonValidators::omitted)
                .pop();
        
        return ctx;
    }

    static ValidationContext preallocateIdBatch(MetadataWriteBatchRequest msg, ValidationContext ctx) {

        ctx = ctx.push(BWR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.pushRepeated(BWR_REQUESTS)
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated((MetadataWriteRequest r, ValidationContext c) ->
                        MetadataApiValidator.preallocateId(r, c, false), MetadataWriteRequest.class)
                .pop();

        return ctx;
    }

    static ValidationContext createPreallocatedObject(MetadataWriteRequest msg, ValidationContext ctx) {

        return createPreallocatedObject(msg, ctx, true);
    }

    private static ValidationContext createPreallocatedObject(MetadataWriteRequest msg, ValidationContext ctx, boolean tenantRequired) {
        ctx = createOrUpdate(ctx, false, TRUSTED_API, tenantRequired);

        // Do not use the regular tag selector validator (ObjectIdValidator::tagSelector)
        // The regular validator will enforce object and tag version > 0
        // Which is a requirement for a regular, valid selector
        // But preallocated IDs have object version = tag version = 0

        ctx = ctx.push(MWR_PRIOR_VERSION)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::preallocated, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, msg.getObjectType())
                .pop();

        ctx = ctx.push(MWR_DEFINITION)
                .apply(CommonValidators::required)
                .apply(ObjectValidator::objectType, ObjectDefinition.class, msg.getObjectType())
                .applyRegistered()
                .pop();
        return ctx;
    }

    private static ValidationContext createOrUpdate(ValidationContext ctx, boolean isVersioned, boolean apiTrust, boolean tenantRequired) {

        if (tenantRequired) {
            ctx = ctx.push(MWR_TENANT)
                    .apply(CommonValidators::required)
                    .apply(CommonValidators::identifier)
                    .pop();
        } else {
            ctx = ctx.push(MWR_TENANT)
                    .apply(CommonValidators::omitted)
                    .pop();
        }

        ctx = ctx.push(MWR_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, ObjectType.class)
                .applyIf(isVersioned, ObjectIdValidator::versioningSupported, ObjectType.class)
                .pop();

        ctx = ctx.pushRepeated(MWR_ATTRS)
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                // Only allow reserved attrs for requests on the trusted API
                .applyRepeated(TagUpdateValidator::reservedAttrs, TagUpdate.class, (apiTrust == TRUSTED_API))
                .pop();

        return ctx;
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

    @Validator(method = "getObject")
    public static ValidationContext getObject(MetadataGetRequest msg, ValidationContext ctx) {

        ctx = ctx.apply(MetadataApiValidator::getRequest, MetadataGetRequest.class);

        ctx = ctx.push(MGR_OBJECT_VERSION)
                .apply(CommonValidators::required)
                .apply(CommonValidators::positive, Integer.class)
                .pop();

        ctx = ctx.push(MGR_TAG_VERSION)
                .apply(CommonValidators::required)
                .apply(CommonValidators::positive, Integer.class)
                .pop();

        return ctx;
    }

    @Validator(method = "getObject")
    public static ValidationContext getLatestObject(MetadataGetRequest msg, ValidationContext ctx) {

        ctx = ctx.apply(MetadataApiValidator::getRequest, MetadataGetRequest.class);

        ctx = ctx.push(MGR_OBJECT_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        ctx = ctx.push(MGR_TAG_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        return ctx;
    }

    @Validator(method = "getObject")
    public static ValidationContext getLatestTag(MetadataGetRequest msg, ValidationContext ctx) {

        ctx = ctx.apply(MetadataApiValidator::getRequest, MetadataGetRequest.class);

        ctx = ctx.push(MGR_OBJECT_VERSION)
                .apply(CommonValidators::required)
                .apply(CommonValidators::positive, Integer.class)
                .pop();

        ctx = ctx.push(MGR_TAG_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        return ctx;
    }

    private static ValidationContext getRequest(MetadataGetRequest msg, ValidationContext ctx) {

        ctx = ctx.push(MGR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(MGR_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::recognizedEnum, ObjectType.class)
                .pop();

        ctx = ctx.push(MGR_OBJECT_ID)
                .apply(CommonValidators::required)
                .apply(CommonValidators::uuid)
                .pop();

        return ctx;
    }

    private static ValidationFunction.Typed<MetadataWriteRequest> uniqueObjectIdCheck(Set<String> knownObjectIds) {
        return (request, ctx) -> {
            var objectId = request.getPriorVersion().getObjectId();
            if (request.getPriorVersion().getObjectId().isEmpty()) {
                return ctx;
            }

            if (knownObjectIds.contains(objectId)) {

                var err = String.format(
                        "object ID [%s] is already modified in the write batch",
                        objectId
                );

                return ctx.error(err);
            }

            knownObjectIds.add(objectId);
            return ctx;
        };
    }
}
