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
import org.finos.tracdap.common.metadata.MetadataUtil;
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

    private static final Descriptors.Descriptor METADATA_WRITE_BATCH_REQUEST;
    private static final Descriptors.FieldDescriptor MWBR_TENANT;
    private static final Descriptors.FieldDescriptor MWBR_CREATE_OBJECTS;
    private static final Descriptors.FieldDescriptor MWBR_UPDATE_OBJECTS;
    private static final Descriptors.FieldDescriptor MWBR_UPDATE_TAGS;
    private static final Descriptors.FieldDescriptor MWBR_PREALLOCATE_IDS;
    private static final Descriptors.FieldDescriptor MWBR_CREATE_PREALLOCATED;

    private static final Descriptors.Descriptor METADATA_READ_REQUEST;
    private static final Descriptors.FieldDescriptor MRR_TENANT;
    private static final Descriptors.FieldDescriptor MRR_SELECTOR;

    private static final Descriptors.Descriptor BATCH_READ_REQUEST;
    private static final Descriptors.FieldDescriptor BRR_TENANT;
    private static final Descriptors.FieldDescriptor BRR_SELECTORS;

    private static final Descriptors.Descriptor METADATA_SEARCH_REQUEST;
    private static final Descriptors.FieldDescriptor MSR_TENANT;
    private static final Descriptors.FieldDescriptor MSR_SEARCH_PARAMS;

    private static final Descriptors.Descriptor METADATA_GET_REQUEST;
    private static final Descriptors.FieldDescriptor MGR_TENANT;
    private static final Descriptors.FieldDescriptor MGR_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor MGR_OBJECT_ID;
    private static final Descriptors.FieldDescriptor MGR_OBJECT_VERSION;
    private static final Descriptors.FieldDescriptor MGR_TAG_VERSION;

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

        METADATA_WRITE_BATCH_REQUEST = MetadataWriteBatchRequest.getDescriptor();
        MWBR_TENANT = field(METADATA_WRITE_BATCH_REQUEST, MetadataWriteBatchRequest.TENANT_FIELD_NUMBER);
        MWBR_CREATE_OBJECTS = field(METADATA_WRITE_BATCH_REQUEST, MetadataWriteBatchRequest.CREATEOBJECTS_FIELD_NUMBER);
        MWBR_UPDATE_OBJECTS = field(METADATA_WRITE_BATCH_REQUEST, MetadataWriteBatchRequest.UPDATEOBJECTS_FIELD_NUMBER);
        MWBR_UPDATE_TAGS = field(METADATA_WRITE_BATCH_REQUEST, MetadataWriteBatchRequest.UPDATETAGS_FIELD_NUMBER);
        MWBR_PREALLOCATE_IDS = field(METADATA_WRITE_BATCH_REQUEST, MetadataWriteBatchRequest.PREALLOCATEIDS_FIELD_NUMBER);
        MWBR_CREATE_PREALLOCATED = field(METADATA_WRITE_BATCH_REQUEST, MetadataWriteBatchRequest.CREATEPREALLOCATED_FIELD_NUMBER);

        METADATA_SEARCH_REQUEST = MetadataSearchRequest.getDescriptor();
        MSR_TENANT = field(METADATA_SEARCH_REQUEST, MetadataSearchRequest.TENANT_FIELD_NUMBER);
        MSR_SEARCH_PARAMS = field(METADATA_SEARCH_REQUEST, MetadataSearchRequest.SEARCHPARAMS_FIELD_NUMBER);

        METADATA_GET_REQUEST = MetadataGetRequest.getDescriptor();
        MGR_TENANT = field(METADATA_GET_REQUEST, MetadataGetRequest.TENANT_FIELD_NUMBER);
        MGR_OBJECT_TYPE = field(METADATA_GET_REQUEST, MetadataGetRequest.OBJECTTYPE_FIELD_NUMBER);
        MGR_OBJECT_ID = field(METADATA_GET_REQUEST, MetadataGetRequest.OBJECTID_FIELD_NUMBER);
        MGR_OBJECT_VERSION = field(METADATA_GET_REQUEST, MetadataGetRequest.OBJECTVERSION_FIELD_NUMBER);
        MGR_TAG_VERSION = field(METADATA_GET_REQUEST, MetadataGetRequest.TAGVERSION_FIELD_NUMBER);
    }

    @Validator(method = "createObject")
    public static ValidationContext createObject(MetadataWriteRequest msg, ValidationContext ctx) {

        return createObject(msg, ctx, PUBLIC_API, null);
    }

    static ValidationContext createObject(MetadataWriteRequest msg, ValidationContext ctx, boolean apiTrust, String expectedTenant) {

        ctx = createOrUpdate(ctx, false, apiTrust, expectedTenant);

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

        return updateObject(msg, ctx, PUBLIC_API, null);
    }

    static ValidationContext updateObject(MetadataWriteRequest msg, ValidationContext ctx, boolean apiTrust, String expectedTenant) {

        ctx = createOrUpdate(ctx, true, apiTrust, expectedTenant);

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

        return updateTag(msg, ctx, PUBLIC_API, null);
    }

    static ValidationContext updateTag(MetadataWriteRequest msg, ValidationContext ctx, boolean apiTrust, String expectedTenant) {

        ctx = createOrUpdate(ctx, false, apiTrust, expectedTenant);

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

        return preallocateId(msg, ctx, null);
    }

    private static ValidationContext preallocateId(MetadataWriteRequest msg, ValidationContext ctx, String expectedTenant) {

        ctx = createOrUpdate(ctx, false, TRUSTED_API, expectedTenant);

        ctx = ctx.push(MWR_PRIOR_VERSION)
                .apply(CommonValidators::omitted)
                .pop();

        ctx = ctx.push(MWR_DEFINITION)
                .apply(CommonValidators::omitted)
                .pop();
        
        return ctx;
    }

    static ValidationContext createPreallocatedObject(MetadataWriteRequest msg, ValidationContext ctx) {

        return createPreallocatedObject(msg, ctx, null);
    }

    private static ValidationContext createPreallocatedObject(MetadataWriteRequest msg, ValidationContext ctx, String expectedTenantd) {

        ctx = createOrUpdate(ctx, false, TRUSTED_API, expectedTenantd);

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

    @Validator(method = "writeBatch")
    public static ValidationContext writeBatch(MetadataWriteBatchRequest msg, ValidationContext ctx) {

        return writeBatch(msg, ctx, PUBLIC_API);
    }

    public static ValidationContext writeBatch(MetadataWriteBatchRequest msg, ValidationContext ctx, boolean apiTrust) {

        ctx = ctx.push(MWBR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        if (msg.getCreateObjectsCount() == 0 &&
            msg.getUpdateObjectsCount() == 0 &&
            msg.getUpdateTagsCount() == 0 &&
            msg.getPreallocateIdsCount() == 0 &&
            msg.getCreatePreallocatedCount() == 0) {

            return ctx.error("Write batch request does not contain any operations");
        }

        ctx = ctx.pushRepeated(MWBR_CREATE_OBJECTS)
                .applyRepeated((m, c) -> createObject(m, c, apiTrust, msg.getTenant()), MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(MWBR_UPDATE_OBJECTS)
                .applyRepeated((m, c) -> updateObject(m, c, apiTrust, msg.getTenant()), MetadataWriteRequest.class)
                .applyRepeated(uniquePriorObject(), MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(MWBR_UPDATE_TAGS)
                .applyRepeated((m, c) -> updateTag(m, c, apiTrust, msg.getTenant()), MetadataWriteRequest.class)
                .applyRepeated(uniquePriorVersion(), MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(MWBR_PREALLOCATE_IDS)
                .applyRepeated((m, c) -> preallocateId(m, c, msg.getTenant()), MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(MWBR_CREATE_PREALLOCATED)
                .applyRepeated((m, c) -> createPreallocatedObject(m, c, msg.getTenant()), MetadataWriteRequest.class)
                .applyRepeated(uniquePriorObject(), MetadataWriteRequest.class)
                .pop();

        return ctx;
    }

    private static ValidationContext createOrUpdate(ValidationContext ctx, boolean isVersioned, boolean apiTrust, String expectedTenant) {

        // For individual requests, expectedTenant == null and the tenant must be specified in the request
        if (expectedTenant == null) {

            ctx = ctx.push(MWR_TENANT)
                    .apply(CommonValidators::required)
                    .apply(CommonValidators::identifier)
                    .pop();
        }
        // For batch requests, expectedTenant is the batch request tenant
        // Including tenant in the individual requests is optional, if it is specified it must match the batch request
        else {

            ctx = ctx.push(MWR_TENANT)
                    .apply(CommonValidators::optional)
                    .apply(CommonValidators::identifier)
                    .apply(CommonValidators.equalTo(expectedTenant, "Tenant does not match the batch request"))
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

    @Validator(method = "getLatestObject")
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

    @Validator(method = "getLatestTag")
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

    private static ValidationFunction.Typed<MetadataWriteRequest> uniquePriorObject() {

        var knownIds = new HashSet<String>();
        return (msg, ctx) -> uniquePriorObject(msg, ctx, knownIds);
    }

    private static ValidationContext uniquePriorObject(MetadataWriteRequest msg, ValidationContext ctx, Set<String> knownObjectIds) {

        var priorId = msg.getPriorVersion().getObjectId();
        var alreadyPresent = knownObjectIds.add(priorId);

        if (alreadyPresent) {

            var err = String.format("Duplicate object ID [%s] in batch operation", priorId);
            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationFunction.Typed<MetadataWriteRequest> uniquePriorVersion() {

        var knownVersions = new HashSet<String>();
        return (msg, ctx) -> uniquePriorVersion(msg, ctx, knownVersions);
    }

    private static ValidationContext uniquePriorVersion(MetadataWriteRequest msg, ValidationContext ctx, Set<String> knownObjectIds) {

        var priorKey = MetadataUtil.objectKey(msg.getPriorVersion());
        var alreadyPresent = knownObjectIds.add(priorKey);

        if (alreadyPresent) {

            var err = String.format(
                    "Duplicate operation for object [%s] version [%d] in batch operation",
                    msg.getPriorVersion().getObjectId(), msg.getPriorVersion().getObjectVersion());

            return ctx.error(err);
        }

        return ctx;
    }
}
