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

import com.google.protobuf.Descriptors;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.CommonValidators;
import org.finos.tracdap.common.validation.static_.ObjectIdValidator;
import org.finos.tracdap.metadata.TagSelector;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC, serviceFile = MetadataTrusted.class, serviceName = TrustedMetadataApiGrpc.SERVICE_NAME)
public class MetadataTrustedApiValidator {
    private static final Descriptors.Descriptor UNIVERSAL_METADATA_WRITE_BATCH_REQUEST;
    private static final Descriptors.FieldDescriptor UMWBR_TENANT;

    static {
        UNIVERSAL_METADATA_WRITE_BATCH_REQUEST = UniversalMetadataWriteBatchRequest.getDescriptor();
        UMWBR_TENANT = field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.TENANT_FIELD_NUMBER);
    }

    // Let's not introduce validation differences between the trusted and regular metadata API
    // Ideally, they should be made into the same API, with differences managed by permissions
    // Or at the very least, two instances of the same API with different settings
    // Currently, the check for what can be done via the trusted/untrusted API is in the service implementation
    // So, validation only needs to worry about what is a semantically valid request
    // This avoids mixing semantics with authorisation

    // Based on this thinking, trusted API validator is just a wrapper on untrusted API validator

    @Validator(method = "createObject")
    public static ValidationContext createObject(MetadataWriteRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.createObject(msg, ctx, MetadataApiValidator.TRUSTED_API, true);
    }

    @Validator(method = "createObjectBatch")
    public static ValidationContext createObjectBatch(MetadataWriteBatchRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.createObjectBatch(msg, ctx, MetadataApiValidator.TRUSTED_API);
    }

    @Validator(method = "updateObject")
    public static ValidationContext updateObject(MetadataWriteRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.updateObject(msg, ctx, MetadataApiValidator.TRUSTED_API, true);
    }

    @Validator(method = "updateObjectBatch")
    public static ValidationContext updateObjectBatch(MetadataWriteBatchRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.updateObjectBatch(msg, ctx, MetadataApiValidator.TRUSTED_API);
    }

    @Validator(method = "updateTag")
    public static ValidationContext updateTag(MetadataWriteRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.updateTag(msg, ctx, MetadataApiValidator.TRUSTED_API, true);
    }

    @Validator(method = "updateTagBatch")
    public static ValidationContext updateTagBatch(MetadataWriteBatchRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.updateTagBatch(msg, ctx, MetadataApiValidator.TRUSTED_API);
    }

    @Validator(method = "preallocateId")
    public static ValidationContext preallocateId(MetadataWriteRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.preallocateId(msg, ctx);  // always a trusted call
    }

    @Validator(method = "preallocateIdBatch")
    public static ValidationContext preallocateIdBatch(MetadataWriteBatchRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.preallocateIdBatch(msg, ctx);  // always a trusted call
    }

    @Validator(method = "createPreallocatedObject")
    public static ValidationContext createPreallocatedObject(MetadataWriteRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.createPreallocatedObject(msg, ctx);  // always a trusted call
    }

    @Validator(method = "createPreallocatedObjectBatch")
    public static ValidationContext createPreallocatedObjectBatch(MetadataWriteBatchRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.createPreallocatedObjectBatch(msg, ctx);  // always a trusted call
    }

    @Validator(method = "universalWrite")
    public static ValidationContext universalWrite(UniversalMetadataWriteBatchRequest msg, ValidationContext ctx) {
        ctx = ctx.push(UMWBR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.pushRepeated(field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.CREATEOBJECTS_FIELD_NUMBER))
                .applyRepeated(MetadataTrustedApiValidator::createObject, MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.CREATEPREALLOCATEDOBJECTS_FIELD_NUMBER))
                .applyRepeated(MetadataTrustedApiValidator::createPreallocatedObject, MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.UPDATEOBJECTS_FIELD_NUMBER))
                .applyRepeated(MetadataTrustedApiValidator::updateObject, MetadataWriteRequest.class)
                .pop();

        ctx = ctx.pushRepeated(field(UNIVERSAL_METADATA_WRITE_BATCH_REQUEST, UniversalMetadataWriteBatchRequest.UPDATETAGS_FIELD_NUMBER))
                .applyRepeated(MetadataTrustedApiValidator::updateTag, MetadataWriteRequest.class)
                .pop();

        return ctx;
    }

    @Validator(method = "readObject")
    public static ValidationContext readObject(MetadataReadRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.readObject(msg, ctx);
    }

    @Validator(method = "search")
    public static ValidationContext search(MetadataSearchRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.search(msg, ctx);
    }

    @Validator(method = "readBatch")
    public static ValidationContext readBatch(MetadataBatchRequest msg, ValidationContext ctx) {
        return MetadataApiValidator.readBatch(msg, ctx);
    }
}
