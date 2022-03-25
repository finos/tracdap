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


@Validator(type = ValidationType.STATIC, serviceFile = MetadataTrusted.class, serviceName = TrustedMetadataApiGrpc.SERVICE_NAME)
public class MetadataTrustedApiValidator {

    // Let's not introduce validation differences between the trusted and regular metadata API
    // Ideally, they should be made into the same API, with differences managed by permissions
    // Or at the very least, two instances of the same API with different settings
    // Currently, the check for what can be done via the trusted/untrusted API is in the service implementation
    // So, validation only needs to worry about what is a semantically valid request
    // This avoids mixing semantics with authorisation

    // Based on this thinking, trusted API validator is just a wrapper on untrusted API validator

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
