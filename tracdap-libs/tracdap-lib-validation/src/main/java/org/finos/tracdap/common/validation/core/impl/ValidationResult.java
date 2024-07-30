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

package org.finos.tracdap.common.validation.core.impl;

import io.grpc.Status;
import org.finos.tracdap.api.TracErrorDetails;
import org.finos.tracdap.api.TracErrorItem;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class ValidationResult {

    private static final String MULTIPLE_ERRORS_MESSAGE = "There were multiple validation errors";

    private final ValidationType validationType;
    private final String shortName;
    private final List<ValidationFailure> failures;

    public static ValidationResult pass(ValidationType validationType, String shortName) {
        return new ValidationResult(validationType, shortName, List.of());
    }

    public static ValidationResult forContext(ValidationContext ctx) {
        return new ValidationResult(ctx.validationType(), ctx.key().shortName(), ctx.getErrors());
    }

    public ValidationResult(ValidationType validationType, String shortName, List<ValidationFailure> failures) {

        this.validationType = validationType;
        this.shortName = shortName;
        this.failures = Collections.unmodifiableList(failures);
    }

    public boolean ok() {
        return failures.isEmpty();
    }

    public List<ValidationFailure> failures() {
        return failures;
    }

    public String failureMessage() {

        if (failures.isEmpty())
            return "";

        if (failures.size() == 1)
            return failures.get(0).locationAndMessage();

        return MULTIPLE_ERRORS_MESSAGE + "\n" + failures().stream()
                .map(ValidationFailure::locationAndMessage)
                .collect(Collectors.joining("\n"));
    }

    public TracErrorDetails errorDetails() {

        var details = TracErrorDetails.newBuilder();

        switch (validationType) {

            case STATIC:
                details.setCode(Status.Code.INVALID_ARGUMENT.value());
                details.setMessage("Validation failed for [" + shortName + "]");
                break;

            case VERSION:
                details.setCode(Status.Code.FAILED_PRECONDITION.value());
                details.setMessage("Validation failed for version"); // todo
                break;

            case REFERENTIAL:
                details.setCode(Status.Code.FAILED_PRECONDITION.value());
                details.setMessage("Validation failed for referential");
                break;

            default:
                throw new EUnexpected();
        }

        for (var failure : failures) {

            var errorItem = TracErrorItem.newBuilder()
                    .setDetail(failure.message())
                    .setLocation(failure.location())
                    .build();

            details.addItems(errorItem);
        }

        return details.build();
    }
}
