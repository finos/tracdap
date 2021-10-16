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

package com.accenture.trac.svc.data.validation.core;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class ValidationResult {

    private final List<ValidationFailure> failures;

    public static ValidationResult pass() {
        return new ValidationResult(List.of());
    }

    public static ValidationResult forContext(ValidationContext ctx) {
        return new ValidationResult(ctx.getErrors());
    }

    public ValidationResult(List<ValidationFailure> failures) {

        this.failures = Collections.unmodifiableList(failures);
    }

    public boolean ok() {
        return failures.isEmpty();
    }

    public List<ValidationFailure> failures() {
        return failures;
    }

    public String failureMessage() {

        return failures().stream()
                .map(ValidationFailure::message)
                .collect(Collectors.joining("\n"));
    }
}
