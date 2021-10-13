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

package com.accenture.trac.svc.data.validation;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;


public class ValidationContext {

    private final List<ValidationLocation> location;
    private final List<ValidationFailure> failures;
    private final ValidationContext parent;

    public static ValidationContext newContext() {
        return new ValidationContext();
    }

    public ValidationContext push(ValidationLocation location) {

        var newLocation = new LinkedList<ValidationLocation>();  // location.withParent(this.location);
        newLocation.add(location);  // TODO

        return new ValidationContext(
                newLocation,
                this.failures,
                this);
    }

    public ValidationContext pop() {

        if (parent == null)
            throw new IllegalStateException();

        if (parent.failures.size() == failures.size())
            return parent;

        return new ValidationContext(
                parent.location,
                this.failures,
                parent.parent);
    }

    public ValidationContext error(String message) {

        var failure = new ValidationFailure(new ValidationLocation(null), message);

        var failures = new Vector<ValidationFailure>(this.failures.size() + 1);
        failures.addAll(this.failures);
        failures.add(failure);

        return new ValidationContext(this.location, failures, this.parent);
    }

    public List<ValidationFailure> getErrors() {
        return failures;
    }

    private ValidationContext() {
        location = new LinkedList<>();
        failures = new ArrayList<>();
        parent = null;
    }

    private ValidationContext(
            List<ValidationLocation> location,
            List<ValidationFailure> failures,
            ValidationContext parent) {

        this.location = location;
        this.failures = failures;
        this.parent = parent;
    }

}
