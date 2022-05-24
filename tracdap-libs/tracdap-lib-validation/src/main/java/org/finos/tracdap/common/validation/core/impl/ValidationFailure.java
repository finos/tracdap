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


public class ValidationFailure {

    private final ValidationLocation location;
    private final String message;

    public ValidationFailure(ValidationLocation location, String message ) {
        this.location = location;
        this.message = message;
    }

    public String message() {

        if (location.isRoot())
            return message;
        else
            return location.elementPath() + ": " + message;
    }
}
