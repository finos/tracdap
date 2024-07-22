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

package org.finos.tracdap.common.exception;


import org.finos.tracdap.api.TracErrorDetails;

/**
 * A validation failure for version compatibility
 *
 * Version validation checks consecutive object versions for backwards compatibility.
 * A failure during this validation stage results in a version validation error.
 */
public class EVersionValidation extends EValidation {

    public EVersionValidation(String message, Throwable cause) {
        super(message, cause);
    }

    public EVersionValidation(String message, TracErrorDetails details) {
        super(message, details);
    }

    public EVersionValidation(String message) {
        super(message);
    }
}
