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
 * A validation error during initial validation of request inputs
 *
 * Input validation errors refer to the first phase of validation, which is
 * validation of fixed inputs. This validation refers to inputs in isolation,
 * i.e. as a static object tree with no references to other versions or referenced
 * objects. As such, input validations are always a result of invalid requests and
 * are reported as ILLEGAL_ARGUMENT errors.
 */
public class EInputValidation extends EValidation {

    public EInputValidation(String message, Throwable cause) {
        super(message, cause);
    }

    public EInputValidation(String message, TracErrorDetails details) {
        super(message, details);
    }

    public EInputValidation(String message) {
        super(message);
    }
}
