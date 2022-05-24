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


/**
 * Represents a validation error
 *
 * Child error types are available to represent the particular type of validation error
 * as they have different response codes. E.g. input validation is an ILLEGAL_ARGUMENT
 * response, where versioned and referential validation are FAILED_PRECONDITION.
 *
 * Validation errors are public errors that should be reported back to the client.
 */
public abstract class EValidation extends ETracPublic {

    public EValidation(String message, Throwable cause) {
        super(message, cause);
    }

    public EValidation(String message) {
        super(message);
    }
}
