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
 * Indicates a problem with data held by or presented to TRAC
 *
 * Data errors relate to the data itself. Examples include
 * - Data does not pass quality or validation checks
 * - Data is in the wrong format
 * - Contents of a data file has become corrupt
 *
 * This is distinct from a storage error, which indicates a problem communicating with the underlying
 * storage technology. Storage errors can generally be corrected by resolving issues in the technology
 * stack, where data errors will require changes to data (or restoring from backup in the case of corruption).
 *
 * Because data errors relate to the content of the data itself,
 * these are business-meaningful errors and should be reported to end users.
 */
public class EData extends ETracPublic {

    public EData(String message, Throwable cause) {
        super(message, cause);
    }

    public EData(String message) {
        super(message);
    }
}
