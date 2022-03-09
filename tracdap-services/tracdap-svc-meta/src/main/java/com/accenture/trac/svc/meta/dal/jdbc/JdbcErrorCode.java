/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.dal.jdbc;

public enum JdbcErrorCode {
    UNKNOWN_ERROR_CODE,
    INSERT_DUPLICATE,
    INSERT_MISSING_FK,
    NO_DATA,
    TOO_MANY_ROWS,

    // Object type of a metadata item does not match what is stored / expected
    WRONG_OBJECT_TYPE,

    // The definition of a metadata item could not be understood
    INVALID_OBJECT_DEFINITION
}
