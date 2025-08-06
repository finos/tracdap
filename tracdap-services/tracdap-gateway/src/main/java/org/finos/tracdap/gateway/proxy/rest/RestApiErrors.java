/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.gateway.proxy.rest;


public class RestApiErrors {


    // User facing errors - can happen at runtime during request translation

    public static final String INVALID_REQUEST_ENUM_VALUE =
            "Invalid REST API request: Bad enum value [%s] for field [%s]";

    public static final String INVALID_REQUEST_INTEGER_VALUE =
            "Invalid REST API request: Bad enum value [%s] for field [%s]";

    public static final String INVALID_REQUEST_LONG_VALUE =
            "Invalid REST API request: Bad enum value [%s] for field [%s]";

    public static final String INVALID_REQUEST_TOO_FEW_PATH_SEGMENTS =
            "Invalid REST API request: Incorrect path (too few segments))";

    public static final String INVALID_REQUEST_TOO_MANY_PATH_SEGMENTS =
            "Invalid REST API request: Incorrect path (too many segments)";

    public static final String INVALID_REQUEST_BAD_JSON_CONTENT =
            "Invalid REST API request: Error in JSON payload: %s";


    // Developer facing errors - can happen when trying to integrate new mappings

    public static final String INVALID_MAPPING_UNKNOWN_FIELD =
            "Invalid REST API mapping: Unknown field [%s] in type {%s] for method [%s]";

    public static final String INVALID_MAPPING_REQUIRED_FIELD_TYPE =
            "Invalid REST API mapping: Field [%s] used in %s has unsupported field type [%s] in method [%s]";

    public static final String INVALID_MAPPING_BAD_FIELD_TYPE =
            "Invalid REST API mapping: Field [%s] used in %s does not field type [%s] in method [%s]";

    public static final String INVALID_MAPPING_BAD_TEMPLATE =
            "Invalid REST API mapping: Illegal path template for method [%s]: %s";

    public static final String INVALID_MAPPING_BAD_HTTP_RULE =
            "Invalid REST API mapping: Illegal HTTP rule for method [%s]: %s";
}
