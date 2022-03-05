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

package com.accenture.trac.common.metadata;

import java.util.regex.Pattern;


public class MetadataConstants {

    public static final int OBJECT_FIRST_VERSION = 1;
    public static final int TAG_FIRST_VERSION = 1;

    // Valid identifiers are made up of alphanumeric characters and the underscore, starting with a letter
    // Use \\A - \\Z to match the whole input
    // ^...$ would allow matches like "my_var\n_gotcha"
    public static final Pattern VALID_IDENTIFIER = Pattern.compile("\\A[a-zA-Z]\\w*\\Z");   // Leading _ not allowed

    // Identifiers starting trac_ or _ are reserved for use by the TRAC platform
    public static final Pattern TRAC_RESERVED_IDENTIFIER = Pattern.compile("\\A(trac_|_).*", Pattern.CASE_INSENSITIVE);

    public static final String TRAC_CREATE_TIME = "trac_create_time";
    public static final String TRAC_CREATE_USER = "trac_create_user";
    public static final String TRAC_CREATE_JOB = "trac_create_job";

    public static final String TRAC_UPDATE_TIME = "trac_update_time";
    public static final String TRAC_UPDATE_USER = "trac_update_user";
    public static final String TRAC_UPDATE_JOB = "trac_update_job";

    public static final String TRAC_JOB_TYPE_ATTR = "trac_job_type";
    public static final String TRAC_JOB_STATUS_ATTR = "trac_job_status";

    public static final String TRAC_SCHEMA_TYPE_ATTR = "trac_schema_type";

    public static final String TRAC_FILE_NAME_ATTR = "trac_file_name";
    public static final String TRAC_FILE_EXTENSION_ATTR = "trac_file_extension";
    public static final String TRAC_FILE_MIME_TYPE_ATTR = "trac_file_mime_type";
    public static final String TRAC_FILE_SIZE_ATTR = "trac_file_size";

    public static final String TRAC_STORAGE_OBJECT_ATTR = "trac_storage_object";

    public static final String TRAC_MODEL_LANGUAGE = "trac_model_language";
    public static final String TRAC_MODEL_REPOSITORY = "trac_model_repository";
    public static final String TRAC_MODEL_PATH = "trac_model_path";
    public static final String TRAC_MODEL_ENTRY_POINT = "trac_model_entry_point";
    public static final String TRAC_MODEL_VERSION = "trac_model_version";
}
