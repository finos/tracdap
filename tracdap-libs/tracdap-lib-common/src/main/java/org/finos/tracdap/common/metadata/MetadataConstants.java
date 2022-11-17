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

package org.finos.tracdap.common.metadata;

import org.finos.tracdap.metadata.ObjectType;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;


public class MetadataConstants {

    public static final int OBJECT_FIRST_VERSION = 1;
    public static final int TAG_FIRST_VERSION = 1;

    // A limited set of object types can be created directly by clients
    // Everything else can only be created by the TRAC platform
    // I.e. TRAC components have to make trusted calls to the metadata service
    public static final List<ObjectType> PUBLIC_WRITABLE_OBJECT_TYPES = Arrays.asList(
            ObjectType.SCHEMA,
            ObjectType.FLOW,
            ObjectType.CUSTOM);

    // Only certain object types can be versioned
    // To enable versioning for an object type, add it to this list
    // A version validator must also be implemented for the object type
    // Otherwise update operations will fail due to a missing validator
    public static final Set<ObjectType> VERSIONED_OBJECT_TYPES = Set.of(
            ObjectType.DATA,
            ObjectType.SCHEMA,
            ObjectType.FILE,
            ObjectType.STORAGE,
            ObjectType.CUSTOM);

    // Valid identifiers are made up of alphanumeric characters, numbers and the underscore, not starting with a number
    // Use \\A - \\Z to match the whole input
    // ^...$ would allow matches like "my_var\n_gotcha"
    public static final Pattern VALID_IDENTIFIER = Pattern.compile("\\A[a-zA-Z_]\\w*\\Z");

    // Identifiers starting trac_ are reserved for use by the TRAC platform
    // Identifiers starting _ are also reserved by convention, for private / protected / system variables
    public static final Pattern TRAC_RESERVED_IDENTIFIER = Pattern.compile("\\A(trac_|_).*", Pattern.CASE_INSENSITIVE);

    public static final String TRAC_CREATE_TIME = "trac_create_time";
    public static final String TRAC_CREATE_USER_ID = "trac_create_user_id";
    public static final String TRAC_CREATE_USER_NAME = "trac_create_user_name";
    public static final String TRAC_CREATE_JOB = "trac_create_job";

    public static final String TRAC_UPDATE_TIME = "trac_update_time";
    public static final String TRAC_UPDATE_USER_ID = "trac_update_user_id";
    public static final String TRAC_UPDATE_USER_NAME = "trac_update_user_name";
    public static final String TRAC_UPDATE_JOB = "trac_update_job";

    public static final String TRAC_JOB_TYPE_ATTR = "trac_job_type";
    public static final String TRAC_JOB_STATUS_ATTR = "trac_job_status";
    public static final String TRAC_JOB_ERROR_MESSAGE_ATTR = "trac_job_error_message";

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
