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

package org.finos.tracdap.common.config;

public class ConfigKeys {

    public static final String TRAC_PREFIX = "trac_";

    // Secondary config keys
    public static final String TENANTS_CONFIG_KEY = "tenants";
    public static final String LOGGING_CONFIG_KEY = "logging";
    public static final String SECRET_TYPE_KEY = "secret.type";
    public static final String SECRET_URL_KEY = "secret.url";
    public static final String SECRET_KEY_KEY = "secret.key";

    public static final String TRAC_SECRET_TYPE = "TRAC_SECRET_TYPE";
    public static final String TRAC_SECRET_URL = "TRAC_SECRET_URL";
    public static final String TRAC_SECRET_KEY = "TRAC_SECRET_KEY";

    // Service keys
    public static final String GATEWAY_SERVICE_KEY = "gateway";
    public static final String METADATA_SERVICE_KEY = "metadata";
    public static final String DATA_SERVICE_KEY = "data";
    public static final String ORCHESTRATOR_SERVICE_KEY = "orchestrator";
    public static final String ADMIN_SERVICE_KEY = "admin";

    // Service properties
    public static final String GATEWAY_ROUTE_NAME = "gateway.route.name";
    public static final String GATEWAY_ROUTE_PREFIX = "gateway.route.prefix";
    public static final String NETWORK_IDLE_TIMEOUT = "network.idleTimeout";

    // Storage defaults
    public static final String STORAGE_DEFAULT_LOCATION = "storage.default.location";
    public static final String STORAGE_DEFAULT_FORMAT = "storage.default.format";
    public static final String STORAGE_DEFAULT_LAYOUT = "storage.default.layout";

    // Runtime results
    public static final String RESULT_ENABLED = "result.enabled";
    public static final String RESULT_LOGS_ENABLED = "result.logs.enabled";
    public static final String RESULT_STORAGE_LOCATION = "result.storage.location";
    public static final String RESULT_STORAGE_PATH = "result.storage.path";
    public static final String RESULT_FORMAT = "result.format";

    // Well-known config classes
    public static final String TRAC_CONFIG = "trac_config";
    public static final String TRAC_RESOURCES = "trac_resources";

    // Tenant-level config entry
    public static final String TRAC_TENANT_CONFIG = "trac_tenant_config";
    public static final String TENANT_DISPLAY_NAME = "tenant.displayName";

    // Secret scopes
    public static final String CONFIG_SCOPE = "config";
    public static final String TENANT_SCOPE = "tenants";
    public static final String RESOURCE_SCOPE = "resources";
}
