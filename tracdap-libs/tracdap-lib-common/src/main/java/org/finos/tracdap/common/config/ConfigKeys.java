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

    // Secondary config keys
    public static final String LOGGING_CONFIG_KEY = "logging";
    public static final String SECRET_TYPE_KEY = "secret.type";
    public static final String SECRET_URL_KEY = "secret.url";
    public static final String SECRET_KEY_KEY = "secret.key";
    public static final String SECRET_KEY_ENV = "TRAC_SECRET_KEY";
    public static final String USER_DB_TYPE = "users.type";
    public static final String USER_DB_URL = "users.url";
    public static final String USER_DB_KEY = "users.key";

    // Service keys
    public static final String METADATA_SERVICE_KEY = "metadata";
    public static final String DATA_SERVICE_KEY = "data";
    public static final String ORCHESTRATOR_SERVICE_KEY = "orchestrator";
    public static final String GATEWAY_SERVICE_KEY = "gateway";

    // Service properties
    public static final String GATEWAY_ROUTE_NAME = "gateway.route.name";
    public static final String GATEWAY_ROUTE_PREFIX = "gateway.route.prefix";
    public static final String NETWORK_IDLE_TIMEOUT = "network.idleTimeout";
}
