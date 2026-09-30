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

package org.finos.tracdap.common.service;

import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.PlatformConfigEntry;

/**
 * A single config write, as seen by an {@link IConfigWriteHook}.
 * Tenant writes carry a prior {@link ConfigEntry}, platform writes a prior {@link PlatformConfigEntry}.
 */
public class ConfigWrite {

    private final String tenant;
    private final ConfigUpdateType operation;
    private final String configClass;
    private final String configKey;
    private final ConfigEntry priorEntry;
    private final PlatformConfigEntry priorPlatformEntry;
    private final ObjectDefinition priorDefinition;
    private final ObjectDefinition definition;
    private final UserMetadata userMetadata;
    private final IConfigReader reader;

    private ConfigWrite(
            String tenant, ConfigUpdateType operation, String configClass, String configKey,
            ConfigEntry priorEntry, PlatformConfigEntry priorPlatformEntry,
            ObjectDefinition priorDefinition, ObjectDefinition definition,
            UserMetadata userMetadata, IConfigReader reader) {

        this.tenant = tenant;
        this.operation = operation;
        this.configClass = configClass;
        this.configKey = configKey;
        this.priorEntry = priorEntry;
        this.priorPlatformEntry = priorPlatformEntry;
        this.priorDefinition = priorDefinition;
        this.definition = definition;
        this.userMetadata = userMetadata;
        this.reader = reader;
    }

    public static ConfigWrite forTenant(
            String tenant, ConfigUpdateType operation, String configClass, String configKey,
            ConfigEntry priorEntry, ObjectDefinition priorDefinition, ObjectDefinition definition,
            UserMetadata userMetadata, IConfigReader reader) {

        return new ConfigWrite(
                tenant, operation, configClass, configKey,
                priorEntry, null, priorDefinition, definition,
                userMetadata, reader);
    }

    public static ConfigWrite forPlatform(
            ConfigUpdateType operation, String configClass, String configKey,
            PlatformConfigEntry priorEntry, ObjectDefinition priorDefinition, ObjectDefinition definition,
            UserMetadata userMetadata, IConfigReader reader) {

        return new ConfigWrite(
                null, operation, configClass, configKey,
                null, priorEntry, priorDefinition, definition,
                userMetadata, reader);
    }

    /** The tenant code, or null for platform config */
    public String tenant() {
        return tenant;
    }

    public ConfigUpdateType operation() {
        return operation;
    }

    public String configClass() {
        return configClass;
    }

    public String configKey() {
        return configKey;
    }

    /** The prior tenant config entry, for tenant updates and deletes */
    public ConfigEntry priorEntry() {
        return priorEntry;
    }

    /** The prior platform config entry, for platform updates and deletes */
    public PlatformConfigEntry priorPlatformEntry() {
        return priorPlatformEntry;
    }

    /** The prior definition, for updates and deletes */
    public ObjectDefinition priorDefinition() {
        return priorDefinition;
    }

    /** The submitted definition, for creates and updates */
    public ObjectDefinition definition() {
        return definition;
    }

    public UserMetadata userMetadata() {
        return userMetadata;
    }

    public IConfigReader reader() {
        return reader;
    }
}
