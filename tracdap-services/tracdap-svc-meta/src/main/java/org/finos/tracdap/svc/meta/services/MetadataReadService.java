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

package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.util.VersionInfo;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.TenantConfigMap;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.store.IMetadataStore;
import org.finos.tracdap.svc.meta.TracMetadataService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;


public class MetadataReadService {

    private static final String ENVIRONMENT_NOT_SET = "ENVIRONMENT_NOT_SET";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IMetadataStore metadataStore;
    private final PlatformConfig platformConfig;
    private final TenantConfigMap tenantConfig;

    public MetadataReadService(IMetadataStore metadataStore, PlatformConfig platformConfig, TenantConfigMap tenantConfig) {
        this.metadataStore = metadataStore;
        this.platformConfig = platformConfig;
        this.tenantConfig = tenantConfig;
    }

    // Literally all the read logic is in the DAL at present!
    // Which is fine, keep a thin logic class here anyway to have a consistent pattern

    public PlatformInfoResponse platformInfo() {

        var tracVersion = VersionInfo.getComponentVersion(TracMetadataService.class);

        var platformInfo = platformConfig.getPlatformInfo();
        var environment = platformInfo.getEnvironment();
        var production = platformInfo.getProduction();    // defaults to false

        // TODO: Validate environment is set during startup
        if (environment.isBlank())
            environment = ENVIRONMENT_NOT_SET;

        return  PlatformInfoResponse.newBuilder()
                .setTracVersion(tracVersion)
                .setEnvironment(environment)
                .setProduction(production)
                .putAllDeploymentInfo(platformInfo.getDeploymentInfoMap())
                .build();
    }

    public ListTenantsResponse listTenants() {
        
        var metadataTenants = metadataStore.listTenants();
        var configFileTenants = new HashMap<>(tenantConfig.getTenantsMap());

        var response = ListTenantsResponse.newBuilder();

        for (var tenantInfo : metadataTenants) {

            var configFileEntry = configFileTenants.remove(tenantInfo.getTenantCode());
            var processedEntry = tenantInfo.toBuilder();

            if (configFileEntry != null && !configFileEntry.getDisplayName().isBlank()) {
                processedEntry.setDescription(configFileEntry.getDisplayName());
            }
            else if (tenantInfo.getDescription().isBlank()) {
                // Use tenant code as the display name if nothing has been set
                processedEntry.setDescription(tenantInfo.getTenantCode());
            }

            response.addTenants(processedEntry);
        }

        if (!configFileTenants.isEmpty()) {
            var inactiveTenants = String.join(",", configFileTenants.keySet());
            log.warn("Some tenants are configured but not activated: [{}]", inactiveTenants);
        }

        return response.build();
    }

    public Tag readObject(String tenant, TagSelector selector) {

        return metadataStore.loadObject(tenant, selector);
    }

    public List<Tag> readObjects(String tenant, List<TagSelector> selectors) {

        return metadataStore.loadObjects(tenant, selectors);
    }

    public Tag loadTag(
            String tenant, ObjectType objectType,
            UUID objectId, int objectVersion, int tagVersion) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(objectVersion)
                .setTagVersion(tagVersion)
                .build();

        return metadataStore.loadObject(tenant, selector);
    }

    public Tag loadLatestTag(
            String tenant, ObjectType objectType,
            UUID objectId, int objectVersion) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(objectVersion)
                .setLatestTag(true)
                .build();

        return metadataStore.loadObject(tenant, selector);
    }

    public Tag loadLatestObject(
            String tenant, ObjectType objectType,
            UUID objectId) {

        var selector = TagSelector.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        return metadataStore.loadObject(tenant, selector);
    }
}
