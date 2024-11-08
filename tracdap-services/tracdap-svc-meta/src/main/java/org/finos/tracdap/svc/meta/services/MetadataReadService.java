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

import org.finos.tracdap.api.ListResourcesResponse;
import org.finos.tracdap.api.ListTenantsResponse;
import org.finos.tracdap.api.PlatformInfoResponse;
import org.finos.tracdap.api.ResourceInfoResponse;
import org.finos.tracdap.common.exception.EResourceNotFound;
import org.finos.tracdap.common.exception.ETenantNotFound;
import org.finos.tracdap.common.util.VersionInfo;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.metadata.ResourceType;
import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.meta.dal.IMetadataDal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;


public class MetadataReadService {

    private static final String ENVIRONMENT_NOT_SET = "ENVIRONMENT_NOT_SET";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IMetadataDal dal;
    private final PlatformConfig config;

    public MetadataReadService(IMetadataDal dal, PlatformConfig platformConfig) {
        this.dal = dal;
        this.config = platformConfig;
    }

    // Literally all the read logic is in the DAL at present!
    // Which is fine, keep a thin logic class here anyway to have a consistent pattern

    public PlatformInfoResponse platformInfo() {

        var tracVersion = VersionInfo.getComponentVersion(TracMetadataService.class);

        var configInfo = config.getPlatformInfo();
        var environment = configInfo.getEnvironment();
        var production = configInfo.getProduction();    // defaults to false

        // TODO: Validate environment is set during startup
        if (environment.isBlank())
            environment = ENVIRONMENT_NOT_SET;

        return  PlatformInfoResponse.newBuilder()
                .setTracVersion(tracVersion)
                .setEnvironment(environment)
                .setProduction(production)
                .putAllDeploymentInfo(configInfo.getDeploymentInfoMap())
                .build();
    }

    public ListTenantsResponse listTenants() {
        
        var tenantInfoList = dal.listTenants();

        return ListTenantsResponse.newBuilder()
            .addAllTenants(tenantInfoList)
            .build();
    }

    public ListResourcesResponse listResources(String tenantCode, ResourceType resourceType) {

        // Explicit check is required because resources currently come from platform config
        checkTenantExists(tenantCode);

        var response = ListResourcesResponse.newBuilder();

        if (resourceType == ResourceType.MODEL_REPOSITORY) {
            for (var repoEntry : config.getRepositoriesMap().entrySet()) {
                response.addResources(buildResourceInfo(resourceType, repoEntry.getKey(), repoEntry.getValue()));
            }
        }
        else if (resourceType == ResourceType.INTERNAL_STORAGE) {
            for (var storageEntry : config.getStorage().getBucketsMap().entrySet()) {
                response.addResources(buildResourceInfo(resourceType, storageEntry.getKey(), storageEntry.getValue()));
            }
        }
        else {
            var message = String.format("Unknown resource type: ['%s]'", resourceType.name());
            log.error(message);
            throw new EResourceNotFound(message);
        }

        return response.build();
    }

    public ResourceInfoResponse resourceInfo(String tenantCode, ResourceType resourceType, String resourceKey) {

        // Explicit check is required because resources currently come from platform config
        checkTenantExists(tenantCode);

        PluginConfig pluginConfig;

        if (resourceType == ResourceType.MODEL_REPOSITORY) {

            if (!this.config.containsRepositories(resourceKey)){
                var message = String.format("Model repository not found: [%s]", resourceKey);
                log.error(message);
                throw new EResourceNotFound(message);
            }

            pluginConfig = this.config.getRepositoriesOrThrow(resourceKey);
        }
        else if (resourceType == ResourceType.INTERNAL_STORAGE) {

            if (!this.config.getStorage().containsBuckets(resourceKey)) {
                var message = String.format("Storage location not found: [%s]", resourceKey);
                log.error(message);
                throw new EResourceNotFound(message);
            }

            pluginConfig = this.config.getStorage().getBucketsOrThrow(resourceKey);
        }
        else {

            var message = String.format("Unknown resource type: ['%s]'", resourceType.name());
            log.error(message);
            throw new EResourceNotFound(message);
        }

        return buildResourceInfo(resourceType, resourceKey, pluginConfig).build();
    }

    private void checkTenantExists(String tenantCode) {

        var tenants = dal.listTenants();

        var requiredTenant = tenants.stream()
                .filter(tenant -> tenant.getTenantCode().equals(tenantCode))
                .findFirst();

        if (requiredTenant.isEmpty())
            throw new ETenantNotFound("Tenant not found: [" + tenantCode + "]");
    }

    private ResourceInfoResponse.Builder buildResourceInfo(
            ResourceType resourceType, String resourceKey,
            PluginConfig pluginConfig) {

        return ResourceInfoResponse.newBuilder()
                .setResourceType(resourceType)
                .setResourceKey(resourceKey)
                .setProtocol(pluginConfig.getProtocol())
                .putAllProperties(pluginConfig.getPublicPropertiesMap());
    }

    public Tag readObject(String tenant, TagSelector selector) {

        return dal.loadObject(tenant, selector);
    }

    public List<Tag> readObjects(String tenant, List<TagSelector> selectors) {

        return dal.loadObjects(tenant, selectors);
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

        return dal.loadObject(tenant, selector);
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

        return dal.loadObject(tenant, selector);
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

        return dal.loadObject(tenant, selector);
    }
}
