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
import org.finos.tracdap.common.config.IDynamicResources;
import org.finos.tracdap.common.exception.ETenantNotFound;
import org.finos.tracdap.common.util.VersionInfo;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.dal.IMetadataDal;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;


public class MetadataReadService {

    private static final String ENVIRONMENT_NOT_SET = "ENVIRONMENT_NOT_SET";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IMetadataDal dal;
    private final PlatformConfig platformConfig;
    private final IDynamicResources resources;

    public MetadataReadService(IMetadataDal dal, PlatformConfig platformConfig, IDynamicResources resources) {
        this.dal = dal;
        this.platformConfig = platformConfig;
        this.resources = resources;
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
        
        var tenantInfoList = dal.listTenants();

        return ListTenantsResponse.newBuilder()
            .addAllTenants(tenantInfoList)
            .build();
    }

    public ListResourcesResponse listResources(String tenantCode, ResourceType resourceType) {

        // Explicit check is required because resources currently come from platform config
        checkTenantExists(tenantCode);

        var matchingResources = resources.getMatchingEntries(resource -> resource.getResourceType() == resourceType);

        var response = ListResourcesResponse.newBuilder();
        matchingResources.forEach((key, value) -> response.addResources(buildResourceInfo(key, value)));

        return response.build();
    }

    public ResourceInfoResponse resourceInfo(String tenantCode, ResourceType resourceType, String resourceKey) {

        // Explicit check is required because resources currently come from platform config
        checkTenantExists(tenantCode);

        var resource = resources.getStrictEntry(resourceKey, resourceType);

        return buildResourceInfo(resourceKey, resource).build();
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
            String resourceKey, ResourceDefinition resource) {

        return ResourceInfoResponse.newBuilder()
                .setResourceType(resource.getResourceType())
                .setResourceKey(resourceKey)
                .setProtocol(resource.getProtocol())
                .putAllProperties(resource.getPublicPropertiesMap());
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
