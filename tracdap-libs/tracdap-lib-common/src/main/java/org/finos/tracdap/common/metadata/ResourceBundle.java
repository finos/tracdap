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

package org.finos.tracdap.common.metadata;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.metadata.ResourceDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ResourceBundle {

    private final Map<String, ResourceDefinition> resources;

    public static ResourceBundle allTenantResources(TenantConfig tenantConfig) {
        return new ResourceBundle(tenantConfig.getResourcesMap());
    }

    public static ResourceBundle filterResources(TenantConfig tenantConfig, List<String> resourceNames) {

        var resources = new HashMap<String, ResourceDefinition>();

        for (var resourceName : resourceNames) {
            if (tenantConfig.containsResources(resourceName))
                resources.put(resourceName, tenantConfig.getResourcesOrThrow(resourceName));
        }

        return new ResourceBundle(resources);
    }

    public ResourceBundle(Map<String, ResourceDefinition> resources) {
        this.resources = resources;
    }

    public boolean hasResource(String resourceName) {
        return resources.containsKey(resourceName);
    }

    public ResourceDefinition getResource(String resourceName) {
        return getResource(resourceName, true);
    }

    public ResourceDefinition getResource(String resourceName, boolean required) {

        var resource = resources.get(resourceName);

        if (resource == null) {
            if (required)
                throw new EUnexpected();
            else
                return null;
        }

        return resource;
    }
}
