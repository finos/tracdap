/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.gateway.builders;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.api.Data;
import org.finos.tracdap.api.Metadata;
import org.finos.tracdap.api.Orchestrator;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.ServiceConfig;

import java.util.ArrayList;
import java.util.List;

public class ServiceInfo {

    // TODO: This info could be coded into the service proto files using a custom proto extension
    // Alternatively, it could be set up as defaults in the service config

    public ServiceInfo(
            Descriptors.ServiceDescriptor descriptor,
            ServiceConfig config,
            String serviceName, String restPrefix) {

        this.descriptor = descriptor;
        this.config = config;

        this.serviceName = serviceName;
        this.restPrefix = restPrefix;
    }

    Descriptors.ServiceDescriptor descriptor;
    ServiceConfig config;
    String serviceName;
    String restPrefix;

    public static List<ServiceInfo> buildServiceInfo(PlatformConfig platformConfig) {

        var metaDescriptor = serviceDescriptor(Metadata.getDescriptor(), "TracMetadataApi");
        var dataDescriptor = serviceDescriptor(Data.getDescriptor(), "TracDataApi");
        var orchDescriptor = serviceDescriptor(Orchestrator.getDescriptor(), "TracOrchestratorApi");

        var metaConfig = platformConfig.getServicesOrThrow(ConfigKeys.METADATA_SERVICE_KEY);
        var dataConfig = platformConfig.getServicesOrThrow(ConfigKeys.DATA_SERVICE_KEY);
        var orchConfig = platformConfig.getServicesOrThrow(ConfigKeys.ORCHESTRATOR_SERVICE_KEY);

        var services = new ArrayList<ServiceInfo>();

        services.add(new ServiceInfo(metaDescriptor, metaConfig, "TRAC Metadata Service", "/trac-meta/api/v1"));
        services.add(new ServiceInfo(dataDescriptor, dataConfig, "TRAC Data Service", "/trac-data/api/v1"));
        services.add(new ServiceInfo(orchDescriptor, orchConfig, "TRAC Orchestrator Service", "/trac-orch/api/v1"));

        return services;
    }

    private static Descriptors.ServiceDescriptor serviceDescriptor(
            Descriptors.FileDescriptor fileDescriptor,
            String serviceName) {

        var serviceDescriptor = fileDescriptor.findServiceByName(serviceName);

        if (serviceDescriptor == null)
            throw new ETracInternal("Service descriptor not found: [" + serviceName + "]");

        return serviceDescriptor;
    }

}
