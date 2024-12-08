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

package org.finos.tracdap.gateway.builders;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.api.Data;
import org.finos.tracdap.api.Metadata;
import org.finos.tracdap.api.Orchestrator;
import org.finos.tracdap.common.config.ConfigDefaults;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ServiceProperties;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.ServiceConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ServiceInfo {

    public static final Map<String, String> SERVICE_NAMES = Map.ofEntries(
            Map.entry(ConfigKeys.AUTHENTICATION_SERVICE_KEY, "TRAC Authentication Service"),
            Map.entry(ConfigKeys.METADATA_SERVICE_KEY, "TRAC Metadata Service"),
            Map.entry(ConfigKeys.DATA_SERVICE_KEY, "TRAC Data Service"),
            Map.entry(ConfigKeys.ORCHESTRATOR_SERVICE_KEY, "TRAC Orchestrator Service"),
            Map.entry(ConfigKeys.WEB_SERVER_SERVICE_KEY, "TRAC Web Server"));

    public static final Map<String, String> SERVICE_PREFIX_DEFAULTS = Map.ofEntries(
            Map.entry(ConfigKeys.AUTHENTICATION_SERVICE_KEY, "/trac-auth/"),
            Map.entry(ConfigKeys.METADATA_SERVICE_KEY, "/trac-meta/"),
            Map.entry(ConfigKeys.DATA_SERVICE_KEY, "/trac-data/"),
            Map.entry(ConfigKeys.ORCHESTRATOR_SERVICE_KEY, "/trac-orch/"),
            Map.entry(ConfigKeys.WEB_SERVER_SERVICE_KEY, "/trac-web/"));

    private static final String API_V1_PREFIX = "api/v1/";

    final String serviceKey;
    final String serviceName;
    final ServiceConfig config;
    final Descriptors.ServiceDescriptor descriptor;

    final String httpPrefix;
    final String restPrefix;

    public static List<ServiceInfo> buildServiceInfo(PlatformConfig platformConfig) {

        var defaultServiceConfig = ServiceConfig.newBuilder().setEnabled(false).build();

        var authConfig = platformConfig.getServicesOrDefault(ConfigKeys.AUTHENTICATION_SERVICE_KEY, defaultServiceConfig);
        var metaConfig = platformConfig.getServicesOrDefault(ConfigKeys.METADATA_SERVICE_KEY, defaultServiceConfig);
        var dataConfig = platformConfig.getServicesOrDefault(ConfigKeys.DATA_SERVICE_KEY, defaultServiceConfig);
        var orchConfig = platformConfig.getServicesOrDefault(ConfigKeys.ORCHESTRATOR_SERVICE_KEY, defaultServiceConfig);
        var webConfig = platformConfig.getServicesOrDefault(ConfigKeys.WEB_SERVER_SERVICE_KEY, defaultServiceConfig);

        var metaDescriptor = serviceDescriptor(Metadata.getDescriptor(), "TracMetadataApi");
        var dataDescriptor = serviceDescriptor(Data.getDescriptor(), "TracDataApi");
        var orchDescriptor = serviceDescriptor(Orchestrator.getDescriptor(), "TracOrchestratorApi");

        var services = new ArrayList<ServiceInfo>();

        if (isEnabled(authConfig))
            services.add(new ServiceInfo(ConfigKeys.AUTHENTICATION_SERVICE_KEY, authConfig));

        if (isEnabled(metaConfig))
            services.add(new ServiceInfo(ConfigKeys.METADATA_SERVICE_KEY, metaConfig, metaDescriptor, API_V1_PREFIX));

        if (isEnabled(dataConfig))
            services.add(new ServiceInfo(ConfigKeys.DATA_SERVICE_KEY, dataConfig, dataDescriptor, API_V1_PREFIX));

        if (isEnabled(orchConfig))
            services.add(new ServiceInfo(ConfigKeys.ORCHESTRATOR_SERVICE_KEY, orchConfig, orchDescriptor, API_V1_PREFIX));

        if (isEnabled(webConfig))
            services.add(new ServiceInfo(ConfigKeys.WEB_SERVER_SERVICE_KEY, webConfig));

        return services;
    }

    private static boolean isEnabled(ServiceConfig serviceConfig) {

        return serviceConfig.getEnabled() || !serviceConfig.hasEnabled();
    }

    private static Descriptors.ServiceDescriptor serviceDescriptor(
            Descriptors.FileDescriptor fileDescriptor,
            String serviceName) {

        var serviceDescriptor = fileDescriptor.findServiceByName(serviceName);

        if (serviceDescriptor == null)
            throw new ETracInternal("Service descriptor not found: [" + serviceName + "]");

        return serviceDescriptor;
    }

    private ServiceInfo(String serviceKey, ServiceConfig config) {

        this.serviceKey = serviceKey;
        this.serviceName = SERVICE_NAMES.get(serviceKey);
        this.config = config;
        this.descriptor = null;

        var serviceProps = new Properties();
        serviceProps.putAll(config.getPropertiesMap());

        this.httpPrefix = ConfigDefaults.readOrDefault(
                serviceProps.getProperty(ServiceProperties.GATEWAY_HTTP_PREFIX),
                SERVICE_PREFIX_DEFAULTS.get(serviceKey));

        this.restPrefix = null;
    }

    private ServiceInfo(
            String serviceKey, ServiceConfig config,
            Descriptors.ServiceDescriptor descriptor, String restPrefix) {

        this.serviceKey = serviceKey;
        this.serviceName = SERVICE_NAMES.get(serviceKey);
        this.config = config;
        this.descriptor = descriptor;

        var serviceProps = new Properties();
        serviceProps.putAll(config.getPropertiesMap());

        var httpPrefix = ConfigDefaults.readOrDefault(
                serviceProps.getProperty(ServiceProperties.GATEWAY_HTTP_PREFIX),
                SERVICE_PREFIX_DEFAULTS.get(serviceKey));

        this.httpPrefix = null;
        this.restPrefix = httpPrefix + restPrefix;
    }

    public boolean hasGrpc() {
        return descriptor != null;
    }

    public boolean hasRest() {
        return restPrefix != null;
    }

    public boolean hasHttp() {
        return httpPrefix != null;
    }
}
