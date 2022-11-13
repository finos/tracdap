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

package org.finos.tracdap.common.plugin;

import org.finos.tracdap.common.exception.ETracInternal;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;


/**
 * Describes a service provided by a TRAC plugin, via ITracPlugin.
 *
 * @see ITracPlugin
 */
public class PluginServiceInfo {

    /** Standard service type for config services **/
    public static final String CONFIG_SERVICE_TYPE = "CONFIG";

    /** Standard service type for secret-loading services **/
    public static final String SECRETS_SERVICE_TYPE = "SECRETS";

    /** Standard service type for secret-loading services **/
    public static final String AUTH_SERVICE_TYPE = "AUTH";

    /** Standard service type for file storage services **/
    public static final String FILE_STORAGE_SERVICE_TYPE = "FILE_STORAGE";

    /** Standard service type for data storage services **/
    public static final String DATA_STORAGE_SERVICE_TYPE = "DATA_STORAGE";

    /** Standard service type for format services (i.e. data codecs) **/
    public static final String FORMAT_SERVICE_TYPE = "FORMAT";

    /** Standard service types for execution services **/
    public static final String EXECUTION_SERVICE_TYPE = "EXECUTION";

    /**
     * Mapping of known service interfaces to service types.
     *
     * <p>Only service interfaces included in this mapping can be loaded
     * using the TRAC plugin mechanism</p>
     **/
    static final Map<String, String> SERVICE_TYPES = Map.ofEntries(
            Map.entry("org.finos.tracdap.common.config.IConfigLoader", CONFIG_SERVICE_TYPE),
            Map.entry("org.finos.tracdap.common.config.ISecretLoader", SECRETS_SERVICE_TYPE),
            Map.entry("org.finos.tracdap.common.codec.ICodec", FORMAT_SERVICE_TYPE),
            Map.entry("org.finos.tracdap.common.storage.IFileStorage", FILE_STORAGE_SERVICE_TYPE),
            Map.entry("org.finos.tracdap.common.storage.IDataStorage", DATA_STORAGE_SERVICE_TYPE),
            Map.entry("org.finos.tracdap.common.exec.IBatchExecutor", EXECUTION_SERVICE_TYPE),
            Map.entry("org.finos.tracdap.gateway.auth.IAuthProvider", AUTH_SERVICE_TYPE));

    private final Class<?> serviceClass;
    private final String serviceName;
    private final String serviceType;
    private final List<String> protocols;

    /**
     * Create a new service info object
     *
     * @param serviceClass The service class interface for this service
     * @param serviceName The service name for this service
     * @param protocols The list of protocols supported by this service
     */
    public PluginServiceInfo(
            @Nonnull Class<?> serviceClass,
            @Nonnull String serviceName,
            @Nonnull List<String> protocols) {

        this.serviceClass = serviceClass;
        this.serviceName = serviceName;
        this.protocols = protocols;

        this.serviceType = SERVICE_TYPES.getOrDefault(serviceClass.getName(), null);

        if (this.serviceType == null)
            throw new ETracInternal("Service class is not a recognized pluggable service class");
    }

    /**
     * Get the service class interface for this service
     *
     * @return The service class interface for this service
     **/
    public Class<?> serviceClass() {
        return serviceClass;
    }

    /**
     * Get the service name for this service
     *
     * @return The service name for this service
     **/
    public String serviceName() {
        return serviceName;
    }

    /**
     * Get the standard service type for this service
     *
     * @return The standard service type for this service
     **/
    public String serviceType() {
        return serviceType;
    }

    /**
     * Get the list of protocols supported by this service
     *
     * @return The list of protocols supported by this service
     **/
    public List<String> protocols() {
        return protocols;
    }
}
