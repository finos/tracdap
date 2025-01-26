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

package org.finos.tracdap.common.plugin;

import javax.annotation.Nonnull;
import java.util.List;


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

    /** Standard service type for generic authentication providers **/
    public static final String AUTH_PROVIDER_SERVICE_TYPE = "AUTH_PROVIDER";

    /** Standard service type for TRAC login providers **/
    public static final String LOGIN_PROVIDER_SERVICE_TYPE = "LOGIN_PROVIDER";

    /** Standard service type for file storage services **/
    public static final String FILE_STORAGE_SERVICE_TYPE = "FILE_STORAGE";

    /** Standard service type for data storage services **/
    public static final String DATA_STORAGE_SERVICE_TYPE = "DATA_STORAGE";

    /** Standard service type for format services (i.e. data codecs) **/
    public static final String FORMAT_SERVICE_TYPE = "FORMAT";

    /** Standard service types for execution services **/
    public static final String EXECUTION_SERVICE_TYPE = "EXECUTOR";

    /** Standard service types for execution services **/
    public static final String JOB_CACHE_SERVICE_TYPE = "JOB_CACHE";

    /** Standard service type for the metadata DAL **/
    public static final String METADATA_DAL_SERVICE_TYPE = "METADATA_DAL";

    private final Class<?> serviceClass;
    private final String serviceName;
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
     * Get the list of protocols supported by this service
     *
     * @return The list of protocols supported by this service
     **/
    public List<String> protocols() {
        return protocols;
    }
}
