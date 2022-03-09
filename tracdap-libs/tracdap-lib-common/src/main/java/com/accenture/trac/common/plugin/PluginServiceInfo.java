/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.plugin;

import com.accenture.trac.common.exception.ETracInternal;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;


public class PluginServiceInfo {

    static final Map<String, String> SERVICE_NAMES = Map.ofEntries(
            Map.entry("com.accenture.trac.common.config.IConfigLoader", "CONFIG"),
            Map.entry("com.accenture.trac.common.codec.ICodec", "FORMAT"),
            Map.entry("com.accenture.trac.common.storage.IFileStorage", "FILE_STORAGE"),
            Map.entry("com.accenture.trac.common.storage.IDataStorage", "DATA_STORAGE"),
            Map.entry("com.accenture.trac.common.exec.IBatchExecutor", "BATCH_EXECUTOR"));

    private final String pluginName;
    private final Class<?> serviceClass;
    private final String serviceTypeName;
    private final String serviceName;
    private final List<String> protocols;

    public PluginServiceInfo(
            @Nonnull String pluginName,
            @Nonnull Class<?> serviceClass,
            @Nonnull String serviceName,
            @Nonnull List<String> protocols) {

        this.pluginName = pluginName;
        this.serviceClass = serviceClass;
        this.serviceName = serviceName;
        this.protocols = protocols;

        this.serviceTypeName = SERVICE_NAMES.getOrDefault(serviceClass.getName(), null);

        if (this.serviceTypeName == null)
            throw new ETracInternal("Service class is not a recognized pluggable service class");
    }

    public String pluginName() {
        return pluginName;
    }

    public Class<?> serviceClass() {
        return serviceClass;
    }

    public String serviceTypeName() {
        return serviceTypeName;
    }

    public String serviceName() {
        return serviceName;
    }

    public List<String> protocols() {
        return protocols;
    }
}
