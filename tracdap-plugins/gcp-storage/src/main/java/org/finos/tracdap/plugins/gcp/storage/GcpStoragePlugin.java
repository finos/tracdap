/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.plugins.gcp.storage;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;
import org.finos.tracdap.common.storage.IFileStorage;
import org.finos.tracdap.common.storage.IStorageManager;

import java.util.List;
import java.util.Properties;


public class GcpStoragePlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "GCP_STORAGE";
    private static final String GCS_OBJECT_STORAGE = "GCS_OBJECT_STORAGE";

    private static final List<PluginServiceInfo> serviceInfo = List.of(
            new PluginServiceInfo(IFileStorage.class, GCS_OBJECT_STORAGE, List.of("gcs")));

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return serviceInfo;
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T createService(String service, Properties properties, ConfigManager configManager) {

        if (service.equals(GCS_OBJECT_STORAGE)) {
            var instance = properties.getProperty(IStorageManager.PROP_STORAGE_KEY);
            return (T) new GcsObjectStorage(instance, properties);
        }

        var message = String.format("Plugin [%s] does not support the service [%s]", pluginName(), service);
        throw new EPluginNotAvailable(message);
    }
}
