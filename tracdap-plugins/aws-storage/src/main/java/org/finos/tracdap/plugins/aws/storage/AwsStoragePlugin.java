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

package org.finos.tracdap.plugins.aws.storage;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;
import org.finos.tracdap.common.storage.IFileStorage;

import java.util.List;
import java.util.Properties;


public class AwsStoragePlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "AWS_STORAGE";
    private static final String S3_OBJECT_STORAGE = "S3_OBJECT_STORAGE";

    private static final List<PluginServiceInfo> serviceInfo = List.of(
            new PluginServiceInfo(IFileStorage.class, S3_OBJECT_STORAGE, List.of("s3")));

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return serviceInfo;
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T createService(String serviceName, Properties properties, ConfigManager configManager) {

        if (serviceName.equals(S3_OBJECT_STORAGE))
            return (T) new S3ObjectStorage(properties);

        var message = String.format("Plugin [%s] does not support the service [%s]", pluginName(), serviceName);
        throw new EPluginNotAvailable(message);
    }
}
