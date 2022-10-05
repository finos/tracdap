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

package org.finos.tracdap.plugins.config.aws;

import org.finos.tracdap.common.config.IConfigLoader;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;

import java.util.List;
import java.util.Properties;


/**
 * A config loader plugin for loading from the AWS S3.
 *
 * <p>This plugin requires AWS credentials and the desired region to be set up in the environment.
 * <a href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html">
 * https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html</a></p>
 */
public class AwsConfigPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "AWS_CONFIG";
    private static final String SERVICE_NAME = "AWS_S3_CONFIG";

    private static final List<PluginServiceInfo> serviceInfo = List.of(
            new PluginServiceInfo(IConfigLoader.class, SERVICE_NAME, List.of("s3")));

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return serviceInfo;
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T createService(String serviceName, Properties properties) {

        if (serviceName.equals(SERVICE_NAME))
            return (T) new AwsConfigLoader();

        throw new EUnexpected();
    }
}
