/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.plugins.config.aws;

import com.accenture.trac.common.config.IConfigLoader;
import com.accenture.trac.common.config.IConfigPlugin;
import com.accenture.trac.common.config.StandardArgs;


/**
 * A config loader plugin for loading from the AWS S3.
 *
 * This plugin requires AWS credentials and the desired region to be set up in the environment.
 * https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
 */
public class AwsConfigPlugin implements IConfigPlugin {

    @Override
    public IConfigLoader createConfigLoader(StandardArgs args) {
        return new AwsConfigLoader();
    }
}
