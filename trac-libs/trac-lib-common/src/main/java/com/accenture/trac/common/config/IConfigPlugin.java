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

package com.accenture.trac.common.config;

import com.accenture.trac.common.exception.*;


/**
 * Interface for implementing config plugins.
 *
 * <p>To implement a config plugin implement this interface and also
 * IConfigLoader. You will also need to add a service entry in the plugin
 * jar so ConfigManager can find the plugin. If there are any errors initializing
 * your plugin, throw an EStartup exception. If you need additional parameters to
 * set up the plugin, such as connection details to find and authenticate with a
 * remote service, these will need to be passed via StandardArgs.</p>
 *
 * <p>For an example of a config plugin, see FilesystemConfigPlugin. For an example
 * of a service entry, look in the resources folder for this library under
 * META-INF/services.</p>
 *
 * @see ConfigManager
 * @see IConfigLoader
 * @see com.accenture.trac.common.config.file.FilesystemConfigPlugin
 */
public interface IConfigPlugin {

    /**
     * Create a config loader instance.
     *
     * @param args Standard command line args
     * @return The newly created config loader instance
     * @throws EStartup There was a problem creating the config loader
     */
    IConfigLoader createConfigLoader(StandardArgs args);
}
