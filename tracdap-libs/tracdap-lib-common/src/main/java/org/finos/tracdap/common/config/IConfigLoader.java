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

package org.finos.tracdap.common.config;

import com.accenture.trac.common.exception.*;
import org.finos.tracdap.common.config.local.LocalConfigLoader;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.TracPlugin;

import java.net.URI;
import java.util.List;


/**
 * Interface for physical loading of config files.
 *
 * <p> Config plugins can supply an IConfigLoader implementation to
 * handle physical loading of config files. The loader can advertise
 * the protocols it supports using the protocols() method and then provide
 * a load method to load files using those protocols. ConfigManager will only
 * call the loader for protocols it supports and will always pass an
 * absolute URL (relative URLs are resolved in ConfigManager). If there
 * are any problems loading a config file, throw an EStartup exception.</p>
 *
 * <p>For an example of a config loader, see LocalConfigLoader.</p>
 *
 * @see ConfigManager
 * @see LocalConfigLoader ;
 * @see TracPlugin
 */
public interface IConfigLoader {

    /**
     * Get the name of this loader (useful for debugging startup failures).
     *
     * @return The name of the loader
     */
    String loaderName();

    /**
     * Get the list of protocols supported by this loader.
     *
     * @return The list of supported protocols
     */
    List<String> protocols();

    /**
     * Use the loader to load a text file.
     *
     * <p>The supplied configUrl will always be an absolute URL using one of the protocols
     * advertised by the protocols() method.</p>
     *
     * @param configUrl URL of the file to load
     * @return The content of the file as text
     * @throws EStartup There was a problem loading the file
     */
    String loadTextFile(URI configUrl);
}
