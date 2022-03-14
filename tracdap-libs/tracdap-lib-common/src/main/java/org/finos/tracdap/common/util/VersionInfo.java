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

package org.finos.tracdap.common.util;


import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETracInternal;

import java.io.IOException;
import java.util.Properties;

public class VersionInfo {

    private static final String VERSION_INFO_PROPS = "version.properties";
    private static final String COMPONENT_NAME_KEY = "trac.component.name";
    private static final String COMPONENT_VERSION_KEY = "trac.component.version";
    private static final String UNPACKED_SUFFIX = " (Unpackaged)";

    public static String getComponentName(Class<?> component) {

        try {
            var packedName = component.getPackage().getImplementationTitle();

            if (packedName != null && !packedName.isBlank())
                return packedName;
            else
                return readVersionInfo(component, COMPONENT_NAME_KEY);
        }
        catch (IOException e) {
            throw new EStartup("Error reading component version info", e);
        }
    }

    public static String getComponentVersion(Class<?> component) {

        try {
            var packedVersion = component.getPackage().getImplementationVersion();

            if (packedVersion != null && !packedVersion.isBlank())
                return packedVersion;
            else
                return readVersionInfo(component, COMPONENT_VERSION_KEY) + UNPACKED_SUFFIX;
        }
        catch (IOException e) {
            throw new EStartup("Error reading component version info", e);
        }
    }

    private static String readVersionInfo(Class<?> component, String propKey) throws IOException {

        var versionInfoStream = component.getClassLoader().getResourceAsStream(VERSION_INFO_PROPS);

        if (versionInfoStream == null)
            throw new ETracInternal("Component version info not available");

        var versionInfo = new Properties();
        versionInfo.load(versionInfoStream);

        var unpackedVersion = versionInfo.getProperty(propKey);

        if (unpackedVersion != null && !unpackedVersion.isBlank())
            return unpackedVersion;

        throw new ETracInternal("Component version info not available");
    }
}
