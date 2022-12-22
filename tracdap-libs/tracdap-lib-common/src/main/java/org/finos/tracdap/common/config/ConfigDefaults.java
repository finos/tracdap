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

public class ConfigDefaults {

    // Really we should mark these defaults up in the .proto files for the config
    // Defaults and validation can both be done using protobuf extensions
    // (not sure if they call it something else now)!
    // Also, we could get the same behavior across different coding languages that way...

    // For now, here are some config defaults!

    public static final int DEFAULT_JWT_EXPIRY = 3600;    // 1 hour
    public static final int DEFAULT_JWT_LIMIT = 57600;    // 16 hours
    public static final int DEFAULT_JWT_REFRESH = 60;     // 5 minutes


    public static int readOrDefault(int configValue, int defaultValue) {

        if (configValue == 0)
            return defaultValue;
        else
            return configValue;
    }

}
