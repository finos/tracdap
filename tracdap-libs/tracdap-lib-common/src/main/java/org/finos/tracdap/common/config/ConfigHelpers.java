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

package org.finos.tracdap.common.config;

import org.finos.tracdap.common.exception.EStartup;

import java.util.Map;
import java.util.Properties;


public class ConfigHelpers {

    private static final String BOOLEAN_TRUE = Boolean.TRUE.toString();
    private static final String BOOLEAN_FALSE = Boolean.FALSE.toString();

    public static String readString(String context, Properties properties, String key) {
        return readString(context, properties, key, true);
    }

    public static String readString(String context, Map<String, String> propertiesMap, String key) {
        var properties = new Properties();
        properties.putAll(propertiesMap);
        return readString(context, properties, key, true);
    }

    public static String readStringOrDefault(String context, Properties properties, String key, String defaultValue) {
        var configValue = readString(context, properties, key, false);
        if (configValue == null || configValue.isEmpty())
            return defaultValue;
        else
            return configValue;
    }

    public static String readString(String context, Properties properties, String key, boolean required) {

        var rawValue = properties.getProperty(key);

        if (rawValue == null || rawValue.isEmpty()) {
            if (required)
                throw new EStartup(String.format("Missing required property [%s] for [%s]", key, context));
            else
                return null;
        }

        return rawValue.trim();
    }

    public static int readInt(String context, Properties properties, String key, int defaultValue) {

        return readInt(context, properties, key, defaultValue, false);
    }

    private static Integer readInt(String context, Properties properties, String key, Integer defaultValue, boolean required) {

        var rawValue = properties.getProperty(key);

        if (rawValue == null || rawValue.isBlank()) {
            if (required)
                throw new EStartup(String.format("Missing required property [%s] for [%s]", key, context));
            else
                return defaultValue;
        }

        try {
            return Integer.parseInt(rawValue);
        }
        catch (NumberFormatException e) {
            throw new EStartup(String.format("Invalid property [%s] for [%s]: Not an integer", key, context));
        }
    }

    public static boolean optionalBoolean(String context, Properties properties, String key, boolean defaultValue) {

        var rawValue = properties.getProperty(key);

        if (rawValue == null || rawValue.isBlank())
            return defaultValue;

        return checkBoolean(context, key, rawValue);
    }

    private static boolean checkBoolean(String context, String key, String rawValue) {

        if (rawValue == null || rawValue.isBlank()) {
            var message = String.format("Missing required property [%s] for [%s]", key, context);
            throw new EStartup(message);
        }

        if (rawValue.equalsIgnoreCase(BOOLEAN_TRUE))
            return true;

        if (rawValue.equalsIgnoreCase(BOOLEAN_FALSE))
            return false;

        var message = String.format("Invalid boolean value for property [%s] in [%s]", key, context);
        throw new EStartup(message);
    }
}
