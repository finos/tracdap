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
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.config.TenantConfigMap;
import org.finos.tracdap.metadata.ResourceDefinition;

import java.util.Map;
import java.util.Properties;


public class ConfigHelpers {

    private static final String BOOLEAN_TRUE = Boolean.TRUE.toString();
    private static final String BOOLEAN_FALSE = Boolean.FALSE.toString();

    public static TenantConfigMap loadTenantConfigMap(ConfigManager configManager) {

        var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
        return loadTenantConfigMap(configManager, platformConfig);
    }

    public static TenantConfigMap loadTenantConfigMap(ConfigManager configManager, PlatformConfig platformConfig) {

        if (!platformConfig.containsConfig(ConfigKeys.TENANTS_CONFIG_KEY))
            return TenantConfigMap.getDefaultInstance();

        var tenantConfigFile = platformConfig.getConfigOrThrow(ConfigKeys.TENANTS_CONFIG_KEY);
        return configManager.loadConfigObject(tenantConfigFile, TenantConfigMap.class);
    }

    public static PluginConfig resourceToPluginConfig(ResourceDefinition resource) {

        return PluginConfig.newBuilder()
                .setProtocol(resource.getProtocol())
                .putAllProperties(resource.getPublicPropertiesMap())
                .putAllProperties(resource.getPropertiesMap())
                .putAllSecrets(resource.getSecretsMap())
                .build();
    }

    public static String readString(String context, Map<String, String> propertiesMap, String key) {
        return readString(context, propertiesMap, key, true);
    }

    public static String readString(String context, Map<String, String> propertiesMap, String key, boolean required) {

        var rawValue = propertiesMap.get(key);

        if (rawValue == null || rawValue.isEmpty()) {
            if (required)
                throw new EStartup(String.format("Missing required property [%s] for [%s]", key, context));
            else
                return null;
        }

        return rawValue.trim();
    }

    public static String readStringOrDefault(String context, Map<String, String> propertiesMap, String key, String defaultValue) {
        var configValue = readString(context, propertiesMap, key, false);
        if (configValue == null || configValue.isEmpty())
            return defaultValue;
        else
            return configValue;
    }

    public static String readString(String context, Properties properties, String key) {
        return readString(context, properties, key, true);
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

    public static String readStringOrDefault(String context, Properties properties, String key, String defaultValue) {
        var configValue = readString(context, properties, key, false);
        if (configValue == null || configValue.isEmpty())
            return defaultValue;
        else
            return configValue;
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

    public static String readOrDefault(String configValue, String defaultValue) {

        if (configValue == null || configValue.isBlank())
            return defaultValue;
        else
            return configValue;
    }

    public static int readOrDefault(int configValue, int defaultValue) {

        if (configValue == 0)
            return defaultValue;
        else
            return configValue;
    }

    public static Properties buildSecretProperties(Map<String, String> configMap, String cliSecretKey) {

        var env = System.getenv();

        var secretType = env.containsKey(ConfigKeys.TRAC_SECRET_TYPE)
                ? env.get(ConfigKeys.TRAC_SECRET_TYPE)
                : configMap.getOrDefault(ConfigKeys.SECRET_TYPE_KEY, null);

        var secretUrl = env.containsKey(ConfigKeys.TRAC_SECRET_URL)
                ? env.get(ConfigKeys.TRAC_SECRET_URL)
                : configMap.getOrDefault(ConfigKeys.SECRET_URL_KEY, null);

        var secretKey = env.getOrDefault(ConfigKeys.TRAC_SECRET_KEY, cliSecretKey);

        var secretProps = new Properties();

        if (secretType != null)
            secretProps.setProperty(ConfigKeys.SECRET_TYPE_KEY, secretType);
        if (secretUrl != null)
            secretProps.setProperty(ConfigKeys.SECRET_URL_KEY, secretUrl);
        if (secretKey != null)
            secretProps.setProperty(ConfigKeys.SECRET_KEY_KEY, secretKey);

        return secretProps;
    }

    public static String getSecretType(Properties secretProps) {

        return secretProps.getProperty(ConfigKeys.SECRET_TYPE_KEY, null);
    }

    public static String getSecretUrl(Properties secretProps) {

        return secretProps.getProperty(ConfigKeys.SECRET_URL_KEY, null);
    }
}
