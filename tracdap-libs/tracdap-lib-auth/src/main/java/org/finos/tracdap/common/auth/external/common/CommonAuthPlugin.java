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

package org.finos.tracdap.common.auth.external.common;

import org.finos.tracdap.common.auth.external.IUserDatabase;
import org.finos.tracdap.common.auth.external.IAuthProvider;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;
import org.finos.tracdap.config.PlatformConfig;

import java.util.List;
import java.util.Properties;


public class CommonAuthPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "COMMON_AUTH";
    private static final String GUEST_AUTH_PROVIDER = "GUEST_AUTH_PROVIDER";
    private static final String BASIC_AUTH_PROVIDER = "BASIC_AUTH_PROVIDER";
    private static final String BUILT_IN_AUTH_PROVIDER = "BUILT_IN_AUTH_PROVIDER";
    private static final String JKS_USER_DATABASE = "JKS_USER_DATABASE";
    private static final String SQL_USER_DATABASE = "SQL_USER_DATABASE";

    private static final List<PluginServiceInfo> serviceInfo = List.of(
            new PluginServiceInfo(IAuthProvider.class, GUEST_AUTH_PROVIDER, List.of("guest")),
            new PluginServiceInfo(IAuthProvider.class, BASIC_AUTH_PROVIDER, List.of("basic")),
            new PluginServiceInfo(IAuthProvider.class, BUILT_IN_AUTH_PROVIDER, List.of("builtin")),
            new PluginServiceInfo(IUserDatabase.class, JKS_USER_DATABASE, List.of("JKS", "PKCS12")),
            new PluginServiceInfo(IUserDatabase.class, SQL_USER_DATABASE, List.of("H2")));

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

        if (serviceName.equals(GUEST_AUTH_PROVIDER))
            return (T) new GuestAuthProvider(properties);

        if (serviceName.equals(BASIC_AUTH_PROVIDER))
            return (T) new BasicAuthProvider(configManager);

        if (serviceName.equals(BUILT_IN_AUTH_PROVIDER))
            return (T) new BuiltInAuthProvider(properties, configManager);

        var message = String.format("Plugin [%s] does not support the service [%s]", pluginName(), serviceName);
        throw new EPluginNotAvailable(message);
    }

    static IUserDatabase createUserDb(ConfigManager configManager) {

        // TODO: Use plugin manager to get the user DB plugin
        // Requires passing plugin manager into createService for non-config plugins

        var config = configManager.loadRootConfigObject(PlatformConfig.class);
        var secretType = config.getConfigOrDefault("users.type", "PKCS12");

        if ("PKCS12".equals(secretType) || "JKS".equals(secretType))
            return new JksUserDb(configManager);

        if ("H2".equals(secretType)) {
            var jdbcUrl = config.getConfigOrDefault("users.url", "local_users");
            return SqlUserDb.getUserDb(configManager, secretType, jdbcUrl);
        }

        throw new EStartup(String.format("Unsupported secret type [%s]", secretType));
    }
}
