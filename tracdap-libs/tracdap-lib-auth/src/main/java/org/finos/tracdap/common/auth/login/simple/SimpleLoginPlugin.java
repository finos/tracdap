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

package org.finos.tracdap.common.auth.login.simple;

import org.finos.tracdap.common.auth.login.ILoginProvider;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;
import org.finos.tracdap.config.PlatformConfig;

import java.util.List;
import java.util.Properties;


public class SimpleLoginPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "SIMPLE_LOGIN";
    private static final String GUEST_LOGIN_PROVIDER = "GUEST_LOGIN_PROVIDER";
    private static final String BASIC_LOGIN_PROVIDER = "BASIC_LOGIN_PROVIDER";
    private static final String BUILT_IN_LOGIN_PROVIDER = "BUILT_IN_LOGIN_PROVIDER";

    private static final List<PluginServiceInfo> serviceInfo = List.of(
            new PluginServiceInfo(ILoginProvider.class, GUEST_LOGIN_PROVIDER, List.of("guest")),
            new PluginServiceInfo(ILoginProvider.class, BASIC_LOGIN_PROVIDER, List.of("basic")),
            new PluginServiceInfo(ILoginProvider.class, BUILT_IN_LOGIN_PROVIDER, List.of("builtin")));

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

        switch (serviceName) {

            case GUEST_LOGIN_PROVIDER:
                return (T) new GuestLoginProvider(properties);

            case BASIC_LOGIN_PROVIDER:
                return (T) new BasicLoginProvider(configManager);

            case BUILT_IN_LOGIN_PROVIDER:
                return (T) new BuiltInLoginProvider(configManager);

            default:
                var message = String.format("Plugin [%s] does not support the service [%s]", pluginName(), serviceName);
                throw new EPluginNotAvailable(message);
        }
    }

    static IUserDatabase createUserDb(ConfigManager configManager) {

        // IUserDatabase is an implementation detail of the simple login provider
        // Not intended for extension / re-use in its current form

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
