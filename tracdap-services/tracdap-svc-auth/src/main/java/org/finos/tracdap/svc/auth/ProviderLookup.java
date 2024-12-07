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

package org.finos.tracdap.svc.auth;

import org.finos.tracdap.auth.login.ILoginProvider;
import org.finos.tracdap.auth.login.LoginAuthProvider;
import org.finos.tracdap.auth.provider.IAuthProvider;
import org.finos.tracdap.common.auth.internal.JwtSetup;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.plugin.IPluginManager;
import org.finos.tracdap.config.PlatformConfig;

import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import org.finos.tracdap.config.PluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class ProviderLookup {

    private static final String LOGIN_PROVIDER_KEY = "login";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IAuthProvider loginProvider;
    private final Map<String, IAuthProvider> externalProviders;

    public ProviderLookup(PlatformConfig platformConfig, ConfigManager configManager, IPluginManager pluginManager) {

        loginProvider = createLoginProvider(platformConfig, configManager, pluginManager);
        externalProviders = createProviders(platformConfig, configManager, pluginManager);
    }

    private IAuthProvider createLoginProvider(PlatformConfig platformConfig, ConfigManager configManager, IPluginManager pluginManager) {

        var authConfig = platformConfig.getAuthentication();
        var jwtProcessor = JwtSetup.createProcessor(platformConfig, configManager);

        var loginConfig = authConfig.getProvider();
        var loginProtocol = loginConfig.getProtocol();

        if (! pluginManager.isServiceAvailable(ILoginProvider.class, loginProtocol)) {
            var error = String.format("No plugin available for login provider [%s]", loginProtocol);
            throw new EStartup(error);
        }

        var loginProvider = pluginManager.createService(ILoginProvider.class, loginConfig, configManager);
        var autoProvider = new LoginAuthProvider(authConfig, jwtProcessor, loginProvider);

        logLoginProvider(loginProtocol);
        logAuthProvider(LOGIN_PROVIDER_KEY, LOGIN_PROVIDER_KEY);

        return autoProvider;
    }

    private Map<String, IAuthProvider> createProviders(PlatformConfig platformConfig, ConfigManager configManager, IPluginManager pluginManager) {

        var providers = new HashMap<String, IAuthProvider>();

        var externalSystems = platformConfig.getAuthentication().getExternalSystemsMap();

        for (var externalSystem : externalSystems.entrySet()) {

            var providerConfig = externalSystem.getValue();
            var provider = createExternalProvider(providerConfig, configManager, pluginManager);

            logAuthProvider(externalSystem.getKey(), providerConfig.getProtocol());

            providers.put(externalSystem.getKey(), provider);
        }

        return Collections.unmodifiableMap(providers);
    }

    private IAuthProvider createExternalProvider(PluginConfig providerConfig, ConfigManager configManager, IPluginManager pluginManager) {

        var protocol = providerConfig.getProtocol();

        if (! pluginManager.isServiceAvailable(IAuthProvider.class, protocol)) {
            var error = String.format("No plugin available for external auth provider [%s]", protocol);
            throw new EStartup(error);
        }

        return  pluginManager.createService(IAuthProvider.class, providerConfig, configManager);
    }

    public String findProvider(HttpRequest request) {

        if (loginProvider.canHandleHttp1(request))
            return LOGIN_PROVIDER_KEY;

        for (var provider : externalProviders.entrySet()) {
            if (provider.getValue().canHandleHttp1(request))
                return provider.getKey();
        }

        return null;
    }

    public ChannelInboundHandler createProvider(String providerName) {

        if (LOGIN_PROVIDER_KEY.equals(providerName))
            return loginProvider.createHttp1Handler();

        var provider = externalProviders.get(providerName);

        if (provider == null)
            throw new EUnexpected();

        return provider.createHttp1Handler();
    }

    private void logLoginProvider(String protocol) {
        log.info("Loaded login provider: protocol = {}", protocol);
    }

    private void logAuthProvider(String key, String protocol) {
        log.info("Loaded auth provider: key = {}, protocol = {}", key, protocol);
    }
}
