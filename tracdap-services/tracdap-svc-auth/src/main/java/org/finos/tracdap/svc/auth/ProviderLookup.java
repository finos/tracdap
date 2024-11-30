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

import org.finos.tracdap.common.auth.internal.JwtSetup;
import org.finos.tracdap.common.auth.login.ILoginProvider;
import org.finos.tracdap.common.auth.login.LoginAuthProvider;
import org.finos.tracdap.common.auth.provider.IAuthProvider;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.IPluginManager;
import org.finos.tracdap.config.ExternalAuthConfig;
import org.finos.tracdap.config.PlatformConfig;

import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class ProviderLookup {

    private final List<IAuthProvider> providers;

    public ProviderLookup(PlatformConfig platformConfig, ConfigManager configManager, IPluginManager pluginManager) {

        providers = createProviders(platformConfig, configManager, pluginManager);
    }

    private List<IAuthProvider> createProviders(PlatformConfig platformConfig, ConfigManager configManager, IPluginManager pluginManager) {

        var providers = new ArrayList<IAuthProvider>();

        var loginProvider = createLoginProvider(platformConfig, configManager, pluginManager);
        providers.add(loginProvider);

        var externalSystems = platformConfig.getAuthentication().getExternalSystemsList();

        for (var externalSystem : externalSystems) {
            var provider = createExternalProvider(externalSystem, configManager, pluginManager);
            providers.add(provider);
        }

        return Collections.unmodifiableList(providers);
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

        return new LoginAuthProvider(authConfig, jwtProcessor, loginProvider);
    }

    private IAuthProvider createExternalProvider(ExternalAuthConfig externalAuthConfig, ConfigManager configManager, IPluginManager pluginManager) {

        var providerConfig = externalAuthConfig.getProvider();
        var protocol = providerConfig.getProtocol();

        if (! pluginManager.isServiceAvailable(IAuthProvider.class, protocol)) {
            var error = String.format("No plugin available for external auth provider [%s]", protocol);
            throw new EStartup(error);
        }

        return  pluginManager.createService(IAuthProvider.class, externalAuthConfig.getProvider(), configManager);
    }

    public ChannelInboundHandler selectAuthProcessor(HttpRequest request) {

        for (var provider : providers) {
            if (provider.canHandleRequest(request))
                return provider.handleRequest(request);
        }


        return null;
    }
}
