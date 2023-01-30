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

package org.finos.tracdap.common.plugin;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.startup.StartupLog;

import org.finos.tracdap.config.PluginConfig;
import org.slf4j.event.Level;

import java.util.*;
import java.util.stream.Collectors;


public class PluginManager implements IPluginManager {

    private static final List<String> CONFIG_SERVICE_TYPES = List.of(
            PluginServiceInfo.CONFIG_SERVICE_TYPE,
            PluginServiceInfo.SECRETS_SERVICE_TYPE);

    private final Map<PluginKey, ITracPlugin> plugins;

    public PluginManager() {
        plugins = new HashMap<>();
    }

    public void initConfigPlugins() {

        StartupLog.log(this, Level.INFO, "Loading config plugins...");

        var availablePlugins = ServiceLoader.load(ITracPlugin.class).iterator();

        while (availablePlugins.hasNext()) {

            try {

                // Handling to allow for plugins that fail to load due to missing dependencies
                // This happens in the sandbox, e.g. if -svc-meta detects a plugin from -svc-data
                // We could log warnings here, noisy in normal use but helpful if plugins are failing to load
                // Perhaps add a switch to turn on extra logging for plugin load?

                var plugin = availablePlugins.next();
                var services = plugin.serviceInfo();

                var configServices = services.stream()
                        .filter(si -> CONFIG_SERVICE_TYPES.contains(si.serviceType()))
                        .collect(Collectors.toList());

                if (!configServices.isEmpty()) {

                    StartupLog.log(this, Level.INFO, String.format("Plugin: [%s]", plugin.pluginName()));

                    registerServices(plugin, configServices);
                }
            }
            catch (ServiceConfigurationError e) {
                // continue
            }
        }
    }

    public void initRegularPlugins() {

        StartupLog.log(this, Level.INFO, "Loading plugins...");

        var availablePlugins = ServiceLoader.load(ITracPlugin.class).iterator();

        while (availablePlugins.hasNext()) {

            try {

                var plugin = availablePlugins.next();
                var services = plugin.serviceInfo();

                var regularServices = services.stream()
                        .filter(si -> !CONFIG_SERVICE_TYPES.contains(si.serviceType()))
                        .collect(Collectors.toList());

                if (!regularServices.isEmpty()) {

                    StartupLog.log(this, Level.INFO, String.format("Plugin: [%s]", plugin.pluginName()));

                    registerServices(plugin, regularServices);
                }
            }
            catch (ServiceConfigurationError e) {

                StartupLog.log(this, Level.WARN, e.getMessage());
            }
        }
    }

    private void registerServices(ITracPlugin plugin, List<PluginServiceInfo> services) {

        for (var service : services) {

            var prettyServiceType = prettyTypeName(service.serviceType(), true);
            var protocols = String.join(", ", service.protocols());


            StartupLog.log(this, Level.INFO, String.format(
                    " |-> %s: [%s] (protocols: %s)",
                    prettyServiceType, service.serviceName(), protocols));

            for (var protocol : service.protocols()) {

                var pluginKey = new PluginKey(service.serviceClass(), protocol);
                plugins.put(pluginKey, plugin);
            }
        }
    }

    @Override
    public List<String> availableProtocols(Class<?> serviceClass) {

        var protocols = new HashSet<String>();

        for (var pluginKey : plugins.keySet()) {
            if (pluginKey.serviceClass() == serviceClass)
                protocols.add(pluginKey.protocol());
        }

        return new ArrayList<>(protocols);
    }

    @Override
    public boolean isServiceAvailable(Class<?> serviceClass, String protocol) {

        var pluginKey = new PluginKey(serviceClass, protocol);
        return plugins.containsKey(pluginKey);
    }

    @Override
    public <T> T createService(Class<T> serviceClass, PluginConfig pluginConfig, ConfigManager configManager) {

        var protocol = pluginConfig.getProtocol();
        var properties = new Properties();
        properties.putAll(pluginConfig.getPropertiesMap());

        for (var secret : pluginConfig.getSecretsMap().entrySet()) {
            var secretKey = secret.getKey();
            var secretValue = configManager.loadPassword(secret.getValue());
            properties.put(secretKey, secretValue);
        }

        return createService(serviceClass, protocol, properties);
    }

    @Override
    public <T> T createService(Class<T> serviceClass, String protocol) {

        return createService(serviceClass, protocol, new Properties());
    }

    @Override
    public <T> T createService(Class<T> serviceClass, String protocol, Properties properties) {

        var pluginKey = new PluginKey(serviceClass, protocol);

        if (!PluginServiceInfo.SERVICE_TYPES.containsKey(serviceClass.getName()))
            throw new EUnexpected();

        if (protocol.isBlank()) {

            var serviceType = PluginServiceInfo.SERVICE_TYPES.get(serviceClass.getName());
            var message = String.format("Protocol not specified for [%s] plugin", serviceType);

            StartupLog.log(this, Level.ERROR, message);
            throw new EPluginNotAvailable(message);
        }

        if (!plugins.containsKey(pluginKey)) {

            var rawTypeName = PluginServiceInfo.SERVICE_TYPES.get(serviceClass.getName());
            var message = String.format(
                    "Plugin not available for %s protocol: [%s]",
                    prettyTypeName(rawTypeName, false), protocol);

            StartupLog.log(this, Level.ERROR, message);
            throw new EPluginNotAvailable(message);
        }

        var plugin = plugins.get(pluginKey);

        return plugin.createService(serviceClass, protocol, properties);
    }

    private String prettyTypeName(String rawTypeName, boolean caps) {

        var caseAndSpaces = rawTypeName
                .toLowerCase()
                .replace("_", " ")
                .replace("-", " ");

        if (caps) {
            var firstLetter = caseAndSpaces.substring(0, 1).toUpperCase();
            return firstLetter + caseAndSpaces.substring(1);
        }
        else
            return caseAndSpaces;
    }
}
