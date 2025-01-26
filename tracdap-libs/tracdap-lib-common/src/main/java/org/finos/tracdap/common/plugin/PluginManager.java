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

    private final List<ITracExtension> extensions;
    private final Map<String, PluginType> pluginTypes;
    private final Map<PluginKey, ITracPlugin> plugins;

    public PluginManager() {

        extensions = new ArrayList<>();
        pluginTypes = new HashMap<>();
        plugins = new HashMap<>();

        for (var corePluginType : PluginType.CORE_PLUGIN_TYPES)
            pluginTypes.put(corePluginType.serviceClassName(), corePluginType);
    }

    public void registerExtensions() {

        StartupLog.log(this, Level.INFO, "Looking for extensions...");

        var availableExtensions = ServiceLoader.load(ITracExtension.class).iterator();

        while (availableExtensions.hasNext()) {

            try {

                var extension = availableExtensions.next();

                StartupLog.log(this, Level.INFO, String.format("Extension: [%s]", extension.extensionName()));

                extensions.add(extension);

                for (var pluginType : extension.pluginTypes())
                    pluginTypes.put(pluginType.serviceClassName(), pluginType);
            }
            catch (ServiceConfigurationError e) {

                StartupLog.log(this, Level.WARN, e.getMessage());
            }
        }
    }

    public List<ITracExtension> getExtensions() {

        return extensions;
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
                        .filter(si -> pluginTypes.containsKey(si.serviceClass().getName()))
                        .filter(si -> pluginTypes.get(si.serviceClass().getName()).isConfigPlugin())
                        .collect(Collectors.toList());

                if (!configServices.isEmpty()) {

                    StartupLog.log(this, Level.INFO, String.format("Plugin: [%s]", plugin.pluginName()));

                    registerServices(plugin, configServices, true);
                }
            }
            catch (ServiceConfigurationError e) {

                StartupLog.log(this, Level.WARN, e.getMessage());
            }
        }
    }

    public void initRegularPlugins() {

        StartupLog.log(this, Level.INFO, "Loading plugins...");

        var availablePlugins = ServiceLoader.load(ITracPlugin.class).iterator();

        while (availablePlugins.hasNext()) {

            try {

                var plugin = availablePlugins.next();

                StartupLog.log(this, Level.INFO, String.format("Plugin: [%s]", plugin.pluginName()));

                registerServices(plugin, plugin.serviceInfo(), false);
            }
            catch (ServiceConfigurationError e) {

                StartupLog.log(this, Level.WARN, e.getMessage());
            }
        }
    }

    private void registerServices(ITracPlugin plugin, List<PluginServiceInfo> services, boolean configPlugins) {

        for (var service : services) {

            var pluginType = pluginTypes.get(service.serviceClass().getName());

            if (pluginType == null) {

                StartupLog.log(this, Level.WARN, String.format(
                        " | UNKNOWN: [%s] (service class not recognized: %s)",
                        service.serviceName(), service.getClass().getName()));

                continue;
            }

            if (pluginType.isConfigPlugin() && !configPlugins)
                continue;

            var serviceType = pluginType.serviceType();
            var prettyServiceType = prettyTypeName(serviceType, true);
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

        var plugin = lookupPlugin(serviceClass, pluginConfig.getProtocol());

        return plugin.createService(serviceClass, pluginConfig, configManager);
    }

    @Override
    public <T> T createService(Class<T> serviceClass, String protocol, ConfigManager configManager) {

        var pluginConfig = PluginConfig.newBuilder()
                .setProtocol(protocol)
                .build();

        return createService(serviceClass, pluginConfig, configManager);
    }

    @Override
    public <T> T createConfigService(Class<T> serviceClass, PluginConfig pluginConfig) {

        var plugin = lookupPlugin(serviceClass, pluginConfig.getProtocol());

        return plugin.createConfigService(serviceClass, pluginConfig);
    }

    @Override
    public <T> T createConfigService(Class<T> serviceClass, String protocol, Properties properties) {

        var pluginConfig = PluginConfig.newBuilder()
                .setProtocol(protocol);

        for (var property: properties.entrySet()) {
            var key = property.getKey().toString();
            var value = property.getValue().toString();
            pluginConfig.putProperties(key, value);
        }

        var plugin = lookupPlugin(serviceClass, protocol);

        return plugin.createConfigService(serviceClass, pluginConfig.build());
    }

    private <T> ITracPlugin lookupPlugin(Class<T> serviceClass, String protocol) {

        var pluginKey = new PluginKey(serviceClass, protocol);

        if (!pluginTypes.containsKey(serviceClass.getName()))
            throw new EUnexpected();

        if (protocol.isBlank()) {

            var pluginType = pluginTypes.get(serviceClass.getName());
            var message = String.format("Protocol not specified for [%s] plugin", pluginType.serviceType());

            StartupLog.log(this, Level.ERROR, message);
            throw new EPluginNotAvailable(message);
        }

        if (!plugins.containsKey(pluginKey)) {

            var rawTypeName = pluginTypes.get(serviceClass.getName()).serviceType();
            var message = String.format(
                    "Plugin not available for %s protocol: [%s]",
                    prettyTypeName(rawTypeName, false), protocol);

            StartupLog.log(this, Level.ERROR, message);
            throw new EPluginNotAvailable(message);
        }

        return plugins.get(pluginKey);
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
