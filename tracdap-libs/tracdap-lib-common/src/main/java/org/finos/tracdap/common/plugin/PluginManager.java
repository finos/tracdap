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

import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.exception.EUnexpected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class PluginManager implements IPluginManager {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<PluginKey, ITracPlugin> plugins;

    public PluginManager() {
        plugins = new HashMap<>();
    }

    public void initConfigPlugins() {

        log.info("Loading config plugins...");

        var availablePlugins = ServiceLoader.load(ITracPlugin.class);

        for (var plugin: availablePlugins) {

            var services = plugin.serviceInfo();

            var configServices = services.stream()
                    .filter(si -> si.serviceType().equals(PluginServiceInfo.CONFIG_SERVICE_TYPE))
                    .collect(Collectors.toList());

            if (!configServices.isEmpty()) {

                log.info("Plugin: [{}]", plugin.pluginName());

                registerServices(plugin, configServices);
            }
        }
    }

    public void initRegularPlugins() {

        log.info("Loading plugins...");

        var availablePlugins = ServiceLoader.load(ITracPlugin.class);

        for (var plugin: availablePlugins) {

            var services = plugin.serviceInfo();

            var regularServices = services.stream()
                    .filter(si -> ! si.serviceType().equals(PluginServiceInfo.CONFIG_SERVICE_TYPE))
                    .collect(Collectors.toList());

            if (!regularServices.isEmpty()) {

                log.info("Plugin: [{}]", plugin.pluginName());

                registerServices(plugin, regularServices);
            }
        }
    }

    private void registerServices(ITracPlugin plugin, List<PluginServiceInfo> services) {

        for (var service : services) {

            var prettyServiceType = prettyTypeName(service.serviceType(), true);
            var protocols = String.join(", ", service.protocols());

            log.info(" |-> {}: [{}] (protocols: {})",
                    prettyServiceType,
                    service.serviceName(),
                    protocols);

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
    public <T> T createService(Class<T> serviceClass, String protocol) {

        return createService(serviceClass, protocol, new Properties());
    }

    @Override
    public <T> T createService(Class<T> serviceClass, String protocol, Properties properties) {

        var pluginKey = new PluginKey(serviceClass, protocol);

        if (!PluginServiceInfo.SERVICE_TYPES.containsKey(serviceClass.getName()))
            throw new EUnexpected();

        if (!plugins.containsKey(pluginKey)) {

            var rawTypeName = PluginServiceInfo.SERVICE_TYPES.get(serviceClass.getName());
            var message = String.format(
                    "Plugin not available for %s protocol: [%s]",
                    prettyTypeName(rawTypeName, false), protocol);

            log.error(message);
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
