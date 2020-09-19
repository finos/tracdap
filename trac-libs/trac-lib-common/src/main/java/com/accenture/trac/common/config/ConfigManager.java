/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.common.config;

import com.accenture.trac.common.exception.*;

import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * <p>ConfigManager allows config files, secrets and other config resources to be loaded
 * in a uniform manner across different deployment environments.</p>
 *
 * <p>To create a ConfigManager you must supply an instance of StandardArgs, which provides
 * the location of the root config file and the master key for unlocking secrets. Use a
 * StandardArgsProcessor to read the required command line arguments. ConfigManager will
 * determine the root config directory, which is just the directory containing the root config
 * file. Before reading any config, initialize the ConfigManager by calling initConfigPlugins()
 * and initLogging().</p>
 *
 * <p>Read the root config file by calling loadRootProperties(). To read additional config
 * files, use the load* functions and specify a config URL for the file you want to load.
 * Config URLs may be relative or absolute. Relative URLs are resolved relative to the root
 * config directory, absolute URLs may either be a local file path or a full URL including
 * a protocol.</p>
 *
 * <p>ConfigManager uses a set of config plugins to physically load files from where they
 * are stored. Config plugins must implement IConfigPlugin and supply an instance of
 * IConfigLoader, which will receive requests to load individual files. The core implementation
 * comes with a config plugin for loading from the filesystem. Additional plugins may be
 * supplied to load config files other locations such as web servers or cloud buckets, any
 * config plugins on the classpath will be loaded whe initConfigPlugins() is called.
 * See IConfigPlugin for details of how to implement a config plugin.</p>
 *
 * @see StandardArgsProcessor
 * @see IConfigPlugin
 */
public class ConfigManager {

    private static final String LOGGING_CONFIG_URL = "config.logging.url";

    private final StandardArgs args;

    private final URI configRootFile;
    private final URI configRootDir;

    private final Map<String, IConfigLoader> configLoaders;

    private Logger log;

    /**
     * Create a ConfigManager for the given standard command line args.
     *
     * @param args Standard command line args, as produced by StandardArgsProcessor
     * @throws EStartup The supplied args are not valid
     */
    public ConfigManager(StandardArgs args) {

        this.args = args;

        configRootFile = resolveConfigRootFile();
        configRootDir = configRootFile.resolve(".").normalize();

        if ("file".equals(configRootDir.getScheme()))
            logInfo("Using config root: " + Paths.get(configRootDir));
        else
            logInfo("Using config root: " + configRootDir);

        this.configLoaders = new HashMap<>();
    }

    /**
     * Get the root config directory, used for resolving relative URLs.
     *
     * @return The root config directory
     */
    public URI configRoot() {
        return configRootDir;
    }

    /**
     * Get the list of supported URL protocols for config loading.
     *
     * <p>The list is determined by the currently loaded set of config plugins.</p>
     *
     * @return List of supported URL protocols for config loading
     */
    public List<String> protocols() {
        return List.copyOf(configLoaders.keySet());
    }

    /**
     * Initialize the available set of config loading plugins.
     *
     * <p>This method must be called before any config can be loaded. It will find and load
     * all the available config loading plugins on the current classpath, including the
     * built in loader for loading from the filesystem.</p>
     *
     * @throws EStartup There was an error initializing one of the plugins
     */
    public void initConfigPlugins() {

        logInfo("Looking for config plugins...");

        var availablePlugins = ServiceLoader.load(IConfigPlugin.class);

        for (var plugin: availablePlugins) {

            var loader = plugin.createConfigLoader(args);

            var discoveryMsg = String.format("Config plugin: %s (protocols: %s)",
                    loader.loaderName(),
                    String.join(", ", loader.protocols()));

            logInfo(discoveryMsg);

            for (var protocol : loader.protocols())
                configLoaders.put(protocol, loader);
        }
    }

    /**
     * Initialize the logging framework.
     *
     * <p>Logging can be configured by setting the property config.logging.url in the
     * root property file to point to the location of a logging config file. TRAC
     * uses Log4j2 as a backend for slf4j, so the logging config file must be a valid
     * log4j2 config file. If no logging config is provided, messages will be logged
     * to stdout.</p>
     *
     * <p>This method should be called immediately after calling initConfigPlugins(),
     * since it uses the config loading mechanism which relies on those plugins being
     * available.</p>
     *
     * @throws EStartup There was an error processing the logging config
     */
    public void initLogging() {

        logInfo("Initialize logging...");

        var rootProps = loadRootProperties();
        var loggingConfigUrl = rootProps.getProperty(LOGGING_CONFIG_URL);

        if (loggingConfigUrl != null && !loggingConfigUrl.isBlank()) {

            var loggingConfig = loadTextFile(loggingConfigUrl);

            try (var configStream = new ByteArrayInputStream(loggingConfig.getBytes())) {
                var configSource = new ConfigurationSource(configStream);
                Configurator.initialize(getClass().getClassLoader(), configSource);

                // Invalid configuration errors cause the process to bomb out
            }
            catch (IOException e) {
                // Unexpected error condition - IO error reading from a byte buffer
                throw new EUnexpected(e);
            }
        }
        else {
            // Fall back on logging to stdout, use log4j2.xml bundled in this library
            Configurator.reconfigure();
        }

        this.log = LoggerFactory.getLogger(getClass());
        logInfo("Logging system initialized");
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Config loading
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Load the root configuration properties, as specified in the constructor by StandardArgs.
     *
     * @return The root configuration properties
     * @throws EStartup The root configuration properties could not be loaded for any reason
     */
    public Properties loadRootProperties() {

        return loadProperties(configRootFile.toString());
    }

    /**
     * Load a secondary set of properties.
     *
     * <p>Config URLs may be relative or absolute. Relative URLs are resolved relative to the
     * root config directory, absolute URLs may either be a local file path or a full URL
     * including a protocol.</p>
     *
     * @param configUrl URL of the properties file to load
     * @return The requested set of properties
     * @throws EStartup The requested set of properties could not be loaded for any reason
     */
    public Properties loadProperties(String configUrl) {

        String content = loadTextFile(configUrl);

        try (var reader = new StringReader(content)) {

            var properties = new Properties();
            properties.load(reader);

            return properties;
        }
        catch (IOException e) {
            // Should never happen, especially since props.load does no validation!
            throw new EUnexpected(e);
        }
    }

    /**
     * Load a secondary config file as text.
     *
     * <p>Config URLs may be relative or absolute. Relative URLs are resolved relative to the
     * root config directory, absolute URLs may either be a local file path or a full URL
     * including a protocol.</p>
     *
     * @param configUrl URL of the config file to load
     * @return The contents of the requested config file, as text
     * @throws EStartup The requested config file could not be loaded for any reason
     */
    public String loadTextFile(String configUrl) {

        if (configUrl == null || configUrl.isBlank())
            throw new EStartup("Config URL is missing or blank");

        var requestedUrl = parseUrl(configUrl);
        var resolvedUrl = resolveUrl(requestedUrl);

        var relativeUrl = configRootDir.relativize(resolvedUrl);
        logInfo("Loading config file: " + relativeUrl);

        var protocol = resolvedUrl.getScheme();
        var loader = configLoaderForProtocol(protocol);

        return loader.loadTextFile(resolvedUrl);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------------

    private URI resolveConfigRootFile() {

        try {

            if (args.getConfigFile() == null || args.getConfigFile().isBlank())
                throw new EStartup("URL for root config file is missing or blank");

            var suppliedUri = URI.create(args.getConfigFile());
            var suppliedScheme = suppliedUri.getScheme();

            // Special handling if the config URI is for a file
            // In this case, it may be relative to the process working dir
            if (suppliedScheme == null || suppliedScheme.isBlank() || suppliedScheme.equals("file")) {

                Path configPath = args.getWorkingDir().resolve(args.getConfigFile());
                return configPath.toUri();
            }

            return suppliedUri;
        }
        catch (IllegalArgumentException e) {

            var message = String.format("URL for root config file is invalid: [%s]", args.getConfigFile());
            throw new EStartup(message);
        }
    }

    private URI parseUrl(String url) {

        if (url.startsWith("/") || url.startsWith("\\")) {
            try {
                return Paths.get(url).toUri().normalize();
            }
            catch (InvalidPathException ignored) { }
        }

        try {
            return URI.create(url);
        }
        catch (IllegalArgumentException ignored) { }

        try {
            return Paths.get(url).toUri().normalize();
        }
        catch (InvalidPathException ignored) { }

        throw new EStartup("Requested config URL is not a valid URL or path: " + url);
    }

    private URI resolveUrl(URI url) {

        var ERROR_MSG_TEMPLATE = "Invalid URL for config file: %2$s [%1$s]";

        var protocol = url.getScheme();

        var path = url.getPath() != null ? url.getPath() : url.getSchemeSpecificPart();
        var isAbsolute = path.startsWith("/") || path.startsWith("\\");

        // Check for correct use of protocols with absolute / relative URLs
        // Absolute URLs must either specify a protocol or be a file path (parse URL will set protocol = file)
        // Relative URLs cannot specify a protocol
        if (isAbsolute) {
            if (protocol == null || protocol.isBlank()) {
                var message = String.format(ERROR_MSG_TEMPLATE, url, "Absolute URLs must specify an explicit protocol");
                throw new EStartup(message);
            }
        }
        else {
            if (protocol != null && !protocol.isBlank()) {
                var message = String.format(ERROR_MSG_TEMPLATE, url, "Relative URLs cannot specify an explicit protocol");
                throw new EStartup(message);
            }
        }

        // Explicit guard against UNC-style paths (most likely this is broken config anyway)
        if ("file".equals(protocol)) {

            if (url.getHost() != null) {
                var message = String.format(ERROR_MSG_TEMPLATE, url, "Network file paths are not supported");
                throw new EStartup(message);
            }
        }

        if (isAbsolute) {
            return url.normalize();
        }
        else {
            return configRootDir.resolve(url).normalize();
        }
    }

    private IConfigLoader configLoaderForProtocol(String protocol) {

        if (protocol == null || protocol.isBlank())
            protocol = "file";

        var loader = configLoaders.get(protocol);

        if (loader == null) {

            var message = String.format("No config loader available for protocol [%s]", protocol);

            // Logging system may not be initialized while loading config!
            // Use stderr for error messages
            logError(message);
            throw new EStartup(message);
        }

        return loader;
    }

    private void logInfo(String message) {

        if (this.log != null)
            log.info(message);
        else
            System.out.println(message);
    }

    private void logError(String message) {

        if (this.log != null)
            log.error(message);
        else
            System.err.println(message);
    }
}
