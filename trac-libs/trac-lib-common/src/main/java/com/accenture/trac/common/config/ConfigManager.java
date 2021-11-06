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

import com.accenture.trac.common.plugin.IPluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.InvalidPathException;
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
 */
public class ConfigManager {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final StandardArgs args;
    private final IPluginManager plugins;

    private final URI configRootFile;
    private final URI configRootDir;

    /**
     * Create a ConfigManager for the given standard command line args.
     *
     * @param args Standard command line args, as produced by StandardArgsProcessor
     * @throws EStartup The supplied args are not valid
     */
    public ConfigManager(StandardArgs args, IPluginManager plugins) {

        this.args = args;
        this.plugins = plugins;

        configRootFile = resolveConfigRootFile();
        configRootDir = configRootFile.resolve(".").normalize();

        if ("file".equals(configRootDir.getScheme()))
            log.info("Using config root: {}", Paths.get(configRootDir));
        else
            log.info("Using config root: {}", configRootDir);
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

        return plugins.availableProtocols(IConfigLoader.class);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Config loading
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Load the root configuration as a structured object
     *
     * The configuration must match the structure of the class provided and the file must be in known config format.
     *
     * @return The root configuration as a structured object
     * @throws EStartup The root configuration file could not be loaded or parsed for any reason
     */
    public <TConfig> TConfig loadRootConfig(Class<TConfig> configClass) {

        var configFormat = ConfigFormat.fromExtension(configRootFile);
        var configData = loadTextFile(configRootFile.toString());

        return ConfigParser.parseStructuredConfig(configData, configFormat, configClass);
    }

    /**
     * Load the root configuration as plain text, for processing by a separate parser
     *
     * @return The root configuration text as a single string
     * @throws EStartup The root configuration file could not be loaded for any reason
     */
    public String loadRootConfigAsText() {

        return loadTextFile(configRootFile.toString());
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
        log.info("Loading config file: {}", relativeUrl);

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

            var configUrl = URI.create(args.getConfigFile());
            var configProtocol = configUrl.getScheme();

            // Special handling if the config URI is for a file
            // In this case, it may be relative to the process working dir
            if (configProtocol == null || configProtocol.isBlank() || configProtocol.equals("file")) {

                if (configUrl.isAbsolute())
                    return configUrl;

                var configPath = args.getWorkingDir().resolve(configUrl.getPath());
                return configPath.toUri();
            }

            return configUrl;
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

        if (!plugins.isServiceAvailable(IConfigLoader.class, protocol)) {

            var message = String.format("No config loader available for protocol [%s]", protocol);

            // Logging system may not be initialized while loading config!
            // Use stderr for error messages
            log.error(message);
            throw new EStartup(message);
        }

        return plugins.createService(IConfigLoader.class, protocol);
    }
}
