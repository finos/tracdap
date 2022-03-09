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

package org.finos.tracdap.common.config;

import com.accenture.trac.common.exception.*;

import org.finos.tracdap.common.plugin.IPluginManager;
import com.google.protobuf.Message;
import io.netty.buffer.Unpooled;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.common.startup.StartupSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * <p>ConfigManager provides capabilities to load config files, secrets and other config resources.
 * It uses config loader plugins to provide the same capabilities in a uniform manner across
 * different deployment environments.</p>
 *
 * <p>These easiest way to create a ConfigManager instance is using the Startup class to create
 * a startup sequence. The startup sequence is standard for all TRAC components, it handles
 * loading any required config plugins, logging during the and provides a ConfigManager with
 * he root config loaded and ready to go. If you don't need the startup sequence it is fine
 * to construct a ConfigManager directly, so long as you supply a plugin manager with the
 * required config plugins loaded.</p>
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
 * @see Startup
 * @see StartupSequence
 */
public class ConfigManager {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IPluginManager plugins;

    private final URI configRootFile;
    private final URI configRootDir;

    /**
     * Create a ConfigManager for the root config URL
     *
     * @param configUrl URL for the root config file, as supplied by the user (e.g. on the command line)
     * @param workingDir Working dir of the current process, used to resolve relative URLs for local files
     * @param plugins Plugin manager, used to obtain config loaders depending on the protocol of the config URL
     * @throws EStartup The supplied settings are not valid
     */
    public ConfigManager(String configUrl, Path workingDir, IPluginManager plugins) {

        this.plugins = plugins;

        configRootFile = resolveConfigRootFile(configUrl, workingDir);
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


    // -----------------------------------------------------------------------------------------------------------------
    // Config loading
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Load a config file as plain text
     *
     * <p>Config URLs may be relative or absolute. Relative URLs are resolved relative to the
     * root config directory, absolute URLs may either be a local file path or a full URL
     * including a protocol. Absolute URLs can use a different protocol from the root config
     * file, so long as a config loader is available that can handle that protocol and the required
     * access has been set up.</p>
     *
     * @param configUrl URL of the config file to load
     * @return The contents of the requested config file, as text
     * @throws EStartup The requested config file could not be loaded for any reason
     */
    public String loadConfigFile(String configUrl) {

        if (configUrl == null || configUrl.isBlank())
            throw new EStartup("Config URL is missing or blank");

        var requestedUrl = parseUrl(configUrl);

        return loadConfigFromUrl(requestedUrl);
    }

    /**
     * Load a config file and convert it into an object using the config parser
     *
     * <p>Config URLs may be relative or absolute. Relative URLs are resolved relative to the
     * root config directory, absolute URLs may either be a local file path or a full URL
     * including a protocol. Absolute URLs can use a different protocol from the root config
     * file, so long as a config loader is available that can handle that protocol and the required
     * access has been set up.</p>
     *
     * <p>Once the config file has been read it is parsed using the ConfigParser.
     * The configuration must match the structure of the class provided and the file
     * must be in known config format.</p>
     *
     * @param configUrl URL of the config file to load
     * @param configClass the object class to convert the configuration into
     * @return A config object of the requested class
     * @throws EStartup The requested config file could not be loaded or parsed for any reason
     *
     * @see ConfigParser
     */
    public <TConfig, X extends Message> TConfig loadConfigObject(String configUrl, Class<TConfig> configClass) {

        if (configUrl == null || configUrl.isBlank())
            throw new EStartup("Config URL is missing or blank");

        var requestedUrl = parseUrl(configUrl);

        var configData = loadConfigFromUrl(requestedUrl);
        var configFormat = ConfigFormat.fromExtension(requestedUrl);

        if (Message.class.isAssignableFrom(configClass)) {

            var configBytes = Unpooled.wrappedBuffer(configData.getBytes(StandardCharsets.UTF_8));
            var configMsgClass = (Class<? extends Message>) configClass;
            return (TConfig) ConfigParser.parseConfig(configBytes, configFormat, configMsgClass);

        }
        else
            return ConfigParser.parseStructuredConfig(configData, configFormat, configClass);
    }

    /**
     * Load the root config file as plain text
     *
     * @return The contents of the root config file, as text
     * @throws EStartup The root configuration file could not be loaded for any reason
     */
    public String loadRootConfigFile() {

        return loadConfigFile(configRootFile.toString());
    }

    /**
     * Load the root config file and convert it into an object using the config parser
     *
     * <p>Once the config file has been read it is parsed using the ConfigParser.
     * The configuration must match the structure of the class provided and the file
     * must be in known config format.</p>
     *
     * @return The root configuration as a structured object
     * @throws EStartup The root configuration file could not be loaded or parsed for any reason
     */
    public <TConfig> TConfig loadRootConfigObject(Class<TConfig> configClass) {

        return loadConfigObject(configRootFile.toString(), configClass);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------------

    private String loadConfigFromUrl(URI requestedUrl) {

        var resolvedUrl = resolveUrl(requestedUrl);
        var relativeUrl = configRootDir.relativize(resolvedUrl);

        log.info("Loading config file: {}", relativeUrl);

        var protocol = resolvedUrl.getScheme();
        var loader = configLoaderForProtocol(protocol);

        return loader.loadTextFile(resolvedUrl);
    }

    private URI resolveConfigRootFile(String suppliedConfigUrl, Path workingDir) {

        try {

            if (suppliedConfigUrl == null || suppliedConfigUrl.isBlank())
                throw new EStartup("URL for root config file is missing or blank");

            var configUrl = URI.create(suppliedConfigUrl);
            var configProtocol = configUrl.getScheme();

            // Special handling if the config URI is for a file
            // In this case, it may be relative to the process working dir
            if (configProtocol == null || configProtocol.isBlank() || configProtocol.equals("file")) {

                if (configUrl.isAbsolute())
                    return configUrl;

                var configPath = workingDir.resolve(configUrl.getPath());
                return configPath.toUri();
            }

            return configUrl;
        }
        catch (IllegalArgumentException e) {

            var message = String.format("URL for root config file is invalid: [%s]", suppliedConfigUrl);
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

            log.error(message);
            throw new EStartup(message);
        }

        return plugins.createService(IConfigLoader.class, protocol);
    }
}
