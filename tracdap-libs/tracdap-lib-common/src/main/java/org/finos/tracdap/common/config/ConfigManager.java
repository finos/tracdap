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

import org.finos.tracdap.common.config.local.JksSecretLoader;
import org.finos.tracdap.common.exception.EConfigLoad;
import org.finos.tracdap.common.plugin.IPluginManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.common.startup.StartupSequence;

import com.google.protobuf.Message;
import org.finos.tracdap.common.startup.StartupLog;
import org.finos.tracdap.config._ConfigFile;
import org.slf4j.event.Level;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;
import java.util.Properties;


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

    private final IPluginManager plugins;

    private final URI rootConfigFile;
    private final URI rootConfigDir;

    private final String secretKey;
    private ISecretLoader secrets = null;

    private byte[] rootConfigCache = null;

    public ConfigManager(String configUrl, Path workingDir, IPluginManager plugins) {
        this(configUrl, workingDir, plugins, null);
    }

    /**
     * Create a ConfigManager for the root config URL
     *
     * @param configUrl URL for the root config file, as supplied by the user (e.g. on the command line)
     * @param workingDir Working dir of the current process, used to resolve relative URLs for local files
     * @param plugins Plugin manager, used to obtain config loaders depending on the protocol of the config URL
     * @throws EStartup The supplied settings are not valid
     */
    public ConfigManager(String configUrl, Path workingDir, IPluginManager plugins, String secretKey) {

        this.plugins = plugins;
        this.secretKey = secretKey;

        this.rootConfigFile = resolveRootUrl(parseUrl(configUrl), workingDir);
        this.rootConfigDir = rootConfigFile.resolve(".").normalize();

        var rootDirDisplay = "file".equals(rootConfigDir.getScheme())
                ? Paths.get(rootConfigDir).toString()
                : rootConfigDir.toString();

        StartupLog.log(this, Level.INFO, String.format("Using config root: %s", rootDirDisplay));
    }

    public void prepareSecrets() {

        var rootConfig = loadRootConfigObject(_ConfigFile.class, /* leniency = */ true);
        var configMap = rootConfig.getConfigMap();

        var secretType = configMap.getOrDefault(ConfigKeys.SECRET_TYPE_KEY, "");
        var secretUrl = configMap.getOrDefault(ConfigKeys.SECRET_URL_KEY, "");

        if (secretType == null || secretType.isBlank()) {
            StartupLog.log(this, Level.INFO, "Using secrets: [none]");
            return;
        }

        StartupLog.log(this, Level.INFO, String.format("Using secrets: [%s] %s", secretType, secretUrl));

        this.secrets = secretLoaderForProtocol(secretType, configMap);
    }

    public ISecretLoader getUserDb() {

        var config = loadRootConfigObject(_ConfigFile.class, /* leniency = */ true);

        if (!config.containsConfig(ConfigKeys.USER_DB_TYPE)) {
            var template = "TRAC user database is not enabled (set config key [%s] to turn it on)";
            var message = String.format(template, ConfigKeys.USER_DB_TYPE);
            throw new EStartup(message);
        }

        if (!config.containsConfig(ConfigKeys.USER_DB_URL)) {
            var message = "Missing required config key [" + ConfigKeys.USER_DB_URL + "]";
            throw new EStartup(message);
        }

        var userDbType = config.getConfigOrThrow(ConfigKeys.USER_DB_TYPE);
        var userDbUrl = config.getConfigOrThrow(ConfigKeys.USER_DB_URL);
        var userDbSecret = config.getConfigOrDefault(ConfigKeys.USER_DB_KEY, "");

        var userDbPath = Paths.get(resolveConfigFile(URI.create(userDbUrl)));
        var userDbKey = userDbSecret.isEmpty()
                ? secretKey
                : loadPassword(userDbSecret);

        var userDbProps = new Properties();
        userDbProps.put(ConfigKeys.SECRET_TYPE_KEY, userDbType);
        userDbProps.put(ConfigKeys.SECRET_URL_KEY, userDbPath.toString());
        userDbProps.put(ConfigKeys.SECRET_KEY_KEY, userDbKey);

        var userDb = new JksSecretLoader(userDbProps);
        userDb.init(this);

        return userDb;
    }

    /**
     * Get the root config directory, used for resolving relative URLs.
     *
     * @return The root config directory
     */
    public URI configRoot() {
        return rootConfigDir;
    }

    /**
     * Allow client code to resolve config files explicitly
     *
     * <p>This is not normally needed by the TRAC services, which read config though this class.
     * But the TRAC utility programs can use this method to find config files for updating.
     *
     * @param relativePath Relative URL of the config file to resolve
     * @return The resolved absolute URL of the config file
     */
    public URI resolveConfigFile(URI relativePath) {

        return resolveUrl(relativePath);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Config loading
    // -----------------------------------------------------------------------------------------------------------------


    /**
     * Load a config file as an array of bytes
     *
     * <p>Config URLs may be relative or absolute. Relative URLs are resolved relative to the
     * root config directory, absolute URLs may either be a local file path or a full URL
     * including a protocol. Absolute URLs can use a different protocol from the root config
     * file, so long as a config loader is available that can handle that protocol and the required
     * access has been set up.</p>
     *
     * @param configUrl URL of the config file to load
     * @return The contents of the requested config file, as a byte array
     * @throws EStartup The requested config file could not be loaded for any reason
     */
    public byte[] loadBinaryConfig(String configUrl) {

        var parsed = parseUrl(configUrl);
        var resolved = resolveUrl(parsed);
        return loadUrl(resolved);
    }

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
    public String loadTextConfig(String configUrl) {

        var parsed = parseUrl(configUrl);
        var resolved = resolveUrl(parsed);
        var bytes = loadUrl(resolved);
        return new String(bytes, StandardCharsets.UTF_8);
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
     * @param configClass Config class used to parse the configuration
     * @param <TConfig> Type variable for the config class (must be a protobuf message type)
     * @param leniency If true, ignore unrecognized fields in the configuration
     * @return A config object of the requested class
     * @throws EStartup The requested config file could not be loaded or parsed for any reason
     *
     * @see ConfigParser
     */
    public <TConfig extends Message> TConfig loadConfigObject(
            String configUrl, Class<TConfig> configClass, boolean leniency) {

        var parsed = parseUrl(configUrl);
        var resolved = resolveUrl(parsed);
        var bytes = loadUrl(resolved);
        var format = ConfigFormat.fromExtension(resolved);

        return ConfigParser.parseConfig(bytes, format, configClass, leniency);
    }

    /**
     * Load a config file and convert it into an object using the config parser
     *
     * <p>Convenience overload with leniency = false</p>
     *
     * @param configUrl URL of the config file to load
     * @param configClass Config class used to parse the configuration
     * @param <TConfig> Type variable for the config class (must be a protobuf message type)
     * @return A config object of the requested class
     * @throws EStartup The requested config file could not be loaded or parsed for any reason
     */
    public <TConfig extends Message> TConfig loadConfigObject(String configUrl, Class<TConfig> configClass) {

        return loadConfigObject(configUrl, configClass, /* leniency = */ false);
    }


    /**
     * Load the root config file and convert it into an object using the config parser
     *
     * <p>Once the config file has been read it is parsed using the ConfigParser.
     * The configuration must match the structure of the class provided and the file
     * must be in known config format.</p>
     *
     * @param configClass Config class used to parse the configuration
     * @param <TConfig> Type variable for the config class (must be a protobuf message type)
     * @param leniency If true, ignore unrecognized fields in the configuration
     * @return The root configuration as a structured object
     * @throws EStartup The root configuration file could not be loaded or parsed for any reason
     */
    public <TConfig extends Message> TConfig loadRootConfigObject(Class<TConfig> configClass, boolean leniency) {

        // Avoid loading root config file multiple times during startup
        var bytes = rootConfigCache;

        if (bytes == null) {

            var parsed = parseUrl(rootConfigFile.toString());
            var resolved = resolveUrl(parsed);

            bytes = loadUrl(resolved);
            rootConfigCache = bytes;
        }

        var format = ConfigFormat.fromExtension(rootConfigFile);

        return ConfigParser.parseConfig(bytes, format, configClass, leniency);
    }

    /**
     * Load the root config file and convert it into an object using the config parser
     *
     * <p>Convenience overload with leniency = false</p>
     *
     * @param configClass Config class used to parse the configuration
     * @param <TConfig> Type variable for the config class (must be a protobuf message type)
     * @return The root configuration as a structured object
     * @throws EStartup The root configuration file could not be loaded or parsed for any reason
     */
    public <TConfig extends Message> TConfig loadRootConfigObject(Class<TConfig> configClass) {

        return loadRootConfigObject(configClass, /* leniency = */ false);
    }

    public boolean hasSecret(String secretName) {

        // If secrets are not enabled, hasSecret() should always return false
        if (secrets == null) {
            return false;
        }

        return secrets.hasSecret(secretName);
    }

    public String loadPassword(String secretName) {

        if (secrets == null) {
            var message = String.format("Secrets are not enabled, to use secrets set secret.type in [%s]", rootConfigFile);
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }

        return secrets.loadPassword(secretName);
    }

    public PublicKey loadPublicKey(String secretName) {

        if (secrets == null) {
            var message = String.format("Secrets are not enabled, to use secrets set secret.type in [%s]", rootConfigFile);
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }

        return secrets.loadPublicKey(secretName);
    }

    public PrivateKey loadPrivateKey(String secretName) {

        if (secrets == null) {
            var message = String.format("Secrets are not enabled, to use secrets set secret.type in [%s]", rootConfigFile);
            StartupLog.log(this, Level.ERROR, message);
            throw new EStartup(message);
        }

        return secrets.loadPrivateKey(secretName);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------------

    private URI parseUrl(String urlString) {

        if (urlString == null || urlString.isBlank())
            throw new EConfigLoad("Config URL is missing or blank");

        Path path = null;
        URI url = null;

        try {
            path = Paths.get(urlString).normalize();
        }
        catch (InvalidPathException ignored) { }

        try {
            url = URI.create(urlString);
        }
        catch (IllegalArgumentException ignored) { }

        if (urlString.startsWith("/") || urlString.startsWith("\\") || urlString.startsWith(":\\", 1)) {
            if (path != null)
                return path.toUri();
        }

        if (url != null)
            return url;

        if (path != null)
            return path.toUri();

        throw new EConfigLoad("Requested config URL is not a valid URL or path: " + urlString);
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
                throw new EConfigLoad(message);
            }
        }
        else {
            if (protocol != null && !protocol.isBlank()) {
                var message = String.format(ERROR_MSG_TEMPLATE, url, "Relative URLs cannot specify an explicit protocol");
                throw new EConfigLoad(message);
            }
        }

        // Explicit guard against UNC-style paths (most likely this is broken config anyway)
        if ("file".equals(protocol)) {

            if (url.getHost() != null) {
                var message = String.format(ERROR_MSG_TEMPLATE, url, "Network file paths are not supported");
                throw new EConfigLoad(message);
            }
        }

        if (isAbsolute) {
            return url.normalize();
        }
        else {
            return rootConfigDir.resolve(url).normalize();
        }
    }

    private URI resolveRootUrl(URI url, Path workingDir) {

        var protocol = url.getScheme();

        // Special handling if the config URI is for a file
        // In this case, it may be relative to the process working dir
        if (protocol == null || protocol.isBlank() || protocol.equals("file")) {

            if (url.isAbsolute()) {
                return url;
            }
            else {
                var configPath = workingDir.resolve(url.getPath());
                return configPath.toUri();
            }
        }
        else {

            if (!url.isAbsolute()) {
                var message = String.format("Relative URL is not allowed for root config file with protocol [%s]: [%s]", protocol, url);
                throw new EConfigLoad(message);
            }
            else {
                return url;
            }
        }
    }

    private byte[] loadUrl(URI absoluteUrl) {

        // Display relative URLs in the log if possible
        var relativeUrl = rootConfigDir.relativize(absoluteUrl);

        var message = String.format("Loading config file: [%s]", relativeUrl);
        StartupLog.log(this, Level.INFO, message);

        var protocol = absoluteUrl.getScheme();
        var loader = configLoaderForProtocol(protocol);

        return loader.loadBinaryFile(absoluteUrl);
    }

    private IConfigLoader configLoaderForProtocol(String protocol) {

        if (protocol == null || protocol.isBlank())
            protocol = "file";

        if (!plugins.isServiceAvailable(IConfigLoader.class, protocol)) {

            var message = String.format("No config loader available for protocol [%s]", protocol);

            StartupLog.log(this, Level.ERROR, message);
            throw new EConfigLoad(message);
        }

        return plugins.createConfigService(IConfigLoader.class, protocol, new Properties());
    }

    private ISecretLoader secretLoaderForProtocol(String protocol, Map<String, String> configMap) {

        if (!plugins.isServiceAvailable(ISecretLoader.class, protocol)) {

            var message = String.format("No secret loader available for protocol [%s]", protocol);

            StartupLog.log(this, Level.ERROR, message);
            throw new EConfigLoad(message);
        }

        var secretProps = buildSecretProps(configMap);
        var secretLoader = plugins.createConfigService(ISecretLoader.class, protocol, secretProps);
        secretLoader.init(this);

        return secretLoader;
    }

    private Properties buildSecretProps(Map<String, String> configMap) {

        // These props are needed for JKS secrets
        // Cloud platforms would normally use IAM to access the secret store
        // But, any set of props can be used, with keys like secret.*

        var secretProps = new Properties();

        for (var secretEntry : configMap.entrySet()) {
            if (secretEntry.getKey().startsWith("secret.") && secretEntry.getValue() != null)
                secretProps.put(secretEntry.getKey(), secretEntry.getValue());
        }

        if (secretKey != null && !secretKey.isBlank()) {
            secretProps.put(ConfigKeys.SECRET_KEY_KEY, secretKey);
        }

        return secretProps;
    }
}
