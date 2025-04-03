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

package org.finos.tracdap.tools.secrets;


import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.CryptoHelpers;
import org.finos.tracdap.common.config.ISecretService;
import org.finos.tracdap.common.exception.EConfigLoad;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.startup.StandardArgs;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.config._ConfigFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;


public class SecretTool {

    public final static String INIT_SECRETS = "init_secrets";
    public final static String ADD_SECRET = "add_secret";
    public final static String DELETE_SECRET = "delete_secret";

    public final static String CREATE_ROOT_AUTH_KEY = "create_root_auth_key";
    public final static String ROTATE_ROOT_AUTH_KEY = "rotate_root_auth_key";

    private final static List<StandardArgs.Task> AUTH_TOOL_TASKS = List.of(
            StandardArgs.task(INIT_SECRETS, "Initialize the secrets store"),
            StandardArgs.task(ADD_SECRET, List.of("alias"), "Add a secret to the secret store (you will be prompted for the secret)"),
            StandardArgs.task(DELETE_SECRET, List.of("alias"), "Delete a secret from the secret store"),
            StandardArgs.task(CREATE_ROOT_AUTH_KEY, List.of("ALGORITHM", "BITS"), "Create the root signing key for authentication tokens"));

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ISecretService secrets;

    private final Path keystorePath;
    private final String secretType;
    private final String secretKey;


    /**
     * Construct a new instance of the auth tool
     * @param configManager A prepared instance of ConfigManager
     * @param secretKey Master key for reading / writing the secret store
     */
    public SecretTool(PluginManager pluginManager, ConfigManager configManager, String secretKey) {

        this.secrets = prepareSecrets(pluginManager, configManager);


        var rootConfig = configManager.loadRootConfigObject(_ConfigFile.class, /* leniency = */ true);
        var secretType = rootConfig.getConfigOrDefault(ConfigKeys.SECRET_TYPE_KEY, "");
        var secretUrl = rootConfig.getConfigOrDefault(ConfigKeys.SECRET_URL_KEY, "");
        this.secretKey = secretKey;

        if (!secretType.equalsIgnoreCase("PKCS12") && secretUrl.isBlank()) {
            var message = "To use auth-tool, set secret.type = PKCS12 and specify a secrets file";
            log.error(message);
            throw new EStartup(message);
        }

        var keystoreUrl = configManager.resolveConfigFile(URI.create(secretUrl));

        if (keystoreUrl.getScheme() != null && !keystoreUrl.getScheme().equals("file")) {
            var message = "To use auth-tool, Secrets file must be on a local disk";
            log.error(message);
            throw new EStartup(message);
        }

        this.keystorePath = Paths.get(keystoreUrl);
        this.secretType = secretType;

        log.info("Using local keystore: [{}]", keystorePath);
    }

    public ISecretService prepareSecrets(PluginManager pluginManager, ConfigManager configManager) {

        var rootConfig = configManager.loadRootConfigObject(_ConfigFile.class, /* leniency = */ true);
        var configMap = rootConfig.getConfigMap();
        var protocol = configMap.getOrDefault(ConfigKeys.SECRET_TYPE_KEY, "");

        if (protocol == null || protocol.isBlank()) {
            var message = "No secret service has been configured";
            log.error(message);
            throw new EConfigLoad(message);
        }

        if (!pluginManager.isServiceAvailable(ISecretService.class, protocol)) {
            var message = String.format("No secret service available for protocol [%s]", protocol);
            log.error(message);
            throw new EConfigLoad(message);
        }

        var secretProps = buildSecretProps(configMap);
        var secretService = pluginManager.createConfigService(ISecretService.class, protocol, secretProps);

        secretService.init(configManager, /* createIfMissing = */ true);

        return secretService.scope(ConfigKeys.CONFIG_SCOPE);
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

    public void runTasks(List<StandardArgs.Task> tasks) {

        try {
            for (var task : tasks) {

                log.info("Running task: {} {}", task.getTaskName(), String.join(" ", task.getTaskArgList()));

                if (INIT_SECRETS.equals(task.getTaskName()))
                    initSecrets();

                else if (ADD_SECRET.equals(task.getTaskName()))
                    addSecret(task.getTaskArg(0));

                else if (DELETE_SECRET.equals(task.getTaskName()))
                    deleteSecret(task.getTaskArg(0));

                else if (CREATE_ROOT_AUTH_KEY.equals(task.getTaskName()))
                    createRootAuthKey(task.getTaskArg(0), task.getTaskArg(1));

                else
                    throw new EStartup(String.format("Unknown task: [%s]", task.getTaskName()));
            }

            log.info("All tasks complete");
        }
        catch (Exception e) {
            var message = "There was an error processing the task: " + e.getMessage();
            log.error(message, e);
            throw new EStartup(message, e);
        }

    }

    private void initSecrets() {

        secrets.commit();
    }

    private void addSecret(String alias) {

        var secret = consoleReadPassword("Enter secret for [%s]: ", alias);

        secrets.storePassword(alias, secret);
        secrets.commit();
    }

    private void deleteSecret(String alias) {

        secrets.deleteSecret(alias);
        secrets.commit();
    }

    private void createRootAuthKey(String algorithm, String bits) {

        // TODO: Move to secret service, or remove

        try {

            var keySize = Integer.parseInt(bits);
            var keyGen = KeyPairGenerator.getInstance(algorithm);
            var random = SecureRandom.getInstance("SHA1PRNG");

            keyGen.initialize(keySize, random);

            var keyPair = keyGen.generateKeyPair();

            var keystore = JksHelpers.loadKeystore(secretType, keystorePath, secretKey, true);
            JksHelpers.writeKeysToKeystore(keystore, secretKey, keyPair);
            JksHelpers.saveKeystore(keystorePath, secretKey, keystore);
        }
        catch (NumberFormatException e) {
            var message = String.format("Key size is not an integer [%s]", bits);
            log.error(message);
            throw new EStartup(message, e);
        }
        catch (NoSuchAlgorithmException e) {
            var message = String.format("Unknown signing algorithm [%s]", algorithm);
            log.error(message);
            throw new EStartup(message, e);
        }
    }

    private void writeKeysToFiles(KeyPair keyPair, Path configDir) {

        try {

            log.info("Signing key will be saved in the config directory: [{}]", configDir);

            var armoredPublicKey = CryptoHelpers.encodePublicKey(keyPair.getPublic(), true);
            var armoredPrivateKey = CryptoHelpers.encodePrivateKey(keyPair.getPrivate(), true);

            var publicKeyPath = configDir.resolve(JksHelpers.TRAC_AUTH_PUBLIC_KEY + ".pem");
            var privateKeyPath = configDir.resolve(JksHelpers.TRAC_AUTH_PRIVATE_KEY + ".pem");

            if (Files.exists(publicKeyPath))
                log.error("Key file already exists: {}", publicKeyPath);

            if (Files.exists(privateKeyPath))
                log.error("Key file already exists: {}", privateKeyPath);

            if (Files.exists(publicKeyPath) || Files.exists(privateKeyPath))
                throw new EStartup("Key files already exist, please move them and try again (auth-tool will not delete or replace key files)");

            Files.write(publicKeyPath, armoredPublicKey.getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            Files.write(privateKeyPath, armoredPrivateKey.getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        }
        catch (IOException e) {

            var innerError = (e.getCause() instanceof UnrecoverableEntryException)
                    ? e.getCause() : e;

            var message = String.format("There was a problem saving the keys: %s", innerError.getMessage());
            log.error(message);
            throw new EStartup(message, innerError);
        }
    }

    private String consoleReadLine(String prompt, Object... args) {

        var console = System.console();

        if (console != null)
            return console.readLine(prompt, args);

        else {
            System.out.printf(prompt, args);
            var scanner = new Scanner(System.in);
            return scanner.nextLine();
        }
    }

    private String consoleReadPassword(String prompt, Object... args) {

        var console = System.console();

        if (console != null)
            return new String(console.readPassword(prompt, args));

        else {
            System.out.printf(prompt, args);
            var scanner = new Scanner(System.in);
            return scanner.nextLine();
        }
    }

    /**
     * Entry point for the SecretTool utility.
     *
     * @param args Command line args
     */
    public static void main(String[] args) {

        try {

            // Do not use secrets during start up
            // This tool is often used to create the secrets file

            var startup = Startup.useCommandLine(SecretTool.class, args, AUTH_TOOL_TASKS);
            startup.runStartupSequence(/* useSecrets = */ false);

            var plugins = startup.getPlugins();
            var config = startup.getConfig();
            var tasks = startup.getArgs().getTasks();
            var secretKey = startup.getArgs().getSecretKey();

            var tool = new SecretTool(plugins, config, secretKey);
            tool.runTasks(tasks);

            System.exit(0);
        }
        catch (EStartup e) {

            if (e.isQuiet())
                System.exit(e.getExitCode());

            System.err.println("The service failed to start: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(e.getExitCode());
        }
        catch (Exception e) {

            System.err.println("There was an unexpected error on the main thread: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(-1);
        }
    }
}
