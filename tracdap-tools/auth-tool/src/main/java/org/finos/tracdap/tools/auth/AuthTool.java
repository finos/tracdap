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

package org.finos.tracdap.tools.auth;


import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.CryptoHelpers;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.startup.StandardArgs;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.config._ConfigFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.*;
import java.util.List;


public class AuthTool {

    public final static String SIGNING_KEY_TASK = "signing_key";

    private final static List<StandardArgs.Task> AUTH_TOOL_TASKS = List.of(
            StandardArgs.task(SIGNING_KEY_TASK, List.of("ALGORITHM", "BITS"), "Create or replace the platform signing key for authentication tokens"));

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;
    private final String secretKey;


    /**
     * Construct a new instance of the auth tool
     * @param configManager A prepared instance of ConfigManager
     */
    public AuthTool(ConfigManager configManager, String secretKey) {
        this.configManager = configManager;
        this.secretKey = secretKey;
    }

    public void runTasks(List<StandardArgs.Task> tasks) {


        for (var task : tasks) {

            if (SIGNING_KEY_TASK.equals(task.getTaskName()))
                createSigningKey(task.getTaskArg(0), task.getTaskArg(1));

            else
                throw new EStartup(String.format("Unknown task: [%s]", task.getTaskName()));
        }

        log.info("All tasks complete");

    }

    private void createSigningKey(String algorithm, String bits) {

        try {

            log.info("Running task: Create signing key...");

            var keySize = Integer.parseInt(bits);
            var keyGen = KeyPairGenerator.getInstance(algorithm);
            var random = SecureRandom.getInstance("SHA1PRNG");

            keyGen.initialize(keySize, random);

            var keyPair = keyGen.generateKeyPair();

            outputSigningKey(keyPair);
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

    private void outputSigningKey(KeyPair keyPair) {

        var rootConfig = configManager.loadRootConfigObject(_ConfigFile.class, /* leniency = */ true);
        var secretType = rootConfig.getConfigOrDefault(ConfigKeys.SECRET_TYPE_KEY, "");
        var secretUrl = rootConfig.getConfigOrDefault(ConfigKeys.SECRET_URL_KEY, "");

        Path keystorePath = null;

        if (secretType.equalsIgnoreCase("PKCS12") && !secretUrl.isBlank()) {

            var keystoreUrl = configManager.resolveConfigFile(URI.create(secretUrl));

            if (keystoreUrl.getScheme() == null ||  keystoreUrl.getScheme().equals("file")) {
                keystorePath= Paths.get(keystoreUrl);
            }
        }

        if (keystorePath != null)
            writeKeysToKeystore(keyPair, secretUrl, secretType);
        else
            writeKeysToFiles(keyPair, Paths.get(configManager.configRoot()));
    }

    private void writeKeysToKeystore(KeyPair keyPair, String secretUrl, String secretType) {

        try {

            var keystorePath = Paths.get(configManager.resolveConfigFile(URI.create(secretUrl)));
            var keystore = KeyStore.getInstance(secretType);

            log.info("Signing key will be saved in the keystore: [{}]", keystorePath);

            if (Files.exists(keystorePath)) {
                try (var in = new FileInputStream(keystorePath.toFile())) {
                    keystore.load(in, secretKey.toCharArray());
                }
            }
            else {
                keystore.load(null, secretKey.toCharArray());
            }

            var publicEncoded = CryptoHelpers.encodePublicKey(keyPair.getPublic(), false);
            var privateEncoded = CryptoHelpers.encodePrivateKey(keyPair.getPrivate(), false);

            CryptoHelpers.writeTextEntry(keystore, ConfigKeys.TRAC_AUTH_PUBLIC_KEY, publicEncoded, secretKey);
            CryptoHelpers.writeTextEntry(keystore, ConfigKeys.TRAC_AUTH_PRIVATE_KEY, privateEncoded, secretKey);

            var keystoreBackup = keystorePath.getParent().resolve((keystorePath.getFileSystem() + ".upd~"));

            if (Files.exists(keystorePath))
                Files.move(keystorePath, keystoreBackup);

            try (var out = new FileOutputStream(keystorePath.toFile())) {
                keystore.store(out, secretKey.toCharArray());
            }

            if (Files.exists(keystoreBackup))
                Files.delete(keystoreBackup);
        }
        catch (IOException | GeneralSecurityException e) {

            var innerError = (e.getCause() instanceof UnrecoverableEntryException)
                    ? e.getCause() : e;

            var message = String.format("There was a problem saving the keys: %s", innerError.getMessage());
            log.error(message);
            throw new EStartup(message, innerError);
        }
    }

    private void writeKeysToFiles(KeyPair keyPair, Path configDir) {

        try {

            log.info("Signing key will be saved in the config directory: [{}]", configDir);

            var armoredPublicKey = CryptoHelpers.encodePublicKey(keyPair.getPublic(), true);
            var armoredPrivateKey = CryptoHelpers.encodePrivateKey(keyPair.getPrivate(), true);

            var publicKeyPath = configDir.resolve(ConfigKeys.TRAC_AUTH_PUBLIC_KEY + ".pem");
            var privateKeyPath = configDir.resolve(ConfigKeys.TRAC_AUTH_PRIVATE_KEY + ".pem");

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

    /**
     * Entry point for the AuthTool utility.
     *
     * @param args Command line args
     */
    public static void main(String[] args) {

        try {

            // Do not use secrets during start up
            // This tool is often used to create the secrets file

            var startup = Startup.useCommandLine(AuthTool.class, args, AUTH_TOOL_TASKS);
            startup.runStartupSequence(/* useSecrets = */ false);

            var config = startup.getConfig();
            var tasks = startup.getArgs().getTasks();
            var secretKey = startup.getArgs().getSecretKey();

            var tool = new AuthTool(config, secretKey);
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
