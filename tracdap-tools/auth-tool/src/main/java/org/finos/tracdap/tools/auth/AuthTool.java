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
import org.finos.tracdap.config.GatewayConfig;
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
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


public class AuthTool {

    public final static String INIT_SECRETS = "init_secrets";

    public final static String CREATE_ROOT_AUTH_KEY = "create_root_auth_key";
    public final static String ROTATE_ROOT_AUTH_KEY = "rotate_root_auth_key";

    public final static String INIT_TRAC_USERS = "init_trac_users";
    public final static String ADD_USER = "add_user";
    public final static String DELETE_USER = "delete_user";

    private final static List<StandardArgs.Task> AUTH_TOOL_TASKS = List.of(
            StandardArgs.task(INIT_SECRETS, "Initialize the secrets store"),
            StandardArgs.task(CREATE_ROOT_AUTH_KEY, List.of("ALGORITHM", "BITS"), "Create the root signing key for authentication tokens"),
            StandardArgs.task(INIT_TRAC_USERS, "Create a new TRAC user database (only required if SSO is not deployed)"),
            StandardArgs.task(ADD_USER, "Add a new user to the TRAC user database"),
            StandardArgs.task(DELETE_USER, "USER_ID", "Add a new user to the TRAC user database"));

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConfigManager configManager;

    private final Path keystorePath;
    private final String secretType;
    private final String secretKey;


    /**
     * Construct a new instance of the auth tool
     * @param configManager A prepared instance of ConfigManager
     */
    public AuthTool(ConfigManager configManager, String secretKey) {

        this.configManager = configManager;
        this.secretKey = secretKey;

        var rootConfig = configManager.loadRootConfigObject(_ConfigFile.class, /* leniency = */ true);
        var secretType = rootConfig.getConfigOrDefault(ConfigKeys.SECRET_TYPE_KEY, "");
        var secretUrl = rootConfig.getConfigOrDefault(ConfigKeys.SECRET_URL_KEY, "");

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

    public void runTasks(List<StandardArgs.Task> tasks) {

        try {
            for (var task : tasks) {

                log.info("Running task: {} {}", task.getTaskName(), String.join(" ", task.getTaskArgList()));

                if (INIT_SECRETS.equals(task.getTaskName()))
                    initSecrets();

                else if (CREATE_ROOT_AUTH_KEY.equals(task.getTaskName()))
                    createRootAuthKey(task.getTaskArg(0), task.getTaskArg(1));

                else if (INIT_TRAC_USERS.equals(task.getTaskName()))
                    initTracUsers();

                else if (ADD_USER.equals(task.getTaskName()))
                    addTracUser();

                else if (DELETE_USER.equals(task.getTaskName()))
                    deleteTracUser(task.getTaskArg());

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

    private void initSecrets() throws IOException, GeneralSecurityException {

        var keystore = loadKeystore(secretType, keystorePath, secretKey, true);
        saveKeystore(keystorePath, secretKey, keystore);
    }

    private void createRootAuthKey(String algorithm, String bits) {

        try {

            var keySize = Integer.parseInt(bits);
            var keyGen = KeyPairGenerator.getInstance(algorithm);
            var random = SecureRandom.getInstance("SHA1PRNG");

            keyGen.initialize(keySize, random);

            var keyPair = keyGen.generateKeyPair();

            writeKeysToKeystore(keyPair);
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

    private void initTracUsers() throws Exception {

        var config = configManager.loadRootConfigObject(GatewayConfig.class);

        if (!config.containsConfig(ConfigKeys.USER_DB_TYPE)) {
            var template = "TRAC user database is not enabled (set [%s] in auth provider properties to turn it on)";
            var message = String.format(template, ConfigKeys.USER_DB_TYPE);
            log.error(message);
            throw new EStartup(message);
        }

        if (!config.containsConfig(ConfigKeys.USER_DB_URL)) {
            var message = "Missing required property [" + ConfigKeys.USER_DB_URL + "] in auth provider properties";
            log.error(message);
            throw new EStartup(message);
        }

        if (!config.containsConfig(ConfigKeys.USER_DB_KEY)) {
            var message = "Missing required secret [" + ConfigKeys.USER_DB_KEY + "] in auth provider secrets";
            log.error(message);
            throw new EStartup(message);
        }

        var userDbType = config.getConfigOrThrow(ConfigKeys.USER_DB_TYPE);
        var userDbUrl = config.getConfigOrThrow(ConfigKeys.USER_DB_URL);
        var userDbSecret = config.getConfigOrThrow(ConfigKeys.USER_DB_KEY);

        String userDbKey;

        var secrets = loadKeystore(secretType, keystorePath, secretKey, false);

        if (CryptoHelpers.containsEntry(secrets, userDbSecret)) {

            userDbKey = CryptoHelpers.readTextEntry(secrets, secretKey, userDbSecret);
        }
        else {

            var random = new SecureRandom();
            var secretBytes = new byte[16];
            random.nextBytes(secretBytes);

            var b64 = Base64.getEncoder().withoutPadding();
            userDbKey = b64.encodeToString(secretBytes);

            CryptoHelpers.writeTextEntry(secrets, secretKey, userDbSecret, userDbKey);
            saveKeystore(keystorePath, secretKey, secrets);
        }

        var userDbPath = Paths.get(configManager.resolveConfigFile(URI.create(userDbUrl)));
        var userDb = loadKeystore(userDbType, userDbPath, userDbKey, /* createIfMissing = */ true);
        saveKeystore(userDbPath, userDbKey, userDb);
    }

    private void addTracUser() throws Exception {

        var userId = consoleReadLine("Enter user ID: ").trim();
        var userName = consoleReadLine("Enter full name for [%s]: ", userId).trim();
        var password = consoleReadPassword("Enter password for [%s]: ", userId);

        var random = new SecureRandom();
        var salt = new byte[16];
        random.nextBytes(salt);

        var hash = CryptoHelpers.encodeSSHA512(password, salt);
        var attrs = Map.of("displayname", userName);

        var userDb = loadUserDb();

        CryptoHelpers.writeTextEntry(userDb, secretKey, userId, hash, attrs);

        saveUserDb(userDb);
    }

    private void deleteTracUser(String userId) throws Exception {

        var userDb = loadUserDb();

        CryptoHelpers.deleteEntry(userDb, userId);

        saveUserDb(userDb);
    }

    private void writeKeysToKeystore(KeyPair keyPair) {

        try {

            var keystore = loadKeystore(secretType, keystorePath, secretKey, /* createIfMissing = */ true);

            var publicEncoded = CryptoHelpers.encodePublicKey(keyPair.getPublic(), false);
            var privateEncoded = CryptoHelpers.encodePrivateKey(keyPair.getPrivate(), false);

            CryptoHelpers.writeTextEntry(keystore, secretKey, ConfigKeys.TRAC_AUTH_PUBLIC_KEY, publicEncoded);
            CryptoHelpers.writeTextEntry(keystore, secretKey, ConfigKeys.TRAC_AUTH_PRIVATE_KEY, privateEncoded);

            saveKeystore(keystorePath, secretKey, keystore);
        }
        catch (IOException | GeneralSecurityException e) {

            var innerError = (e.getCause() instanceof UnrecoverableEntryException)
                    ? e.getCause() : e;

            var message = String.format("There was a problem saving the keys: %s", innerError.getMessage());
            log.error(message);
            throw new EStartup(message, innerError);
        }
    }

    private static KeyStore loadKeystore(
            String keystoreType, Path keystorePath, String keystoreKey,
            boolean createIfMissing)
            throws IOException, GeneralSecurityException {

        var keystore = KeyStore.getInstance(keystoreType);

        if (!Files.exists(keystorePath) && createIfMissing) {
            keystore.load(null, keystoreKey.toCharArray());
        }
        else {
            try (var in = new FileInputStream(keystorePath.toFile())) {
                keystore.load(in, keystoreKey.toCharArray());
            }
        }

        return keystore;
    }

    private static void saveKeystore(
            Path keystorePath, String keystoreKey, KeyStore keystore)
            throws IOException, GeneralSecurityException {

        var keystoreBackup = keystorePath.getParent().resolve((keystorePath.getFileSystem() + ".upd~"));

        if (Files.exists(keystorePath))
            Files.move(keystorePath, keystoreBackup);

        try (var out = new FileOutputStream(keystorePath.toFile())) {
            keystore.store(out, keystoreKey.toCharArray());
        }

        if (Files.exists(keystoreBackup))
            Files.delete(keystoreBackup);
    }

    private KeyStore loadUserDb() throws IOException, GeneralSecurityException {

        var secrets = loadKeystore(secretType, keystorePath, secretKey, false);

        var config = configManager.loadRootConfigObject(GatewayConfig.class);
        var userDbType = config.getConfigOrThrow(ConfigKeys.USER_DB_TYPE);
        var userDbUrl = config.getConfigOrThrow(ConfigKeys.USER_DB_URL);
        var userDbSecret = config.getConfigOrThrow(ConfigKeys.USER_DB_KEY);

        var userDbPath = Paths.get(configManager.resolveConfigFile(URI.create(userDbUrl)));
        var userDbKey = CryptoHelpers.readTextEntry(secrets, secretKey, userDbSecret);

        return loadKeystore(userDbType, userDbPath, userDbKey, false);
    }

    private void saveUserDb(KeyStore userDb) throws IOException, GeneralSecurityException {

        var secrets = loadKeystore(secretType, keystorePath, secretKey, false);

        var config = configManager.loadRootConfigObject(GatewayConfig.class);
        var userDbUrl = config.getConfigOrThrow(ConfigKeys.USER_DB_URL);
        var userDbSecret = config.getConfigOrThrow(ConfigKeys.USER_DB_KEY);

        var userDbPath = Paths.get(configManager.resolveConfigFile(URI.create(userDbUrl)));
        var userDbKey = CryptoHelpers.readTextEntry(secrets, secretKey, userDbSecret);

        saveKeystore(userDbPath, userDbKey, userDb);
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
