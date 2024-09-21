/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.plugins.exec.ssh;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.exec.*;
import org.finos.tracdap.common.util.ResourceHelpers;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.NamedResource;
import org.apache.sshd.common.keyprovider.KeyIdentityProvider;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.apache.sshd.netty.NettyIoServiceFactoryFactory;
import org.apache.sshd.scp.client.ScpClient;
import org.apache.sshd.scp.client.ScpClientCreator;
import org.apache.sshd.scp.common.helpers.ScpTimestampCommandDetails;

import org.finos.tracdap.config.StorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.attribute.PosixFilePermission;
import java.rmi.ServerException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public class SshExecutor implements IBatchExecutor<SshExecutorState> {

    public static final String REMOTE_HOST_KEY = "remoteHost";
    public static final String REMOTE_PORT_KEY = "remotePort";
    public static final String BATCH_USER_KEY = "batchUser";
    public static final String KEY_FILE_KEY = "keyFile";
    public static final String CONFIG_VENV_PATH = "venvPath";
    public static final String CONFIG_BATCH_DIR = "batchDir";
    public static final String CONFIG_BATCH_PERSIST = "batchPersist";

    private static final String LAUNCH_SCRIPT_NAME = "launch_batch.sh";
    private static final String POLL_SCRIPT_NAME = "poll_batch.sh";
    private static final String LAUNCH_SCRIPT = ResourceHelpers.loadResourceAsString("/scripts/launch_batch.sh", SshExecutor.class);
    private static final String POLL_SCRIPT = ResourceHelpers.loadResourceAsString("/scripts/poll_batch.sh", SshExecutor.class);

    private static final String CREATE_BATCH_DIR_COMMAND = "mkdir -p -m %s \"%s\"";
    private static final String DESTROY_BATCH_PROCESS_COMMAND = "if ps -p %d; then kill -s KILL %d; fi";
    private static final String DESTROY_BATCH_DIR_COMMAND = "rm -r \"%s\"";
    private static final String CREATE_VOLUME_COMMAND = "mkdir -m %s \"%s\"";
    private static final String TEST_FILE_EXISTS_COMMAND = "if [ -f \"%s\" ]; then echo true; else echo false; fi";

    private static final List<PosixFilePermission> DEFAULT_FILE_PERMISSIONS = List.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.GROUP_READ);

    private static final List<PosixFilePermission> DEFAULT_DIRECTORY_PERMISSIONS = List.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE,
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.GROUP_EXECUTE);

    private static final String DEFAULT_DIRECTORY_MODE = "750";

    private static final List<String> TRAC_CMD_ARGS = List.of("-m", "tracdap.rt.launch");

    private static final String BATCH_FAILED_MESSAGE = "SSH batch terminated with non-zero exit code [%d]";

    private static final List<Feature> EXECUTOR_FEATURES = List.of(Feature.OUTPUT_VOLUMES);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Properties properties;
    private final SshClient client;
    private final Map<String, SessionState> sessionMap;

    private final String venvPath;
    private final String batchRootDir;
    private final boolean batchPersist;

    private final List<KeyPair> sshKeyPairs;


    public SshExecutor(Properties properties, ConfigManager configManager) {

        this.properties = properties;
        this.client = SshClient.setUpDefaultClient();
        this.sessionMap = new ConcurrentHashMap<>();

        venvPath = requiredProperty(CONFIG_VENV_PATH);
        batchRootDir = requiredProperty(CONFIG_BATCH_DIR);
        batchPersist = properties.containsKey(CONFIG_BATCH_PERSIST) &&
                Boolean.parseBoolean(requiredProperty(CONFIG_BATCH_PERSIST));

        sshKeyPairs = loadSshKeys(configManager);
    }

    private List<KeyPair> loadSshKeys(ConfigManager configManager) {

        try {

            var keyFile = requiredProperty(KEY_FILE_KEY);
            var keyData = configManager.loadTextConfig(keyFile);

            var keyLoader = SecurityUtils.getKeyPairResourceParser();
            var keyPairs = keyLoader.loadKeyPairs(null, NamedResource.ofName("executor ssh key"), null, keyData);

            return new ArrayList<>(keyPairs);
        }
        catch (GeneralSecurityException | IOException e) {
            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    public void start() {

        try {

            log.info("SSH executor is starting up...");

            // It is not clear what Apache SSHD does with its event loop group
            // Is it blocking operations, or non-block low-level IO? (or non-block operations)
            // If the wrong loops are used for the wrong things this will cause problems!
            // So, using the default for now

            var ioService = new NettyIoServiceFactoryFactory();
            client.setIoServiceFactoryFactory(ioService);
            client.setKeyIdentityProvider(KeyIdentityProvider.wrapKeyPairs(sshKeyPairs));

            client.start();
        }
        catch (Exception e) {
            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    public void stop() {

        log.info("SSH executor is shutting down...");

        client.stop();
    }

    @Override
    public boolean hasFeature(Feature feature) {
        return EXECUTOR_FEATURES.contains(feature);
    }

    @Override
    public SshExecutorState createBatch(String batchKey) {

        try {

            log.info("SSH EXECUTOR createBatch() [{}]", batchKey);

            var session = allocateSession(batchKey);

            var remoteAddress = (InetSocketAddress) session.getConnectAddress();
            var remoteHost = remoteAddress.getHostString();
            var remotePort = remoteAddress.getPort();
            var batchUser = session.getUsername();

            var batchDir = buildBatchPath(batchKey);
            var command = String.format(CREATE_BATCH_DIR_COMMAND, DEFAULT_DIRECTORY_MODE, batchDir);

            session.executeRemoteCommand(command);

            var volumesDir = batchDir + "/volumes";
            var volumesCmd = String.format(CREATE_BATCH_DIR_COMMAND, DEFAULT_DIRECTORY_MODE, volumesDir);

            session.executeRemoteCommand(volumesCmd);

            return new SshExecutorState(remoteHost, remotePort, batchUser, batchDir);
        }
        catch (IOException e) {

            log.info("Got an error");

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed to create executor batch [%s]: %s", batchKey, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }
    }

    @Override
    public SshExecutorState addVolume(String batchKey, SshExecutorState batchState, String volumeName, BatchVolumeType volumeType) {

        try {

            log.info("SSH EXECUTOR addVolume() [{}, {}]", batchKey, volumeName);

            // TODO: Check existing volumes?

            var session = getSession(batchState);
            var volumePath = buildVolumePath(batchState, volumeName);
            var command = String.format(CREATE_VOLUME_COMMAND, DEFAULT_DIRECTORY_MODE, volumePath);

            session.executeRemoteCommand(command);

            return batchState.withVolume(volumeName);
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed to create executor volume [%s]: %s", volumeName, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }
    }

    @Override
    public SshExecutorState addFile(
            String batchKey, SshExecutorState batchState,
            String volumeName, String fileName,
            byte[] fileContent) {

        log.info("SSH EXECUTOR addFile() [{}, {}, {}]", batchKey, volumeName, fileName);

        return writeFile(
                batchState, volumeName, fileName, fileContent,
                DEFAULT_DIRECTORY_PERMISSIONS);
    }

    private SshExecutorState writeFile(
            SshExecutorState batchState, String volumeName, String fileName, byte[] fileContent,
            List<PosixFilePermission> posixPermissions) {

        try {

            var scp = getSessionScp(batchState);
            var remotePath = buildRemotePath(batchState, volumeName, fileName);

            // Set create, modify and access times all to the upload time
            var timestampEpoch = Instant.now().getEpochSecond();
            var timestampCmd = String.format("T%d %d %d", timestampEpoch, timestampEpoch, timestampEpoch);
            var timestamp = ScpTimestampCommandDetails.parse(timestampCmd);

            scp.upload(fileContent, remotePath, posixPermissions, timestamp);

            // No need to update state object for individual files
            return batchState;
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed sending file to executor [%s]: %s", fileName, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }

        // No need to close scp client, not a closable resource and session stays open
    }

    @Override
    public SshExecutorState submitBatch(String batchKey, SshExecutorState batchState, BatchConfig batchConfig) {

        try {

            log.info("SSH EXECUTOR submitBatch() [{}]", batchKey);

            var launchCmd = batchConfig.getLaunchCmd();
            var launchArgs = batchConfig.getLaunchArgs();

            var session = getSession(batchState);
            var command = buildBatchCommand(batchKey, batchState, launchCmd, launchArgs);

            if (!batchState.getVolumes().contains("log"))
                throw new ETracInternal("Executor log volume has not been configured");

            var batchAdminDir = buildVolumePath(batchState, "trac_admin");

            String stdOutLog;
            String stdErrLog;

            if (batchConfig.isRedirectOutput()) {
                stdOutLog = buildRemotePath(batchState, "log", "trac_rt_stdout.txt");
                stdErrLog = buildRemotePath(batchState, "log", "trac_rt_stderr.txt");
            }
            else {
                stdOutLog = "";
                stdErrLog = "";
            }

            // Setting environment variables as part of the command causes issues
            // Running VAR=value ./script.sh & does not properly background the process
            // As a work-around, substitute the variables directly into the script before sending

            // It may be possible to use an SSH exec shell and set up env vars that way
            // Didn't work on first try, but it seems like it should work the way you'd expect...

            var launchScriptContent = LAUNCH_SCRIPT
                    .replace("${BATCH_ADMIN_DIR}", batchAdminDir)
                    .replace("${BATCH_STDOUT}", stdOutLog)
                    .replace("${BATCH_STDERR}", stdErrLog)
                    .getBytes(StandardCharsets.UTF_8);

            var pollScriptContent = POLL_SCRIPT
                    .replace("${BATCH_ADMIN_DIR}", batchAdminDir)
                    .getBytes(StandardCharsets.UTF_8);

            var executePermissions = new ArrayList<>(DEFAULT_FILE_PERMISSIONS);
            executePermissions.add(PosixFilePermission.OWNER_EXECUTE);
            executePermissions.add(PosixFilePermission.GROUP_EXECUTE);

            batchState = addVolume(batchKey, batchState, "trac_admin", BatchVolumeType.SCRATCH_VOLUME);
            writeFile(batchState, "trac_admin", LAUNCH_SCRIPT_NAME, launchScriptContent, executePermissions);
            writeFile(batchState, "trac_admin", POLL_SCRIPT_NAME, pollScriptContent, executePermissions);

            var launchScript = buildRemotePath(batchState, "trac_admin", LAUNCH_SCRIPT_NAME);
            String launchCommand = launchScript + " " + command;

            log.info("Launch command: {}", command);

            session.executeRemoteCommand(launchCommand);

            var pidFile = buildRemotePath(batchState, "trac_admin", "pid");
            var pidCommand = String.format("cat %s", pidFile);
            var pidText = session.executeRemoteCommand(pidCommand);
            var pid = tryParseLong(pidText, "Start batch failed, invalid value for [pid]");

            return batchState.withPid(pid);
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed to start executor batch [%s]: %s", batchKey, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }
    }

    @Override
    public SshExecutorState cancelBatch(String batchKey, SshExecutorState batchState) {

        // This should never be called, the executor does not advertise cancellation in its features
        throw new ETracInternal("SSH executor does not support batch cancellation");
    }

    @Override
    public void deleteBatch(String batchKey, SshExecutorState batchState) {

        boolean processDown = false;

        try {

            log.info("SSH EXECUTOR deleteBatch() [{}]", batchKey);

            var session = getSession(batchState);

            // In case the process is still running, ensure that it is shut down (avoid orphan batches)
            var killCommand = String.format(DESTROY_BATCH_PROCESS_COMMAND, batchState.getPid(), batchState.getPid());
            session.executeRemoteCommand(killCommand);

            processDown = true;

            var batchDir = buildBatchPath(batchKey);

            if (batchPersist) {
                log.warn("Batch-persist is enabled, batch dir will not be removed: [{}]", batchDir);
            }
            else {
                var batchDirCommand = String.format(DESTROY_BATCH_DIR_COMMAND, batchDir);
                session.executeRemoteCommand(batchDirCommand);
            }
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;

            if (!processDown) {
                var message = String.format("Failed to shut down executor batch [%s]: %s", batchKey, cause.getMessage());
                log.error(message, cause);
                throw new EExecutorFailure(message, cause);
            }
            else {
                var message = String.format("Executor batch completed but was not shut down cleanly [%s]: %s", batchKey, cause.getMessage());
                log.warn(message, cause);
            }
        }
    }

    @Override
    public BatchStatus getBatchStatus(String batchKey, SshExecutorState batchState) {

        try {

            if (log.isTraceEnabled())
                log.trace("SSH EXECUTOR getBatchStatus() [{}]", batchKey);

            var session = getSession(batchState);

            // See startBatch(), env vars are already set in the poll script before it was sent

            var pollScript = buildRemotePath(batchState, "trac_admin", POLL_SCRIPT_NAME);
            var pollOutput = session.executeRemoteCommand(pollScript);
            var pollResponse = new HashMap<String, String>();

            for (var line : pollOutput.split("\n")) {
                var sep = line.indexOf(":");
                var key = line.substring(0, sep);
                var value = line.substring(sep + 1).trim();
                pollResponse.put(key, value);
            }

            var ok = pollResponse.get("trac_poll_ok");
            var pid = tryParseLong(pollResponse.get("pid"), "Invalid poll response for [pid");
            var running = tryParseLong(pollResponse.get("running"), "Invalid poll response for [running]");

            if (ok == null || !ok.equals("ok") || pid != batchState.getPid()) {
                throw new EExecutorFailure("Invalid poll response");
            }

            if (running == 0)
                return new BatchStatus(BatchStatusCode.RUNNING);

            var exitCode = (int)(long) tryParseLong(pollResponse.get("exit_code"), "Invalid poll response for [exit_code]");

            if (exitCode == 0)
                return new BatchStatus(BatchStatusCode.SUCCEEDED);

            // Job has failed - set a generic failure message in the status
            // Batch executor is generic and only knows about generic processes
            // The TRAC job executor can try to get more meaningful info from the logs

            var statusMessage = String.format(BATCH_FAILED_MESSAGE, exitCode);

            return new BatchStatus(BatchStatusCode.FAILED, statusMessage);
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed polling executor batch [%s]: %s", batchKey, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }
    }

    @Override
    public boolean hasOutputFile(String batchKey, SshExecutorState batchState, String volumeName, String fileName) {

        try {

            log.info("SSH EXECUTOR hasOutputFile() [{}, {}, {}]", batchKey, volumeName, fileName);

            var remotePath = buildRemotePath(batchState, volumeName, fileName);
            var command = String.format(TEST_FILE_EXISTS_COMMAND, remotePath);

            var session = getSession(batchState);
            var result = session.executeRemoteCommand(command);

            return result.trim().equals("true");
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed getting file from executor [%s]: %s", fileName, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }
    }

    @Override
    public byte[] getOutputFile(String batchKey, SshExecutorState batchState, String volumeName, String fileName) {

        try {

            log.info("SSH EXECUTOR getOutputFile() [{}, {}, {}]", batchKey, volumeName, fileName);

            var scp = getSessionScp(batchState);
            var remotePath = buildRemotePath(batchState, volumeName, fileName);

            // Perform SCP download
            return scp.downloadBytes(remotePath);
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed getting file from executor [%s]: %s", fileName, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }

        // No need to close scp client, not a closable resource and session stays open
    }

    @Override
    public InetSocketAddress getBatchAddress(String batchKey, SshExecutorState batchState) {

        // This should never be called, the executor does not advertise expose_port in its features
        throw new ETracInternal("SSH executor does not support expose_port");
    }

    @Override
    public SshExecutorState configureBatchStorage(
            String batchKey, SshExecutorState batchState,
            StorageConfig storageConfig, Consumer<StorageConfig> storageUpdate) {

        // This should never be called, the executor does not advertise storage_mapping in its features
        throw new ETracInternal("SSH executor does not support storage_mapping");
    }

    private ClientSession allocateSession(String jobKey) throws IOException {

        var remoteHost = requiredProperty(REMOTE_HOST_KEY);
        var remotePort = requiredProperty(REMOTE_PORT_KEY);
        var batchUser = requiredProperty(BATCH_USER_KEY);

        var port = (int) (long) tryParseLong(remotePort, "Invalid value of [" + REMOTE_PORT_KEY + "] in SSH executor config");

        return getSession(remoteHost, port, batchUser).session;
    }

    private ClientSession getSession(SshExecutorState batchState) throws IOException {

        var remoteHost = batchState.getRemoteHost();
        var remotePort = (short) batchState.getRemotePort();
        var batchUser = batchState.getBatchUser();

        return getSession(remoteHost, remotePort, batchUser).session;
    }

    private ScpClient getSessionScp(SshExecutorState batchState) throws IOException {

        var remoteHost = batchState.getRemoteHost();
        var remotePort = (short) batchState.getRemotePort();
        var batchUser = batchState.getBatchUser();

        return getSession(remoteHost, remotePort, batchUser).scpClient;
    }

    private SessionState getSession(String remoteHost, int remotePort, String batchUser) throws IOException {

        var sessionKey = String.format("%s:%d:%s", remoteHost, remotePort, batchUser);

        var state = sessionMap.get(sessionKey);

        // Use an existing session if possible, but check in case it is already closed
        if (state != null) {
            if (state.session.getSessionState().contains(ClientSession.ClientSessionEvent.CLOSED))
                sessionMap.remove(sessionKey, state);
            else
                return state;
        }

        return createSession(remoteHost, remotePort, batchUser);
    }

    private SessionState createSession(String remoteHost, int remotePort, String batchUser) throws IOException {

        var sessionKey = String.format("%s:%d:%s", remoteHost, remotePort, batchUser);
        var timeout = Duration.ofSeconds(3);

        var newSession = client
                .connect(batchUser, remoteHost, remotePort)
                .verify(timeout)
                .addListener(this::logSessionConnect)
                .getSession();

        newSession.auth().verify(Duration.ofSeconds(5));

        var newState = new SessionState();
        newState.session = newSession;
        newState.scpClient = ScpClientCreator.instance().createScpClient(newSession);

        var existingState = sessionMap.putIfAbsent(sessionKey, newState);

        if (existingState != null) {
            newSession.close(false);
            return existingState;
        }

        newSession.addCloseFutureListener(future -> cleanUpSession(sessionKey, newState));

        return newState;
    }

    private CompletionStage<Boolean> cleanUpSession(String sessionKey, SessionState state) {

        sessionMap.remove(sessionKey, state);

        if (state.session.getSessionState().contains(ClientSession.ClientSessionEvent.CLOSED))
            return CompletableFuture.completedFuture(true);

        var close = state.session.close(false);
        var future = new CompletableFuture<Boolean>();

        close.addListener(result -> future.complete(result.isClosed()));

        return future;
    }

    private void logSessionConnect(ConnectFuture connect) {

        if (connect.isConnected()) {
            log.info("CONNECT OK [{}@{}]",
                    connect.getSessionContext().getUsername(),
                    connect.getSessionContext().getRemoteAddress());
        }

        else if (connect.isCanceled()) {
            log.warn("CONNECT CANCELLED [{}]", connect.getSessionContext().getRemoteAddress());
        }

        else {
            log.error("CONNECT FAILED [{}]: {}",
                    connect.getSessionContext().getRemoteAddress(),
                    connect.getException().getMessage(),
                    connect.getException());
        }
    }


    private String buildBatchPath(String batchKey) {

        var batchPath = new StringBuilder();
        batchPath.append(batchRootDir);

        if (!batchRootDir.endsWith("/"))
            batchPath.append("/");

        // TODO: Get batch time from the job
        var batchTime = Instant.now().atZone(ZoneOffset.UTC).toLocalDate();
        var batchYear = batchTime.getYear();
        var batchDate = DateTimeFormatter.ISO_DATE.format(batchTime);

        batchPath.append(batchYear);
        batchPath.append("/");
        batchPath.append(batchDate);
        batchPath.append("/");
        batchPath.append(batchKey);

        return batchPath.toString();
    }

    private String buildVolumePath(SshExecutorState batchState, String volumeName) {

        var volumePath = new StringBuilder();
        volumePath.append(batchState.getBatchDir());

        if (!batchState.getBatchDir().endsWith("/"))
            volumePath.append("/");

        volumePath.append("volumes/");
        volumePath.append(volumeName);

        return volumePath.toString();
    }

    private String buildRemotePath(SshExecutorState batchState, String volumeName, String fileName) {

        return buildVolumePath(batchState, volumeName) + "/" + fileName;
    }

    private String buildBatchCommand(String batchKey, SshExecutorState batchState, LaunchCmd launchCmd, List<LaunchArg> launchArgs) {

        var processArgs = new ArrayList<String>();

        if (launchCmd.isTrac()) {
            var pythonExe = venvPath + (venvPath.endsWith("/") ? "" : "/") + "bin/python";
            processArgs.add(pythonExe);
            processArgs.addAll(TRAC_CMD_ARGS);
        }
        else {
            processArgs.add(launchCmd.command());
            launchCmd.commandArgs().stream()
                    .map(arg -> decodeLaunchArg(arg, batchState))
                    .forEach(processArgs::add);
        }

        var decodedArgs = launchArgs.stream()
                .map(arg -> decodeLaunchArg(arg, batchState))
                .collect(Collectors.toList());

        processArgs.addAll(decodedArgs);

        return String.join(" ", processArgs);
    }

    private String decodeLaunchArg(LaunchArg arg, SshExecutorState batchState) {

        switch (arg.getArgType()) {

            case STRING:
                return arg.getStringArg();

            case PATH:

                var volume = arg.getPathVolume();

                if (!batchState.getVolumes().contains(volume)) {
                    var errorMsg = String.format("Requested volume does not exist: [%s]", volume);
                    log.error(errorMsg);
                    throw new ETracInternal(errorMsg);
                }

                return buildRemotePath(batchState, volume, arg.getPathArg());

            default:

                throw new EUnexpected();  // TODO
        }
    }

    private String requiredProperty(String propertyName) {

        var propertyValue = properties.getProperty(propertyName);

        if (propertyValue == null || propertyValue.trim().equals("")) {
            var message = String.format("Missing required property [%s] for SSH executor", propertyName);
            throw new EStartup(message);
        }

        return propertyValue;
    }

    private Long tryParseLong(String text, String errorMessage) {

        try {
            return Long.parseLong(text.trim());
        }
        catch (NumberFormatException e) {
            var message = String.format("%s (%s)", errorMessage, e.getMessage());
            throw new EExecutorFailure(message, e);
        }
    }

    private static class SessionState {

        ClientSession session;
        ScpClient scpClient;
    }
}
