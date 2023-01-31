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

import org.apache.sshd.scp.client.ScpClient;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.exec.*;
import org.finos.tracdap.common.util.ResourceHelpers;
import org.finos.tracdap.metadata.JobStatusCode;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.NamedResource;
import org.apache.sshd.common.keyprovider.KeyIdentityProvider;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.apache.sshd.netty.NettyIoServiceFactoryFactory;
import org.apache.sshd.scp.client.ScpClientCreator;
import org.apache.sshd.scp.common.helpers.ScpTimestampCommandDetails;

import com.google.protobuf.Parser;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class SshExecutor implements IBatchExecutor<SshBatchState> {

    public static final String REMOTE_HOST_KEY = "remoteHost";
    public static final String REMOTE_PORT_KEY = "remotePort";
    public static final String BATCH_USER_KEY = "batchUser";
    public static final String KEY_FILE_KEY = "keyFile";
    public static final String CONFIG_VENV_PATH = "venvPath";
    public static final String CONFIG_BATCH_DIR = "batchDir";
    public static final String CONFIG_BATCH_PERSIST = "batchPersist";

    private static final String LAUNCH_SCRIPT_NAME = "launch_batch.sh";
    private static final String POLL_SCRIPT_NAME = "poll_batch.sh";
    private static final byte[] LAUNCH_SCRIPT = ResourceHelpers.loadResourceAsBytes("/scripts/launch_batch.sh", SshExecutor.class);
    private static final byte[] POLL_SCRIPT = ResourceHelpers.loadResourceAsBytes("/scripts/poll_batch.sh", SshExecutor.class);
    // private static final String POLL_EXECUTOR_COMMAND = "ps -a | grep launch_batch.sh | grep -v \"grep launch_batch.sh\"";

    private static final String CREATE_BATCH_DIR_COMMAND = "mkdir -p -m %s \"%s\"";
    private static final String DESTROY_BATCH_PROCESS_COMMAND = "if ps -p %d; then kill -s KILL %d; fi";
    private static final String DESTROY_BATCH_DIR_COMMAND = "rm -r \"%s\"";
    private static final String CREATE_VOLUME_COMMAND = "mkdir -m %s \"%s\"";

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
    private static final Pattern TRAC_ERROR_LINE = Pattern.compile("tracdap.rt.exceptions.(E\\w+): (.+)");

    private static final String FALLBACK_ERROR_MESSAGE = "Local batch terminated with non-zero exit code [%d]";
    private static final String FALLBACK_ERROR_DETAIL = "No details available";

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

        client.stop();
    }

    @Override
    public void executorStatus() {
        // not used
    }

    @Override
    public Parser<SshBatchState> stateDecoder() {
        return SshBatchState.parser();
    }

    @Override
    public SshBatchState createBatch(String batchKey) {

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

            return SshBatchState.newBuilder()
                    .setRemoteHost(remoteHost)
                    .setRemotePort(remotePort)
                    .setBatchUser(batchUser)
                    .setBatchDir(batchDir)
                    .build();
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
    public void destroyBatch(String batchKey, SshBatchState batchState) {

        boolean processDown = false;

        try {

            log.info("SSH EXECUTOR destroyBatch() [{}]", batchKey);

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
    public SshBatchState createVolume(String batchKey, SshBatchState batchState, String volumeName, ExecutorVolumeType volumeType) {

        try {

            log.info("SSH EXECUTOR createVolume() [{}, {}]", batchKey, volumeName);

            // TODO: Check existing volumes?

            var session = getSession(batchState);
            var volumePath = buildVolumePath(batchState, volumeName);
            var command = String.format(CREATE_VOLUME_COMMAND, DEFAULT_DIRECTORY_MODE, volumePath);

            session.executeRemoteCommand(command);

            return batchState.toBuilder()
                    .addVolumes(volumeName)
                    .build();
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed to create executor volume [%s]: %s", volumeName, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }
    }

    @Override
    public SshBatchState writeFile(
            String batchKey, SshBatchState batchState,
            String volumeName, String fileName,
            byte[] fileContent) {

        return writeFile(
                batchKey, batchState,
                volumeName, fileName, fileContent,
                DEFAULT_DIRECTORY_PERMISSIONS);
    }

    private SshBatchState writeFile(
            String batchKey, SshBatchState batchState,
            String volumeName, String fileName,
            byte[] fileContent, List<PosixFilePermission> posixPermissions) {

        try {

            log.info("SSH EXECUTOR writeFile() [{}, {}, {}]", batchKey, volumeName, fileName);

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
    public byte[] readFile(String batchKey, SshBatchState batchState, String volumeName, String fileName) {

        try {

            log.info("SSH EXECUTOR readFile() [{}, {}, {}]", batchKey, volumeName, fileName);

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
    public SshBatchState startBatch(String batchKey, SshBatchState batchState, LaunchCmd launchCmd, List<LaunchArg> launchArgs) {

        try {

            log.info("SSH EXECUTOR startBatch() [{}]", batchKey);

            var session = getSession(batchState);
            var command = buildBatchCommand(batchKey, batchState, launchCmd, launchArgs);

            if (!batchState.getVolumesList().contains("log"))
                throw new ETracInternal("Executor log volume has not been configured");

            var executePermissions = new ArrayList<>(DEFAULT_FILE_PERMISSIONS);
            executePermissions.add(PosixFilePermission.OWNER_EXECUTE);
            executePermissions.add(PosixFilePermission.GROUP_EXECUTE);

            batchState = createVolume(batchKey, batchState, "trac_admin", ExecutorVolumeType.SCRATCH_DIR);
            writeFile(batchKey, batchState, "trac_admin", LAUNCH_SCRIPT_NAME, LAUNCH_SCRIPT, executePermissions);
            writeFile(batchKey, batchState, "trac_admin", POLL_SCRIPT_NAME, POLL_SCRIPT, executePermissions);

            var batchAdminDir = buildVolumePath(batchState, "trac_admin");
            var stdOutLog = buildRemotePath(batchState, "log", "trac_rt_stdout.txt");
            var stdErrLog = buildRemotePath(batchState, "log", "trac_rt_stderr.txt");

            var launchScript = buildRemotePath(batchState, "trac_admin", LAUNCH_SCRIPT_NAME);
            var launchCommand = new StringBuilder();
            launchCommand.append("TRAC_BATCH_ADMIN_DIR=");
            launchCommand.append(batchAdminDir);
            launchCommand.append(" TRAC_BATCH_STDOUT=");
            launchCommand.append(stdOutLog);
            launchCommand.append(" TRAC_BATCH_STDERR=");
            launchCommand.append(stdErrLog);
            launchCommand.append(" ");
            launchCommand.append(launchScript);
            launchCommand.append(" ");
            launchCommand.append(command);
            launchCommand.append(" &");

            log.info("Launch command: {}", command);

            session.executeRemoteCommand(launchCommand.toString());

            var pidFile = buildRemotePath(batchState, "trac_admin", "pid");
            var pidCommand = String.format("cat %s", pidFile);
            var pidText = session.executeRemoteCommand(pidCommand);
            var pid = tryParseLong(pidText, "Start batch failed, invalid value for [pid]");

            return batchState.toBuilder()
                    .setPid(pid)
                    .build();
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed to start executor batch [%s]: %s", batchKey, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }


    }

    @Override
    public SshBatchState cancelBatch(String batchKey, SshBatchState batchState) {

        throw new ETracInternal("Batch cancellation not available yet for SSH executor");
    }

    @Override
    public ExecutorPollResult<SshBatchState> pollBatch(String batchKey, SshBatchState batchState) {

        try {

            if (log.isTraceEnabled())
                log.trace("SSH EXECUTOR pollBatch() [{}]", batchKey);

            var session = getSession(batchState);

            var batchAdminDir = buildVolumePath(batchState, "trac_admin");

            var pollScript = buildRemotePath(batchState, "trac_admin", POLL_SCRIPT_NAME);
            var pollCommand = new StringBuilder();
            pollCommand.append("TRAC_BATCH_ADMIN_DIR=");
            pollCommand.append(batchAdminDir);
            pollCommand.append(" ");
            pollCommand.append(pollScript);

            var pollOutput = session.executeRemoteCommand(pollCommand.toString());
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

            var result = new ExecutorPollResult<SshBatchState>();
            result.jobKey = batchKey;
            result.batchState = batchState;

            if (running == 0) {
                result.statusCode = JobStatusCode.RUNNING;
            }
            else {
                var exitCode = (int)(long) tryParseLong(pollResponse.get("exit_code"), "Invalid poll response for [exit_code]");
                result.statusCode = exitCode == 0 ? JobStatusCode.SUCCEEDED : JobStatusCode.FAILED;

                if (result.statusCode == JobStatusCode.FAILED) {

                    try {

                        var errorBytes = readFile(batchKey, batchState, "log", "trac_rt_stderr.txt");
                        var errorDetail = new String(errorBytes, StandardCharsets.UTF_8);

                        result.statusMessage = extractErrorMessage(errorDetail, exitCode);
                        result.errorDetail = errorDetail;
                    }
                    catch (EExecutorFailure e) {

                        result.statusMessage = String.format(FALLBACK_ERROR_MESSAGE, exitCode);
                        result.errorDetail = FALLBACK_ERROR_DETAIL;
                    }
                }
            }

            return result;
        }
        catch (IOException e) {

            var cause = e.getCause() instanceof ServerException ? e.getCause() : e;
            var message = String.format("Failed polling executor batch [%s]: %s", batchKey, cause.getMessage());

            log.error(message, cause);
            throw new EExecutorFailure(message, cause);
        }
    }

    @Override
    public List<ExecutorPollResult<SshBatchState>> pollAllBatches(Map<String, SshBatchState> priorStates) {

        var updates = new ArrayList<ExecutorPollResult<SshBatchState>>();

        for (var job : priorStates.entrySet()) {

            try {

                var priorState = job.getValue();
                var pollResult = pollBatch(job.getKey(), priorState);

                if (pollResult.statusCode == JobStatusCode.SUCCEEDED || pollResult.statusCode == JobStatusCode.FAILED)
                    updates.add(pollResult);
            }
            catch (Exception e) {

                log.warn("Failed to poll job: [{}] {}", job.getKey(), e.getMessage(), e);
            }
        }

        return updates;
    }


    private ClientSession allocateSession(String jobKey) throws IOException {

        var remoteHost = requiredProperty(REMOTE_HOST_KEY);
        var remotePort = requiredProperty(REMOTE_PORT_KEY);
        var batchUser = requiredProperty(BATCH_USER_KEY);

        var port = (int) (long) tryParseLong(remotePort, "Invalid value of [" + REMOTE_PORT_KEY + "] in SSH executor config");

        return getSession(remoteHost, port, batchUser).session;
    }

    private ClientSession getSession(SshBatchState batchState) throws IOException {

        var remoteHost = batchState.getRemoteHost();
        var remotePort = (short) batchState.getRemotePort();
        var batchUser = batchState.getBatchUser();

        return getSession(remoteHost, remotePort, batchUser).session;
    }

    private ScpClient getSessionScp(SshBatchState batchState) throws IOException {

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

    private String buildVolumePath(SshBatchState batchState, String volumeName) {

        var volumePath = new StringBuilder();
        volumePath.append(batchState.getBatchDir());

        if (!batchState.getBatchDir().endsWith("/"))
            volumePath.append("/");

        volumePath.append("volumes/");
        volumePath.append(volumeName);

        return volumePath.toString();
    }

    private String buildRemotePath(SshBatchState batchState, String volumeName, String fileName) {

        return buildVolumePath(batchState, volumeName) + "/" + fileName;
    }

    private String buildBatchCommand(String batchKey, SshBatchState batchState, LaunchCmd launchCmd, List<LaunchArg> launchArgs) {



        var processArgs = new ArrayList<String>();

        if (launchCmd.isTrac()) {
            var pythonExe = venvPath + (venvPath.endsWith("/") ? "" : "/") + "bin/python";
            processArgs.add(pythonExe);
            processArgs.addAll(TRAC_CMD_ARGS);
        }
        else {
            processArgs.add(launchCmd.customCommand());
            launchCmd.customArgs().stream()
                    .map(arg -> decodeLaunchArg(arg, batchState))
                    .forEach(processArgs::add);
        }

        var decodedArgs = launchArgs.stream()
                .map(arg -> decodeLaunchArg(arg, batchState))
                .collect(Collectors.toList());

        processArgs.addAll(decodedArgs);

        return String.join(" ", processArgs);
    }

    private String decodeLaunchArg(LaunchArg arg, SshBatchState batchState) {

        switch (arg.getArgType()) {

            case STRING:
                return arg.getStringArg();

            case PATH:

                var volume = arg.getPathVolume();

                if (!batchState.getVolumesList().contains(volume)) {
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

    private String extractErrorMessage(String errorDetail, int exitCode) {

        // TODO: Better way of reporting runtime errors

        var lastLineIndex = errorDetail.stripTrailing().lastIndexOf("\n");
        var lastLine = errorDetail.substring(lastLineIndex + 1).stripTrailing();

        var tracError = TRAC_ERROR_LINE.matcher(lastLine);

        if (tracError.matches()) {

            var exception = tracError.group(1);
            var message = tracError.group(2);

            log.error("Runtime error [{}]: {}", exception, message);
            return message;
        }
        else {

            return String.format(FALLBACK_ERROR_MESSAGE, exitCode);
        }
    }

    private static class SessionState {

        ClientSession session;
        ScpClient scpClient;
    }
}
