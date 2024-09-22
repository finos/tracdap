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

package org.finos.tracdap.common.exec.local;

import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.exec.*;
import org.finos.tracdap.common.metadata.MetadataConstants;

import org.finos.tracdap.config.StorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public class LocalBatchExecutor implements IBatchExecutor<LocalBatchState> {

    public static final String CONFIG_VENV_PATH = "venvPath";
    public static final String CONFIG_BATCH_DIR = "batchDir";
    public static final String CONFIG_BATCH_PERSIST = "batchPersist";

    public static final String PROCESS_USERNAME_PROPERTY = "user.name";

    private static final String JOB_DIR_TEMPLATE = "tracdap_%s_";

    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
    private static final String PYTHON_EXE = IS_WINDOWS ? "python.exe" : "python";
    private static final String VENV_BIN_SUBDIR = IS_WINDOWS ? "Scripts" : "bin";
    private static final String VENV_ENV_VAR = "VIRTUAL_ENV";
    private static final List<String> TRAC_CMD_ARGS = List.of("-m", "tracdap.rt.launch");

    private static final String BATCH_FAILED_MESSAGE = "Local batch terminated with non-zero exit code [%d]";

    // Do not enable the runtime API for local batches yet (i.e. stick with the current behavior)
    private static final List<Feature> EXECUTOR_FEATURES = List.of(Feature.OUTPUT_VOLUMES);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Path tracRuntimeVenv;
    private final Path batchRootDir;
    private final boolean batchPersist;
    private final String batchUser;

    private final Map<Long, Process> processMap = new HashMap<>();
    private final Set<Long> completionLog = new HashSet<>();

    public LocalBatchExecutor(Properties properties) {

        this.tracRuntimeVenv = prepareVenvPath(properties);
        this.batchRootDir = prepareBatchRootDir(properties);
        this.batchPersist = prepareBatchPersist(properties);
        this.batchUser = lookupBatchUser();
    }

    @Override
    public void start() {

        log.info("Local executor is starting up...");
    }

    @Override
    public void stop() {

        log.info("Local executor is shutting down...");
    }

    @Override
    public boolean hasFeature(Feature feature) {
        return EXECUTOR_FEATURES.contains(feature);
    }

    @Override
    public LocalBatchState createBatch(String jobKey) {

        try {

            var batchDirPrefix = String.format(JOB_DIR_TEMPLATE, jobKey.toLowerCase());
            var batchDir = (batchRootDir != null)
                    ? Files.createTempDirectory(batchRootDir, batchDirPrefix)
                    : Files.createTempDirectory(batchDirPrefix);

            var batchDirOwner = Files.getOwner(batchDir);

            if (batchUser != null && batchDirOwner != null && !batchUser.equals(batchDirOwner.getName())) {

                fixSandboxPermissions(batchDir);
                batchDirOwner = Files.getOwner(batchDir);
            }

            var batchState = new LocalBatchState(batchDir.toString());

            log.info("Job [{}] sandbox directory created: [{}], owner = [{}]", jobKey, batchDir, batchDirOwner);

            return batchState;
        }
        catch (IOException e) {

            var errorMessage = String.format(
                    "Job [%s] failed to create sandbox directory: %s",
                    jobKey, e.getMessage());

            log.error(errorMessage, e);
            throw new EExecutorFailure(errorMessage, e);
        }
    }

    @Override
    public LocalBatchState addVolume(String jobKey, LocalBatchState state, String volumeName, BatchVolumeType volumeType) {

        try {

            var batchState = validState(state);

            var isValid = MetadataConstants.VALID_IDENTIFIER.matcher(volumeName);
            var isReserved = MetadataConstants.TRAC_RESERVED_IDENTIFIER.matcher(volumeName);

            if (!isValid.matches()) {

                var errorMsg = String.format(
                        "Job [%s] requested volume name is not a valid identifier: [%s]",
                        jobKey, volumeName);

                log.error(errorMsg);
                throw new EExecutorValidation(errorMsg);
            }

            if (isReserved.matches()) {

                var errorMsg = String.format(
                        "Job [%s] requested volume name is a reserved identifier: [%s]",
                        jobKey, volumeName);

                log.error(errorMsg);
                throw new EExecutorValidation(errorMsg);
            }

            // Since volumeName is an identifier, it cannot be an absolute path

            var batchDir = Path.of(batchState.getBatchDir());
            var volumeDir = batchDir.resolve(volumeName);

            Files.createDirectory(volumeDir);

            return batchState.withVolume(volumeName);
        }
        catch (AccessDeniedException e) {

            // Permissions errors reported as executor access error

            var errorMessage = String.format(
                    "Job [%s] failed to create batch volume [%s]: %s",
                    jobKey, volumeName, e.getMessage());

            log.error(errorMessage, e);
            throw new EExecutorAccess(errorMessage, e);
        }
        catch (IOException e) {

            var errorMessage = String.format(
                    "Job [%s] failed to create batch volume [%s]: %s",
                    jobKey, volumeName, e.getMessage());

            log.error(errorMessage, e);
            throw new EExecutorFailure(errorMessage, e);
        }
    }

    @Override
    public LocalBatchState addFile(String jobKey, LocalBatchState state, String volumeName, String fileName, byte[] fileContent) {

        try {

            var batchState = validState(state);
            var process = processMap.get(batchState.getPid());

            if (!batchState.getVolumes().contains(volumeName)) {
                var errorMsg = String.format("Requested Volume does not exist: [%s]", volumeName);
                log.error(errorMsg);
                throw new ETracInternal(errorMsg);
            }

            if (process != null ){
                log.error("writeFile() called after process was started");
                throw new ETracInternal("writeFile() called after process was started");
            }

            var batchDir = Path.of(batchState.getBatchDir());
            var volumeDir = batchDir.resolve(volumeName);
            var filePath = volumeDir.resolve(fileName);

            Files.write(filePath, fileContent, StandardOpenOption.CREATE_NEW);

            return batchState;
        }
        catch (IOException e) {

            // Includes FileAlreadyExistsException

            // TODO
            throw new RuntimeException("TODO", e);
        }
    }

    @Override
    public LocalBatchState submitBatch(String jobKey, LocalBatchState jobState, BatchConfig batchConfig) {

        try {

            var launchCmd = batchConfig.getLaunchCmd();
            var launchArgs = batchConfig.getLaunchArgs();

            var batchState = validState(jobState);
            var batchDir = Paths.get(batchState.getBatchDir());

            var pythonExe = tracRuntimeVenv.resolve(VENV_BIN_SUBDIR).resolve(PYTHON_EXE).toString();
            var decodedArgs = launchArgs.stream()
                    .map(arg -> decodeLaunchArg(arg, batchState))
                    .collect(Collectors.toList());

            var processArgs = new ArrayList<String>();

            // Override launch command for TRAC to handle Windows execution environment
            if (launchCmd.isTrac()) {
                processArgs.add(pythonExe);
                processArgs.addAll(TRAC_CMD_ARGS);
            }
            else {
                processArgs.add(launchCmd.command());
                launchCmd.commandArgs().stream()
                        .map(arg -> decodeLaunchArg(arg, batchState))
                        .forEach(processArgs::add);
            }

            processArgs.addAll(decodedArgs);

            if (batchPersist)
                processArgs.add("--scratch-dir-persist");

            var pb = new ProcessBuilder();
            pb.command(processArgs);
            pb.directory(batchDir.toFile());

            var env = pb.environment();
            env.put(VENV_ENV_VAR, tracRuntimeVenv.toString());

            if (batchConfig.isRedirectOutput()) {

                var stdOut = decodeLaunchArg(batchConfig.getStdOut(), batchState);
                var stdOutPath = Paths.get(stdOut);
                pb.redirectOutput(stdOutPath.toFile());

                var stdErr = decodeLaunchArg(batchConfig.getStdErr(), batchState);
                var stdErrPath = Paths.get(stdErr);
                pb.redirectError(stdErrPath.toFile());
            }

            var process = pb.start();

            processMap.put(process.pid(), process);

            logBatchStart(jobKey, pb, process);

            return batchState.withPid(process.pid());
        }
        catch (IOException e) {
            e.printStackTrace();
            // TODO
            throw new RuntimeException("TODO");
        }
    }

    @Override
    public LocalBatchState cancelBatch(String batchKey, LocalBatchState batchState) {

        // This should never be called, the executor does not advertise cancellation in its features
        throw new ETracInternal("Local executor does not support batch cancellation");
    }

    @Override
    public void deleteBatch(String jobKey, LocalBatchState state) {

        var batchState = validState(state);
        var batchDir = Paths.get(batchState.getBatchDir());
        var process = processMap.get(batchState.getPid());

        if (process == null)
            throw new EUnexpected();  // TODO

        if (process.isAlive()) {
            log.warn("Process for job [{}] is not complete, it will be forcibly terminated", jobKey);
            process.destroyForcibly();
        }

        if (!batchPersist) {

            log.info("Cleaning up sandbox directory [{}]...", batchDir);

            try (var files = Files.walk(batchDir)) {

                files.sorted(Comparator.reverseOrder()).forEach(f -> {
                    try { Files.delete(f); }
                    catch (IOException e) { throw new CompletionException(e); }
                });
            }
            catch (IOException e) {
                log.warn("Failed to clean up sandbox directory [{}]: {}", batchDir, e.getMessage(), e);
            }
            catch (CompletionException e) {
                var cause = e.getCause();
                log.warn("Failed to clean up sandbox directory [{}]: {}", batchDir, cause.getMessage(), cause);
            }
        }
        else {

            log.info("Sandbox directory [{}] will be retained", batchDir);
        }

        processMap.remove(batchState.getPid());
        completionLog.remove(batchState.getPid());
    }

    @Override public BatchStatus
    getBatchStatus(String jobKey, LocalBatchState jobState) {

        try {

            var batchState = validState(jobState);
            var process = processMap.get(batchState.getPid());

            if (process == null)
                throw new EUnexpected();  // TODO

            if (process.isAlive())
                return new BatchStatus(BatchStatusCode.RUNNING);

            logBatchComplete(jobKey, process);

            if (process.exitValue() == 0)
                return new BatchStatus(BatchStatusCode.SUCCEEDED);

            // Job has failed - set a generic failure message in the status
            // Batch executor is generic and only knows about generic processes
            // The TRAC job executor can try to get more meaningful info from the logs

            var statusMessage = String.format(BATCH_FAILED_MESSAGE, process.exitValue());

            return new BatchStatus(BatchStatusCode.FAILED, statusMessage);
        }
        catch (RuntimeException e) {
            e.printStackTrace();
            // TODO
            throw new RuntimeException("TODO");
        }
    }

    @Override
    public boolean hasOutputFile(String jobKey, LocalBatchState state, String volumeName, String fileName) {

        var batchState = validState(state);
        var process = processMap.get(batchState.getPid());

        if (!batchState.getVolumes().contains(volumeName)) {
            var errorMsg = String.format("Volume for output file does not exist: [%s]", volumeName);
            log.error(errorMsg);
            throw new ETracInternal(errorMsg);
        }

        if (process == null || process.isAlive()){
            log.error("hasOutputFile() called before process is complete");
            throw new ETracInternal("readFile() called before process is complete");
        }

        var batchDir = Path.of(batchState.getBatchDir());
        var volumeDir = batchDir.resolve(volumeName);
        var filePath = volumeDir.resolve(fileName);

        return Files.exists(filePath);
    }

    @Override
    public byte[] getOutputFile(String jobKey, LocalBatchState state, String volumeName, String fileName) {

        try {

            var batchState = validState(state);
            var process = processMap.get(batchState.getPid());

            if (!batchState.getVolumes().contains(volumeName)) {
                var errorMsg = String.format("Volume for output file does not exist: [%s]", volumeName);
                log.error(errorMsg);
                throw new ETracInternal(errorMsg);
            }

            if (process == null || process.isAlive()){
                log.error("getOutputFile() called before process is complete");
                throw new ETracInternal("getOutputFile() called before process is complete");
            }

            var batchDir = Path.of(batchState.getBatchDir());
            var volumeDir = batchDir.resolve(volumeName);
            var filePath = volumeDir.resolve(fileName);

            return Files.readAllBytes(filePath);
        }
        // TODO: These error messages will not be meaningful to users
        catch (NoSuchFileException e) {
            var message = String.format("Executor read failed for [%s]: File not found", fileName);
            throw new EExecutorFailure(message, e);
        }
        catch (IOException e) {
            var message = String.format("Executor read failed for [%s]: %s", fileName, e.getMessage());
            throw new EExecutorFailure(message, e);
        }
    }

    @Override
    public InetSocketAddress getBatchAddress(String batchKey, LocalBatchState batchState) {

        // This should never be called, the executor does not advertise expose_port in its features
        throw new ETracInternal("Local executor does not support expose_port");
    }

    @Override
    public LocalBatchState configureBatchStorage(
            String batchKey, LocalBatchState batchState,
            StorageConfig storageConfig, Consumer<StorageConfig> storageUpdate) {

        // This should never be called, the executor does not advertise storage_mapping in its features
        throw new ETracInternal("Local executor does not support storage_mapping");
    }

    private Path prepareVenvPath(Properties properties) {

        var venvPath = properties.getProperty(CONFIG_VENV_PATH);

        if (venvPath == null) {

            var err = String.format("Local executor config is missing a required property: [%s]", CONFIG_VENV_PATH);
            log.error(err);
            throw new EStartup(err);
        }

        var tracRuntimeVenv = Paths.get(venvPath)
                .toAbsolutePath()
                .normalize();

        if (!Files.exists(tracRuntimeVenv) || !Files.isDirectory(tracRuntimeVenv)) {
            var err = String.format("Local executor venv path is not a valid directory: [%s]", tracRuntimeVenv);
            log.error(err);
            throw new EStartup(err);
        }

        return tracRuntimeVenv;
    }

    private Path prepareBatchRootDir(Properties properties) {

        var batchDir = properties.getProperty(CONFIG_BATCH_DIR);

        if (batchDir == null)
            return null;

        var batchRootDir = Paths.get(batchDir)
                .toAbsolutePath()
                .normalize();

        if (!Files.exists(batchRootDir) || !Files.isDirectory(batchRootDir) || !Files.isWritable(batchRootDir)) {
            var err = String.format("Local executor batch dir is not a writable directory: [%s]", batchRootDir);
            log.error(err);
            throw new EStartup(err);
        }

        return batchRootDir;
    }

    private boolean prepareBatchPersist(Properties properties) {

        var batchPersist = properties.getProperty(CONFIG_BATCH_PERSIST);

        if (batchPersist == null)
            return false;

        return Boolean.parseBoolean(batchPersist);
    }

    private String lookupBatchUser() {

        var processUser = System.getProperty(PROCESS_USERNAME_PROPERTY);

        if (processUser == null || processUser.isBlank()) {
            log.warn("Local executor batch user could not be determined: Process property [{}] is not set", PROCESS_USERNAME_PROPERTY);
            return null;
        }

        return processUser;
    }

    private LocalBatchState validState(LocalBatchState jobState) {

        if (jobState == null)
            throw new EUnexpected();

        return jobState;
    }



    private void fixSandboxPermissions(Path batchDir) {

        try {

            log.info("Sandbox directory [{}] has the wrong owner, attempting to fix...", batchDir.getFileName());

            var fs = FileSystems.getDefault();
            var userLookup = fs.getUserPrincipalLookupService();
            var batchUserPrincipal = userLookup.lookupPrincipalByName(batchUser);

            Files.setOwner(batchDir, batchUserPrincipal);

            log.info("Sandbox directory [{}] permissions have been repaired", batchDir.getFileName());
        }
        catch (IOException | UnsupportedOperationException e) {

            log.warn("Sandbox directory [{}] permissions could not be repaired: {}",
                    batchDir.getFileName(), e.getMessage(), e);
        }
    }

    private String decodeLaunchArg(LaunchArg arg, LocalBatchState batchState) {

        switch (arg.getArgType()) {

            case STRING:
                return arg.getStringArg();

            case PATH:

                var batchDir = Path.of(batchState.getBatchDir());
                var volume = arg.getPathVolume();

                if (!batchState.getVolumes().contains(volume)) {
                    var errorMsg = String.format("Requested volume does not exist: [%s]", volume);
                    log.error(errorMsg);
                    throw new ETracInternal(errorMsg);
                }

                return batchDir
                        .resolve(volume)
                        .resolve(arg.getPathArg())
                        .normalize()
                        .toString();

            default:

                throw new EUnexpected();  // TODO
        }
    }

    private void logBatchStart(String jobKey, ProcessBuilder processBuilder, Process process) {

        log.info("Starting batch process [{}]", jobKey);
        log.info("Command: [{}]", String.join(" ", processBuilder.command()));
        log.info("Working dir: [{}]", processBuilder.directory());
        log.info("User: [{}]", process.info().user().orElse("unknown"));
        log.info("PID: [{}]", process.pid());
    }

    private void logBatchComplete(String jobKey, Process process) {

        if (completionLog.contains(process.pid()))
            return;

        var procInfo = process.info();

        var succeededOrFailed = process.exitValue() == 0 ? "succeeded" : "failed";

        log.info("Batch process {}: [{}]", succeededOrFailed, jobKey);
        log.info("Exit code: [{}]", process.exitValue());
        log.info("CPU time: [{}]", procInfo.totalCpuDuration().orElse(Duration.ZERO));

        completionLog.add(process.pid());
    }
}
