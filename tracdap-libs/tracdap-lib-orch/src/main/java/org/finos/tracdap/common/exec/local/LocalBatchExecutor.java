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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class LocalBatchExecutor implements IBatchExecutor<LocalBatchState> {

    public static final String CONFIG_VENV_PATH = "venvPath";
    public static final String CONFIG_BATCH_DIR = "batchDir";
    public static final String CONFIG_BATCH_PERSIST = "batchPersist";

    public static final String PROCESS_USERNAME_PROPERTY = "user.name";

    private static final String JOB_DIR_TEMPLATE = "tracdap_%s_";
    private static final String JOB_LOG_SUBDIR = "log";

    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
    private static final String PYTHON_EXE = IS_WINDOWS ? "python.exe" : "python";
    private static final String VENV_BIN_SUBDIR = IS_WINDOWS ? "Scripts" : "bin";
    private static final String VENV_ENV_VAR = "VIRTUAL_ENV";
    private static final List<String> TRAC_CMD_ARGS = List.of("-m", "tracdap.rt.launch");

    private static final String FALLBACK_ERROR_MESSAGE = "Local batch terminated with non-zero exit code [%d]";
    private static final String FALLBACK_ERROR_DETAIL = "No details available";

    private static final Pattern TRAC_ERROR_LINE = Pattern.compile("tracdap.rt.exceptions.(E\\w+): (.+)");

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
    public Class<LocalBatchState> stateClass() {
        return LocalBatchState.class;
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
    public void destroyBatch(String jobKey, LocalBatchState state) {

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

    @Override
    public LocalBatchState createVolume(String jobKey, LocalBatchState state, String volumeName, ExecutorVolumeType volumeType) {

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
    public LocalBatchState writeFile(String jobKey, LocalBatchState state, String volumeName, String fileName, byte[] fileContent) {

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
    public byte[] readFile(String jobKey, LocalBatchState state, String volumeName, String fileName) {

        try {

            var batchState = validState(state);
            var process = processMap.get(batchState.getPid());

            if (!batchState.getVolumes().contains(volumeName)) {
                var errorMsg = String.format("Volume for readFile() does not exist: [%s]", volumeName);
                log.error(errorMsg);
                throw new ETracInternal(errorMsg);
            }

            if (process == null || process.isAlive()){
                log.error("readFile() called before process is complete");
                throw new ETracInternal("readFile() called before process is complete");
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
    public LocalBatchState startBatch(String jobKey, LocalBatchState jobState, LaunchCmd launchCmd, List<LaunchArg> launchArgs) {

        try {

            var batchState = validState(jobState);
            var batchDir = Paths.get(batchState.getBatchDir());

            var pythonExe = tracRuntimeVenv.resolve(VENV_BIN_SUBDIR).resolve(PYTHON_EXE).toString();
            var decodedArgs = launchArgs.stream()
                    .map(arg -> decodeLaunchArg(arg, batchState))
                    .collect(Collectors.toList());

            var processArgs = new ArrayList<String>();

            if (launchCmd.isTrac()) {
                processArgs.add(pythonExe);
                processArgs.addAll(TRAC_CMD_ARGS);
            }
            else {
                processArgs.add(launchCmd.customCommand());
                launchCmd.customArgs().stream()
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

            // Record logs under the batch dir
            var logDir = batchDir.resolve(JOB_LOG_SUBDIR);
            var stdoutPath = logDir.resolve("trac_rt_stdout.txt");
            var stderrPath = logDir.resolve("trac_rt_stderr.txt");

            // createDirectories() will not error if logic code has already created the log dir
            Files.createDirectories(logDir);
            pb.redirectOutput(stdoutPath.toFile());
            pb.redirectError(stderrPath.toFile());

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

    @Override public ExecutorJobInfo
    pollBatch(String jobKey, LocalBatchState jobState) {

        try {

            var batchState = validState(jobState);
            var process = processMap.get(batchState.getPid());

            if (process == null)
                throw new EUnexpected();  // TODO

            if (process.isAlive())
                return new ExecutorJobInfo(ExecutorJobStatus.RUNNING);

            logBatchComplete(jobKey, process, jobState);

            if (process.exitValue() == 0)
                return new ExecutorJobInfo(ExecutorJobStatus.SUCCEEDED);

            // Job has failed, try to get some helpful info on the error

            if (Files.exists(Path.of(batchState.getBatchDir(), JOB_LOG_SUBDIR, "trac_rt_stderr.txt"))) {

                var errorBytes = readFile(jobKey, batchState, JOB_LOG_SUBDIR, "trac_rt_stderr.txt");
                var errorDetail = new String(errorBytes, StandardCharsets.UTF_8);
                var statusMessage = extractErrorMessage(errorDetail, process.exitValue());

                return new ExecutorJobInfo(ExecutorJobStatus.FAILED, statusMessage, errorDetail);
            }
            else {

                var statusMessage = String.format(FALLBACK_ERROR_MESSAGE, process.exitValue());

                return new ExecutorJobInfo(ExecutorJobStatus.FAILED, statusMessage, FALLBACK_ERROR_DETAIL);
            }
        }
        catch (RuntimeException e) {
            e.printStackTrace();
            // TODO
            throw new RuntimeException("TODO");
        }
    }

    @Override public List<ExecutorJobInfo>
    pollBatches(List<Map.Entry<String, LocalBatchState>> priorStates) {

        var results = new ArrayList<ExecutorJobInfo>();

        for (var job : priorStates) {

            try {

                var jobState = validState(job.getValue());
                var pollResult = pollBatch(job.getKey(), jobState);
                results.add(pollResult);
            }
            catch (Exception e) {

                log.warn("Failed to poll job: [{}] {}", job.getKey(), e.getMessage(), e);
                results.add(new ExecutorJobInfo(ExecutorJobStatus.STATUS_UNKNOWN));
            }
        }

        return results;
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

    private void logBatchComplete(String jobKey, Process batchProcess, LocalBatchState batchState) {

        if (completionLog.contains(batchProcess.pid()))
            return;

        var procInfo = batchProcess.info();

        if (batchProcess.exitValue() == 0) {

            log.info("Batch process succeeded: [{}]", jobKey);
            log.info("Exit code: [{}]", batchProcess.exitValue());
            log.info("CPU time: [{}]", procInfo.totalCpuDuration().orElse(Duration.ZERO));
        }
        else {

            var errorBytes = readFile(jobKey, batchState, JOB_LOG_SUBDIR, "trac_rt_stderr.txt");
            var errorDetail = new String(errorBytes, StandardCharsets.UTF_8);

            log.error("Batch process failed: [{}]", jobKey);
            log.error("Exit code: [{}]", batchProcess.exitValue());
            log.error("CPU time: [{}]", procInfo.totalCpuDuration().orElse(Duration.ZERO));
            log.error(errorDetail);
        }

        completionLog.add(batchProcess.pid());
    }

    private String extractErrorMessage(String errorDetail, int exitCode) {

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
}
