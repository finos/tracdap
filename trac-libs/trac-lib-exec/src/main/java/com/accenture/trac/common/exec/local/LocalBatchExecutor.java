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

package com.accenture.trac.common.exec.local;

import com.accenture.trac.metadata.JobStatusCode;
import com.accenture.trac.common.exception.*;
import com.accenture.trac.common.exec.*;
import com.accenture.trac.common.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;


public class LocalBatchExecutor implements IBatchExecutor {

    public static final String CONFIG_VENV_PATH = "venvPath";

    private static final String JOB_DIR_TEMPLATE = "trac-job-%s";
    private static final String JOB_LOG_SUBDIR = "log";

    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
    private static final String PYTHON_EXE = IS_WINDOWS ? "python.exe" : "python";
    private static final String VENV_BIN_SUBDIR = IS_WINDOWS ? "Scripts" : "bin";
    private static final String VENV_ENV_VAR = "VIRTUAL_ENV";
    private static final List<String> TRAC_CMD_ARGS = List.of("-m", "trac.rt.launch");

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Path tracRuntimeVenv;
    private final Path batchRootDir;

    private final Map<Long, Process> processMap = new HashMap<>();

    public LocalBatchExecutor(Properties properties) {

        var venvPath = properties.getProperty(CONFIG_VENV_PATH);

        if (venvPath == null) {

            var err = String.format("Local executor config is missing a required property: [%s]", CONFIG_VENV_PATH);
            log.error(err);
            throw new EStartup(err);
        }

        this.tracRuntimeVenv = Paths.get(venvPath)
                .toAbsolutePath()
                .normalize();

        if (!Files.exists(tracRuntimeVenv) || !Files.isDirectory(tracRuntimeVenv)) {
            var err = String.format("Local executor venv path is not a valid directory: [%s]", tracRuntimeVenv);
            log.error(err);
            throw new EStartup(err);
        }

        this.batchRootDir = null;
    }

    private LocalBatchState validState(ExecutorState jobState) {

        if (!(jobState instanceof LocalBatchState))
            throw new EUnexpected();

        return (LocalBatchState) jobState;
    }

    @Override
    public void executorStatus() {

    }

    @Override
    public ExecutorState createBatch(String jobKey) {

        try {

            var batchDirPrefix = String.format(JOB_DIR_TEMPLATE, jobKey);
            var batchDir = (batchRootDir != null)
                    ? Files.createTempDirectory(batchRootDir, batchDirPrefix)
                    : Files.createTempDirectory(batchDirPrefix);

            var batchState = new LocalBatchState(jobKey);
            batchState.setBatchDir(batchDir.toString());

            log.info("Job [{}] sandbox directory created: [{}]", jobKey, batchDir);

            return batchState;
        }
        catch (AccessDeniedException e) {

            // Permissions errors reported as executor access error

            var errorMessage = String.format(
                    "Job [%s] failed to create sandbox directory: %s",
                    jobKey, e.getMessage());

            log.error(errorMessage, e);
            throw new EExecutorAccess(errorMessage, e);
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
    public void destroyBatch(String jobKey, ExecutorState state) {

        var batchState = validState(state);
        var process = processMap.get(batchState.getPid());

        if (process == null)
            throw new EUnexpected();  // TODO

        if (process.isAlive()) {
            log.warn("Process for job [{}] is not complete, it will be forcibly terminated", jobKey);
            process.destroyForcibly();
        }

        // TODO: Remove files depending on config

        processMap.remove(batchState.getPid());
    }

    @Override
    public ExecutorState createVolume(ExecutorState state, String volumeName, ExecutorVolumeType volumeType) {

        try {

            var batchState = validState(state);

            var isValid = MetadataConstants.VALID_IDENTIFIER.matcher(volumeName);
            var isReserved = MetadataConstants.TRAC_RESERVED_IDENTIFIER.matcher(volumeName);

            if (!isValid.matches()) {

                var errorMsg = String.format(
                        "Job [%s] requested volume name is not a valid identifier: [%s]",
                        state.getJobKey(), volumeName);

                log.error(errorMsg);
                throw new EExecutorValidation(errorMsg);
            }

            if (isReserved.matches()) {

                var errorMsg = String.format(
                        "Job [%s] requested volume name is a reserved identifier: [%s]",
                        state.getJobKey(), volumeName);

                log.error(errorMsg);
                throw new EExecutorValidation(errorMsg);
            }

            // Since volumeName is an identifier, it cannot be an absolute path

            var batchDir = Path.of(batchState.getBatchDir());
            var volumeDir = batchDir.resolve(volumeName);

            Files.createDirectory(volumeDir);

            batchState.getVolumes().add(volumeName);

            return batchState;
        }
        catch (AccessDeniedException e) {

            // Permissions errors reported as executor access error

            var errorMessage = String.format(
                    "Job [%s] failed to create batch volume [%s]: %s",
                    state.getJobKey(), volumeName, e.getMessage());

            log.error(errorMessage, e);
            throw new EExecutorAccess(errorMessage, e);
        }
        catch (IOException e) {

            var errorMessage = String.format(
                    "Job [%s] failed to create batch volume [%s]: %s",
                    state.getJobKey(), volumeName, e.getMessage());

            log.error(errorMessage, e);
            throw new EExecutorFailure(errorMessage, e);
        }
    }

    @Override
    public ExecutorState writeFile(ExecutorState state, String volumeName, String fileName, byte[] fileContent) {

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
        catch (FileAlreadyExistsException e) {

            // TODO
            throw new RuntimeException("TODO", e);
        }
        catch (IOException e) {

            // TODO
            throw new RuntimeException("TODO", e);
        }
    }

    @Override
    public byte[] readFile(ExecutorState state, String volumeName, String fileName) {

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
        catch (NoSuchFileException e) {

            // TODO
            throw new RuntimeException("TODO", e);
        }
        catch (IOException e) {

            // TODO
            throw new RuntimeException("TODO", e);
        }
    }

    @Override
    public ExecutorState startBatch(ExecutorState jobState, LaunchCmd launchCmd, List<LaunchArg> launchArgs) {

        try {

            var batchState = validState(jobState);
            var batchDir = Paths.get(batchState.getBatchDir());

            var pythonExe = tracRuntimeVenv.resolve(VENV_BIN_SUBDIR).resolve(PYTHON_EXE).toString();
            var decodedArgs = launchArgs.stream()
                    .map(arg -> decodeLaunchArg(arg, batchState))
                    .collect(Collectors.toList());

            var processArgs = new ArrayList<String>();
            processArgs.add(pythonExe);
            processArgs.addAll(TRAC_CMD_ARGS);
            processArgs.addAll(decodedArgs);

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

            batchState.setPid(process.pid());
            processMap.put(process.pid(), process);

            return batchState;
        }
        catch (IOException e) {
            e.printStackTrace();
            // TODO
            throw new RuntimeException("TODO");
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
                        .toString();

            default:

                throw new EUnexpected();  // TODO
        }
    }


    @Override
    public ExecutorState cancelBatch(ExecutorState jobState) {

        throw new ETracInternal("Cancellation not implemented yet");
    }


    @Override
    public ExecutorPollResult pollBatch(ExecutorState jobState) {

        try {

            var batchState = validState(jobState);
            var batchDir = Paths.get(batchState.getBatchDir());

            var process = processMap.get(batchState.getPid());

            if (process == null)
                throw new EUnexpected();  // TODO

            var pollResult = new ExecutorPollResult();
            pollResult.jobKey = jobState.getJobKey();
            pollResult.statusCode = process.isAlive()
                ? JobStatusCode.RUNNING :
                process.exitValue() == 0
                    ? JobStatusCode.SUCCEEDED
                    : JobStatusCode.FAILED;

            return pollResult;
        }
        catch (RuntimeException e) {
            e.printStackTrace();
            // TODO
            throw new RuntimeException("TODO");
        }
    }

    @Override
    public List<ExecutorPollResult> pollAllBatches(Map<String, ExecutorState> priorStates) {

        var updates = new ArrayList<ExecutorPollResult>();

        for (var job : priorStates.entrySet()) {

            var priorState = validState(job.getValue());
            var pid = priorState.getPid();
            var process = processMap.get(pid);

            if (process == null)
                throw new EUnexpected();  // TODO

            var currentStatus = process.isAlive()
                    ? JobStatusCode.RUNNING :
                    process.exitValue() == 0
                            ? JobStatusCode.SUCCEEDED
                            : JobStatusCode.FAILED;


            if (currentStatus == JobStatusCode.SUCCEEDED || currentStatus == JobStatusCode.FAILED) {

                var result = new ExecutorPollResult();
                result.jobKey = job.getKey();
                result.statusCode = currentStatus;
                result.executorState = job.getValue();

                updates.add(result);
            }
        }

        return updates;
    }
}
