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

package com.accenture.trac.svc.orch.exec.local;

import com.accenture.trac.api.JobStatusCode;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.config.JobResult;
import com.accenture.trac.svc.orch.exec.ExecutorPollResult;
import com.accenture.trac.svc.orch.exec.IBatchExecutor;
import com.accenture.trac.svc.orch.exec.ExecutorState;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;


public class LocalBatchExecutor implements IBatchExecutor {

    public static final String CONFIG_VENV_PATH = "venvPath";

    private static final String JOB_DIR_TEMPLATE = "trac-job-%s";
    private static final String JOB_CONFIG_SUBDIR = "config";
    private static final String JOB_RESULT_SUBDIR = "result";
    private static final String JOB_LOG_SUBDIR = "log";

    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
    private static final String PYTHON_EXE = IS_WINDOWS ? "python.exe" : "python";
    private static final String VENV_BIN_SUBDIR = IS_WINDOWS ? "Scripts" : "bin";
    private static final String VENV_ENV_VAR = "VIRTUAL_ENV";

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
    public ExecutorState createBatchSandbox(String jobKey) {

        try {

            var batchDirPrefix = String.format(JOB_DIR_TEMPLATE, jobKey);
            var batchDir = (batchRootDir != null)
                    ? Files.createTempDirectory(batchRootDir, batchDirPrefix)
                    : Files.createTempDirectory(batchDirPrefix);

            var configDir = batchDir.resolve(JOB_CONFIG_SUBDIR);
            Files.createDirectory(configDir);

            var resultDir = batchDir.resolve(JOB_RESULT_SUBDIR);
            Files.createDirectory(resultDir);

            var logDir = batchDir.resolve(JOB_LOG_SUBDIR);
            Files.createDirectory(logDir);

            var batchState = new LocalBatchState();
            batchState.setBatchDir(batchDir.toString());

            log.info("Job [{}] sandbox directory created: [{}]", jobKey, batchDir);

            return batchState;
        }
        catch (IOException e) {

            // TODO
            throw new RuntimeException("TODO");
        }
    }

    @Override
    public ExecutorState writeTextConfig(String jobKey, ExecutorState jobState, Map<String, String> configFiles) {

        var binaryMap = new HashMap<String, byte[]>();

        for (var configFile : configFiles.entrySet()) {

            var binaryContent = configFile.getValue().getBytes(StandardCharsets.UTF_8);
            binaryMap.put(configFile.getKey(), binaryContent);
        }

        return writeConfig(jobState, binaryMap);
    }

    @Override
    public ExecutorState writeBinaryConfig(String jobKey, ExecutorState jobState, Map<String, byte[]> configFiles) {

        return writeConfig(jobState, configFiles);
    }

    private ExecutorState writeConfig(ExecutorState jobState, Map<String, byte[]> configFiles) {

        try {

            var batchState = validState(jobState);
            var batchDir = Paths.get(batchState.getBatchDir());
            var configDir = batchDir.resolve(JOB_CONFIG_SUBDIR);

            for (var configFile : configFiles.entrySet()) {
                var configFilePath = configDir.resolve(configFile.getKey());
                Files.write(configFilePath, configFile.getValue(), StandardOpenOption.CREATE_NEW);
            }

            return batchState;
        }
        catch (IOException e) {

            // TODO
            throw new RuntimeException("TODO");
        }

    }

    @Override
    public ExecutorState startBatch(String jobKey, ExecutorState jobState, Set<String> configFiles) {

        try {

            var batchState = validState(jobState);
            var batchDir = Paths.get(batchState.getBatchDir());

            var sysConfigFile = batchDir.resolve(JOB_CONFIG_SUBDIR).resolve("sys_config.json");
            var jobConfigFile = batchDir.resolve(JOB_CONFIG_SUBDIR).resolve("job_config.json");
            var jobResultDir = batchDir.resolve(JOB_RESULT_SUBDIR);

            var launchExe = tracRuntimeVenv.resolve(VENV_BIN_SUBDIR).resolve(PYTHON_EXE);
            var launchArgs = List.of(
                    launchExe.toString(), "-m", "trac.rt.launch",
                    "--sys-config", sysConfigFile.toString(),
                    "--job-config", jobConfigFile.toString(),
                    "--job-result-dir", jobResultDir.toString(),
                    "--job-result-format", "json",
                    "--dev-mode");

            var pb = new ProcessBuilder();
            pb.command(launchArgs);
            pb.directory(batchDir.toFile());

            var env = pb.environment();
            env.put(VENV_ENV_VAR, tracRuntimeVenv.toString());

            var stdoutPath = batchDir.resolve(JOB_LOG_SUBDIR).resolve("stdout.txt");
            var stderrPath = batchDir.resolve(JOB_LOG_SUBDIR).resolve("stderr.txt");
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

    @Override
    public void getBatchStatus(String jobKey, ExecutorState jobState) {

        try {

            var batchState = validState(jobState);
            var batchDir = Paths.get(batchState.getBatchDir());

            var process = processMap.get(batchState.getPid());

            if (process == null)
                throw new EUnexpected();  // TODO

            if (process.isAlive())
                return;  // tODO: running

        }
        catch (RuntimeException e) {
            e.printStackTrace();
            // TODO
            throw new RuntimeException("TODO");
        }
    }

    @Override
    public ExecutorPollResult readBatchResult(String jobKey, ExecutorState jobState) {

        try {

            var batchState = validState(jobState);
            var process = processMap.get(batchState.getPid());

            if (process == null || process.isAlive())
                throw new EUnexpected();  // TODO

            var batchDir = Paths.get(batchState.getBatchDir());
            var resultDir = batchDir.resolve(JOB_RESULT_SUBDIR);

            var jobResultFile = String.format("job_result_%s.json", jobKey);
            var jobResultPath = resultDir.resolve(jobResultFile);
            var jobResultBytes = Files.readAllBytes(jobResultPath);
            var jobResultString = new String(jobResultBytes, StandardCharsets.UTF_8);

            var jobResultBuilder = JobResult.newBuilder();
            JsonFormat.parser().merge(jobResultString, JobResult.newBuilder());

            var result = new ExecutorPollResult();
            result.jobKey = jobKey;
            result.jobResult = jobResultBuilder.build();

            return result;
        }
        catch (InvalidProtocolBufferException e) {

            log.error("Garbled result from job execution: {}", e.getMessage(), e);

            var result = new ExecutorPollResult();
            result.jobKey = jobKey;
            result.statusCode = JobStatusCode.FAILED;
            result.errorMessage = "Garbled result from job execution";

            return result;
        }
        catch (IOException e) {
            e.printStackTrace();
            // TODO
            throw new RuntimeException("TODO");
        }
    }

    @Override
    public ExecutorState cancelBatch(String jobKey, ExecutorState jobState) {

        return jobState;
    }

    @Override
    public ExecutorState cleanUpBatch(String jobKey, ExecutorState jobState) {

        var batchState = validState(jobState);
        var process = processMap.get(batchState.getPid());

        if (process == null)
            throw new EUnexpected();  // TODO

        if (process.isAlive()) {
            log.warn("Process for job [{}] is not complete, it will be forcibly terminated", jobKey);
            process.destroyForcibly();
        }

        // TODO: Remove files depending on config

        processMap.remove(batchState.getPid());

        return batchState;
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
                            ? JobStatusCode.COMPLETE
                            : JobStatusCode.FAILED;


            if (currentStatus == JobStatusCode.COMPLETE || currentStatus == JobStatusCode.FAILED) {

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
