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

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.svc.orch.exec.IBatchExecutor;
import com.accenture.trac.svc.orch.exec.JobExecState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class LocalBatchExecutor implements IBatchExecutor {

    private static final String JOB_DIR_TEMPLATE = "trac-job-%s";
    private static final String JOB_CONFIG_SUBDIR = "config";
    private static final String JOB_RESULT_SUBDIR = "result";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Path batchRootDir;

    private final Map<Long, Process> processMap = new HashMap<>();

    public LocalBatchExecutor() {

        this.batchRootDir = null;
    }

    private LocalBatchState validState(JobExecState jobState) {

        if (!(jobState instanceof LocalBatchState))
            throw new EUnexpected();

        return (LocalBatchState) jobState;
    }

    @Override
    public void executorStatus() {

    }

    @Override
    public JobExecState createBatchSandbox(String jobKey) {

        try {

            var batchDirPrefix = String.format(JOB_DIR_TEMPLATE, jobKey);
            var batchDir = (batchRootDir != null)
                    ? Files.createTempDirectory(batchRootDir, batchDirPrefix)
                    : Files.createTempDirectory(batchDirPrefix);

            var configDir = batchDir.resolve(JOB_CONFIG_SUBDIR);
            Files.createDirectory(configDir);

            var resultDir = batchDir.resolve(JOB_RESULT_SUBDIR);
            Files.createDirectory(resultDir);

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
    public JobExecState writeTextConfig(String jobKey, JobExecState jobState, Map<String, String> configFiles) {

        var binaryMap = new HashMap<String, byte[]>();

        for (var configFile : configFiles.entrySet()) {

            var binaryContent = configFile.getValue().getBytes(StandardCharsets.UTF_8);
            binaryMap.put(configFile.getKey(), binaryContent);
        }

        return writeConfig(jobState, binaryMap);
    }

    @Override
    public JobExecState writeBinaryConfig(String jobKey, JobExecState jobState, Map<String, byte[]> configFiles) {

        return writeConfig(jobState, configFiles);
    }

    private JobExecState writeConfig(JobExecState jobState, Map<String, byte[]> configFiles) {

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
    public JobExecState startBatch(String jobKey, JobExecState jobState, Set<String> configFiles) {

        try {

            var batchState = validState(jobState);
            var batchDir = Paths.get(batchState.getBatchDir());

            var sysConfigFile = batchDir.resolve(JOB_CONFIG_SUBDIR).resolve("sys_config.json");
            var jobConfigFile = batchDir.resolve(JOB_CONFIG_SUBDIR).resolve("job_config.json");

            var launchArgs = List.of(
                    "python", "-m", "trac.rt.launch",
                    "--sys-config", sysConfigFile.toString(),
                    "--job-config", jobConfigFile.toString(),
//                            "--job-result-dir", "/mnt/result",
//                            "--job-result-format", "json",
                    "--dev-mode");

            var pb = new ProcessBuilder();
            pb.command(launchArgs);
            pb.directory(batchDir.toFile());

            var env = pb.environment();
            var path = env.get("PATH");

            env.put("PYTHON_HOME", "");
            env.put("PATH", path);

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
    public void getBatchStatus(String jobKey, JobExecState jobState) {

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
    public void readBatchResult(String jobKey, JobExecState jobState) {

        try {

            var batchState = validState(jobState);
            var process = processMap.get(batchState.getPid());

            if (process == null || process.isAlive())
                throw new EUnexpected();  // TODO

            var batchDir = Paths.get(batchState.getBatchDir());
            var resultDir = batchDir.resolve(JOB_RESULT_SUBDIR);

        }
        catch (RuntimeException e) {
            e.printStackTrace();
            // TODO
            throw new RuntimeException("TODO");
        }
    }

    @Override
    public JobExecState cancelBatch(String jobKey, JobExecState jobState) {

        return jobState;
    }

    @Override
    public JobExecState cleanUpBatch(String jobKey, JobExecState jobState) {

        return jobState;
    }

    @Override
    public void pollAllBatches() {

    }
}
