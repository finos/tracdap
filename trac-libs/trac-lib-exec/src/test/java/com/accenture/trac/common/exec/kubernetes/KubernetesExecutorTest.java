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

package com.accenture.trac.common.exec.kubernetes;

import com.accenture.trac.common.exec.ExecutorVolumeType;
import com.accenture.trac.common.exec.LaunchArg;
import com.accenture.trac.common.exec.LaunchCmd;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.config.JobConfig;
import com.accenture.trac.config.RepositoryConfig;
import com.accenture.trac.config.RuntimeConfig;
import com.accenture.trac.metadata.ImportModelJobDetails;
import com.accenture.trac.metadata.JobDefinition;
import com.accenture.trac.metadata.JobType;
import com.accenture.trac.metadata.TagHeader;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;


@Disabled
public class KubernetesExecutorTest {

    @Test
    void testKubeStatus() {

        var executor = new KubernetesBatchExecutor();
        executor.executorStatus();
    }

    @Test
    void startSomething() {

        var executor = new KubernetesBatchExecutor();

        var jobId = UUID.randomUUID();

        var jobKey = jobId.toString();
        var jobState = executor.createBatch(jobKey);

        var launchCmd = LaunchCmd.trac();
        var launchArgs = List.of(
                LaunchArg.string("--sys-config"), LaunchArg.path("CONFIG", "job_config.json"),
                LaunchArg.string("--job-config"), LaunchArg.path("CONFIG", "sys_config.json"),
                LaunchArg.string("--dev-mode"));

        executor.startBatch(jobState, launchCmd, launchArgs);
    }

    @Test
    void runImportModel() throws Exception {

        var jobUuid = UUID.randomUUID();
        var jobTimestamp = Instant.now();

        var jobId = TagHeader.newBuilder()
                .setObjectId(jobUuid.toString())
                .setObjectVersion(1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(jobTimestamp))
                .setTagVersion(1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(jobTimestamp));

        var jobConfig = JobConfig.newBuilder()
                .setJobId(jobId)
                .setJob(JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_MODEL)
                .setImportModel(ImportModelJobDetails.newBuilder()
                .setLanguage("python")
                .setRepository("trac_git_repo")
                .setPath("examples/models/python/hello_world")
                .setEntryPoint("hello_world.HelloWorldModel")
                .setVersion("main")))
                .build();

        var sysConfig = RuntimeConfig.newBuilder()
                .putRepositories("trac_git_repo", RepositoryConfig.newBuilder()
                .setRepoType("git")
                .setRepoUrl("https://github.com/accenture/trac")
                .build()).build();

        var jobConfigJson = JsonFormat.printer().print(jobConfig).getBytes(StandardCharsets.UTF_8);
        var sysConfigJson = JsonFormat.printer().print(sysConfig).getBytes(StandardCharsets.UTF_8);

        var executor = new KubernetesBatchExecutor();

        var jobKey = jobId.toString();
        var state = executor.createBatch(jobKey);
        state = executor.createVolume(state, "config", ExecutorVolumeType.CONFIG_DIR);
        state = executor.createVolume(state, "results", ExecutorVolumeType.RESULT_DIR);
        state = executor.writeFile(state, "config", "sys_config.json", sysConfigJson);
        state = executor.writeFile(state, "config", "job_config.json", jobConfigJson);

        var launchCmd = LaunchCmd.trac();
        var launchArgs = List.of(
                LaunchArg.string("--sys-config"), LaunchArg.path("CONFIG", "job_config.json"),
                LaunchArg.string("--job-config"), LaunchArg.path("CONFIG", "sys_config.json"),
                LaunchArg.string("--dev-mode"));

        executor.startBatch(state, launchCmd, launchArgs);
    }
}
