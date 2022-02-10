/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.orc.exec;

import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.config.JobConfig;
import com.accenture.trac.config.RepositoryConfig;
import com.accenture.trac.config.RuntimeConfig;
import com.accenture.trac.metadata.ImportModelJobDetails;
import com.accenture.trac.metadata.JobDefinition;
import com.accenture.trac.metadata.JobType;
import com.accenture.trac.metadata.TagHeader;
import com.accenture.trac.svc.orch.exec.kube.KubeBatchExecutor;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.resultOf;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.waitFor;


public class KubeExecutorTest {

    @Test
    void testKubeStatus() throws Exception {

        var executor = new KubeBatchExecutor();
        executor.executorStatus();
    }

    @Test
    void startSomething() throws Exception {

        var executor = new KubeBatchExecutor();

        var jobId = UUID.randomUUID();

        var configFiles = new HashMap<String, String>();
        configFiles.put("sys_config.json", "");
        configFiles.put("job_config.json", "");

        executor.writeTextConfig(jobId, configFiles);
        executor.startBatch(jobId, configFiles.keySet());
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

        var jobConfigJson = JsonFormat.printer().print(jobConfig);
        var sysConfigJson = JsonFormat.printer().print(sysConfig);

        var configFiles = new HashMap<String, String>();
        configFiles.put("sys_config.json", sysConfigJson);
        configFiles.put("job_config.json", jobConfigJson);

        var executor = new KubeBatchExecutor();
        executor.writeTextConfig(jobUuid, configFiles);
        executor.startBatch(jobUuid, configFiles.keySet());
    }
}
