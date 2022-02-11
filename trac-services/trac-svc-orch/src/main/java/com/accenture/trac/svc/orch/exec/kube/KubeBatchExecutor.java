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

package com.accenture.trac.svc.orch.exec.kube;

import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.svc.orch.exec.IBatchRunner;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class KubeBatchExecutor implements IBatchRunner {

    private final ApiClient kubeClient;
    private final CoreV1Api kubeCoreApi;
    private final BatchV1Api kubeBatchApi;

    public KubeBatchExecutor() {

        try {
            this.kubeClient = Config.defaultClient();  // TODO: Timeouts
            this.kubeCoreApi = new CoreV1Api(kubeClient);
            this.kubeBatchApi = new BatchV1Api(kubeClient);
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);  // TODO: Error
        }
    }

    @Override
    public void executorStatus() {

        try {

            var nodeList = kubeCoreApi.listNode(null, null, null, null, null, null, null, null, 10, false);
            nodeList.getItems().forEach(System.out::println);
        }
        catch (ApiException e) {

            e.printStackTrace();
            throw new ETracInternal("Kubernetes error", e);  // TODO: Error
        }

    }

    @Override
    public void createBatchSandbox() {

    }

    @Override
    public void writeTextConfig(String jobKey, Map<String, String> configFiles) {

        var configMap = new V1ConfigMap()
                .data(configFiles);

        writeConfig(jobKey, configMap);
    }

    @Override
    public void writeBinaryConfig(String jobKey, Map<String, byte[]> configFiles) {

        var configMap = new V1ConfigMap()
                .binaryData(configFiles);

        writeConfig(jobKey, configMap);
    }

    private void writeConfig(String jobKey, V1ConfigMap configMap) {

        try {

            var namespace = "default";  // TODO: Namespace should be based on tenant? io.trac-platform.runtime.TENANT ?

            var configMetadata = new V1ObjectMeta()
                    .name("trac-runtime-config-" + jobKey)
                    .namespace(namespace);

            configMap.setApiVersion("v1");
            configMap.setKind("ConfigMap");
            configMap.setMetadata(configMetadata);

            var configMapResult = kubeCoreApi.createNamespacedConfigMap(namespace, configMap, null, null, null);

            // TODO: Any need to use result?
        }
        catch (ApiException e) {

            e.printStackTrace();
            throw new ETracInternal("Kubernetes error", e);  // TODO: Error
        }
    }

    @Override
    public void startBatch(String jobKey, Set<String> configFiles) {

        try {

            var jobMetadata = new V1ObjectMeta()
                    .name("trac-job-" + jobKey)
                    .namespace("default");

            var configSource = new V1ConfigMapVolumeSource();
            configSource.name("trac-runtime-config-" + jobKey);

            for (var configFile : configFiles) {
                configSource.addItemsItem(new V1KeyToPath()
                        .key(configFile)
                        .path(configFile));
            }

            var configVolume = new V1Volume()
                    .name("config-volume")
                    .configMap(configSource);

            var jobContainer = new V1Container()

                    // Container name and image
                    .name("trac-runtime-python")
                    .image("trac/runtime-python:DEVELOPMENT")
                    .imagePullPolicy("Never")

                    // Mounted volumes
                    .addVolumeMountsItem(new V1VolumeMount()
                            .name("config-volume")
                            .mountPath("/mnt/config")
                            .readOnly(true))

                    // Startup args (these are applied to the image ENTRYPOINT)
                    .args(List.of(
                            "--sys-config",
                            "/mnt/config/sys_config.json",
                            "--job-config",
                            "/mnt/config/job_config.json",
                            "--dev-mode"));

            var jobSpec = new V1JobSpec()
                    .backoffLimit(0)
                    .template(new V1PodTemplateSpec()
                    .spec(new V1PodSpec()
                    .restartPolicy("Never")
                    .addVolumesItem(configVolume)
                    .addContainersItem(jobContainer)));

            var job = new V1Job()
                    .apiVersion("batch/v1")
                    .kind("Job")
                    .metadata(jobMetadata)
                    .spec(jobSpec);

            var jobResult = kubeBatchApi.createNamespacedJob("default", job, null, null, null);

            System.out.println(jobResult.toString());
        }
        catch (ApiException error) {

            error.printStackTrace();
            System.out.println(error.getResponseBody());
            throw new ETracInternal("Kubernetes error", error);  // TODO: Error
        }
    }

    @Override
    public void getBatchStatus() {
    }

    @Override
    public void readBatchResult() {
    }

    @Override
    public void cancelBatch() {
    }

    @Override
    public void cleanUpBatch() {
    }
}
