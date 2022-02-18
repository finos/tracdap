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

import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exec.ExecutorPollResult;
import com.accenture.trac.common.exec.IBatchExecutor;

import com.accenture.trac.common.exec.ExecutorState;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.StorageV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class KubernetesBatchExecutor implements IBatchExecutor {

    private static final String JOB_CONFIG_VOL_NAME_TEMPLATE = "trac-job-config-%s";
    private static final String JOB_RESULT_VOL_NAME_TEMPLATE = "trac-job-config-%s";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ApiClient kubeClient;
    private final CoreV1Api kubeCoreApi;
    private final BatchV1Api kubeBatchApi;
    private final StorageV1Api kubeStorageApi;

    private final Map<String, Path> resultMappings = new HashMap<>();

    public KubernetesBatchExecutor() {

        try {
            this.kubeClient = Config.defaultClient();  // TODO: Timeouts
            this.kubeCoreApi = new CoreV1Api(kubeClient);
            this.kubeBatchApi = new BatchV1Api(kubeClient);
            this.kubeStorageApi = new StorageV1Api(kubeClient);
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
    public ExecutorState createBatchSandbox(String jobKey) {

        return null;
    }

    @Override
    public ExecutorState writeTextConfig(String jobKey, ExecutorState jobState, Map<String, String> configFiles) {

        var configMap = new V1ConfigMap()
                .data(configFiles);

        return writeConfig(jobKey, jobState, configMap);
    }

    @Override
    public ExecutorState writeBinaryConfig(String jobKey, ExecutorState jobState, Map<String, byte[]> configFiles) {

        var configMap = new V1ConfigMap()
                .binaryData(configFiles);

        return writeConfig(jobKey, jobState, configMap);
    }

    private ExecutorState writeConfig(String jobKey, ExecutorState jobState, V1ConfigMap configMap) {

        try {

            var namespace = "default";  // TODO: Namespace should be based on tenant? io.trac-platform.runtime.TENANT ?

            var configVolName = String.format(JOB_CONFIG_VOL_NAME_TEMPLATE, jobKey);
            var configMetadata = new V1ObjectMeta()
                    .name(configVolName)
                    .namespace(namespace);

            configMap.setApiVersion("v1");
            configMap.setKind("ConfigMap");
            configMap.setMetadata(configMetadata);

            var configMapResult = kubeCoreApi.createNamespacedConfigMap(namespace, configMap, null, null, null);

            // TODO: Any need to use result?

            return jobState;
        }
        catch (ApiException e) {

            if (e.getCode() == 409)
                return jobState;

            e.printStackTrace();
            throw new ETracInternal("Kubernetes error", e);  // TODO: Error
        }
    }

    @Override
    public ExecutorState startBatch(String jobKey, ExecutorState jobState, Set<String> configFiles) {

        try {

            var namespace = "default";

            var configVolName = String.format(JOB_CONFIG_VOL_NAME_TEMPLATE, jobKey);
            var resultVolName = String.format(JOB_CONFIG_VOL_NAME_TEMPLATE, jobKey);

            // CONFIG VOLUME

            var configSource = new V1ConfigMapVolumeSource();
            configSource.name(configVolName);

            for (var configFile : configFiles) {
                configSource.addItemsItem(new V1KeyToPath()
                        .key(configFile)
                        .path(configFile));
            }

            var configVolume = new V1Volume()
                    .name("config-volume")
                    .configMap(configSource);

            // RESULT_VOLUME

            var localStorageClass = new V1StorageClass()
                    .metadata(new V1ObjectMeta()
                    .name("local-storage")
                    .namespace(namespace))
                    .provisioner("kubernetes.io/no-provisioner")
                    .volumeBindingMode("WaitForFirstConsumer");

            // kubeStorageApi.createStorageClass(localStorageClass, null, null, null);

            var localResultDir = Files.createTempDirectory("trac-job-" + jobKey)
                    .toAbsolutePath()
                    .normalize();

            resultMappings.put(jobKey, localResultDir);


            var resultVolInfo =new V1PersistentVolume()
                    .metadata(new V1ObjectMeta()
                    .name(resultVolName + "-vol")
                    .namespace(namespace))
                    .spec(new V1PersistentVolumeSpec()
                    .volumeMode("Filesystem")
                    .addAccessModesItem("ReadWriteOnce")
                    .persistentVolumeReclaimPolicy("Retain")
                    .storageClassName("local-storage")
                    .local(new V1LocalVolumeSource()
                    .path(localResultDir.toString()))
                    .putCapacityItem("storage", Quantity.fromString("10Mi"))
                    .nodeAffinity(new V1VolumeNodeAffinity()
                    .required(new V1NodeSelector()
                    .addNodeSelectorTermsItem(new V1NodeSelectorTerm()
                            .addMatchFieldsItem(new V1NodeSelectorRequirement()
                                    .key("kubernetes.io/hostname")
                                    .operator("In")
                                    .addValuesItem("docker-desktop"))))));

            var resultVolResult = kubeCoreApi.createPersistentVolume(resultVolInfo, null, null, null);

            var pvc = new V1PersistentVolumeClaim()
                    .metadata(new V1ObjectMeta()
                            .name(resultVolName)
                            .namespace(namespace))
                    .spec(new V1PersistentVolumeClaimSpec()
                            .storageClassName("local-storage")
                            .volumeMode("Filesystem")
                            .volumeName(resultVolName + "-vol")
                            .addAccessModesItem("ReadWriteOnce")
                            .resources(new V1ResourceRequirements()
                            .putRequestsItem("storage", Quantity.fromString("10Mi"))));

            var pvcResult = kubeCoreApi.createNamespacedPersistentVolumeClaim(namespace, pvc, null, null, null);


            var resultVolume = new V1Volume()
                    .name("result-volume")
                    .persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
                    .claimName(resultVolName));

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

                    .addVolumeMountsItem(new V1VolumeMount()
                            .name("result-volume")
                            .mountPath("/mnt/result")
                            .readOnly(true))

                    // Startup args (these are applied to the image ENTRYPOINT)
                    .args(List.of(
                            "--sys-config", "/mnt/config/sys_config.json",
                            "--job-config", "/mnt/config/job_config.json",
//                            "--job-result-dir", "/mnt/result",
//                            "--job-result-format", "json",
                            "--dev-mode"));

            var jobSpec = new V1JobSpec()
                    .backoffLimit(0)
                    .template(new V1PodTemplateSpec()
                    .spec(new V1PodSpec()
                    .restartPolicy("Never")
                    .addVolumesItem(configVolume)
                    .addVolumesItem(resultVolume)
                    .addContainersItem(jobContainer)));

            var jobMetadata = new V1ObjectMeta()
                    .name("trac-job-" + jobKey)
                    .namespace("default");

            var job = new V1Job()
                    .apiVersion("batch/v1")
                    .kind("Job")
                    .metadata(jobMetadata)
                    .spec(jobSpec);

            var jobResult = kubeBatchApi.createNamespacedJob("default", job, null, null, null);

            System.out.println(jobResult.toString());

            return jobState;
        }
        catch (ApiException error) {

            error.printStackTrace();
            System.out.println(error.getResponseBody());
            throw new ETracInternal("Kubernetes error", error);  // TODO: Error
        }

        catch (IOException error) {

            error.printStackTrace();
            throw new ETracInternal("Local IO error", error);  // TODO: Error
        }
    }

    @Override
    public List<ExecutorPollResult> pollAllBatches(Map<String, ExecutorState> priorStates) {

        try {

            var namespace = "default";

            var jobList = kubeBatchApi.listNamespacedJob(
                    namespace, null, null, null, null /*"status.conditions[?(@.type==\"Complete\")]"*/, null, null, null, null, null, false);

            for (var kubeJob : jobList.getItems()) {

                var jobMetadata = kubeJob.getMetadata();
                var jobStatus = kubeJob.getStatus();

                // TODO: Handle this
                if (jobMetadata == null || jobStatus == null) {
                    log.warn("Job has null metadata or status");
                    continue;
                }

                log.info("Job [{}] has status [active={}, succeeded={}, failed={}]",
                        jobMetadata.getName(),
                        jobStatus.getActive(),
                        jobStatus.getSucceeded(),
                        jobStatus.getFailed());


                log.info(jobStatus.toString());

                var conditions = kubeJob.getStatus().getConditions();

                var complete =
                        conditions != null &&
                        conditions.stream().anyMatch(c ->
                        (c.getType().equals("Complete") || c.getType().equals("Failed")) &&
                        c.getStatus().equals("True"));

                if (complete) {

                    log.info("Removing completed job....");

                    var deleteStatus = kubeBatchApi.deleteNamespacedJob(
                            jobMetadata.getName(), namespace,
                            null, null, 0, null, null, new V1DeleteOptions());

                    log.info(deleteStatus.toString());
                }
            }

            return List.of();  // TODO: Report updates
        }
        catch (ApiException error) {

            error.printStackTrace();
            System.out.println(error.getResponseBody());
            throw new ETracInternal("Kubernetes error", error);  // TODO: Error
        }
    }

    @Override
    public void getBatchStatus(String jobKey, ExecutorState jobState) {
    }

    @Override
    public byte[] readBatchResult(String jobKey, ExecutorState jobState) {


        // kubeBatchApi.

        return null;
    }

    @Override
    public ExecutorState cancelBatch(String jobKey, ExecutorState jobState) {

        return jobState;
    }

    @Override
    public ExecutorState cleanUpBatch(String jobKey, ExecutorState jobState) {

        return jobState;
    }
}
