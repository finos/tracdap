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
import com.accenture.trac.svc.orch.exec.IBatchRunner;
import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class KubeBatchExecutor implements IBatchRunner {

    private final ApiClient kubeClient;
    private final CoreV1Api kubeCoreApi;
    private final BatchV1Api kubeBatchApi;

    public KubeBatchExecutor() {

        try {
            this.kubeClient = Config.defaultClient();
            this.kubeCoreApi = new CoreV1Api(kubeClient);
            this.kubeBatchApi = new BatchV1Api(kubeClient);
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);  // TODO: Error
        }
    }

    @Override
    public CompletionStage<Void> executorStatus() {

        var result = new CompletableFuture<Void>();

        try {

            // Async method returns a "Call", which is cancellable
            // If there is a way to plug in to the cancel() method
            kubeCoreApi.listNodeAsync(
                    null, null, null, null, null, null, null, null, 10, false,
                    basicApiCallback(this::executorStatusSuccess, this::executorStatusFailed, result));

        }
        catch (ApiException e) {

            e.printStackTrace();

            result.completeExceptionally(e);
        }

        return result;
    }

    private void executorStatusSuccess(
            CompletableFuture<Void> future, V1NodeList nodeList,
            int statusCode, Map<String, List<String>> responseHeaders) {

        nodeList.getItems().forEach(System.out::println);
        future.complete(null);
    }

    private void executorStatusFailed(
            CompletableFuture<Void> future, ApiException error,
            int statusCode, Map<String, List<String>> responseHeaders) {

        future.completeExceptionally(error);
    }

    @Override
    public CompletionStage<Void> createBatchSandbox() {
        return null;
    }

    @Override
    public CompletionStage<Void> writeBatchConfig(UUID jobId) {

        try {

            var namespace = "default";  // TODO: Namespace should be based on tenant? io.trac-platform.runtime.TENANT ?

            var configMetadata = new V1ObjectMeta()
                    .name("trac-runtime-config-" + jobId)
                    .namespace(namespace);

            var configFiles = Map.ofEntries(
                    Map.entry("sys_config", ""),
                    Map.entry("job_config", ""));

            var configMap = new V1ConfigMap()
                    .apiVersion("v1")
                    .kind("ConfigMap")
                    .metadata(configMetadata)
                    .data(configFiles);

            var configMapResult = new CompletableFuture<Void>();

            kubeCoreApi.createNamespacedConfigMapAsync(
                    namespace, configMap, null, null, null, basicApiCallback(
                    (fut, result, status, headers) -> fut.complete(null),
                    (fut, error, status, headers) -> fut.completeExceptionally(error),
                    configMapResult));

            return configMapResult;
        }
        catch (ApiException e) {

            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<Void> startBatch(UUID jobId) {

        try {

            var jobMetadata = new V1ObjectMeta()
                    .name("trac-job-" + jobId)
                    .namespace("default");

            var configVolume = new V1Volume()
                    .name("config-volume")
                    .configMap(new V1ConfigMapVolumeSource()
                    .name("trac-runtime-config-" + jobId)
                    .addItemsItem(new V1KeyToPath()
                            .key("sys_config")
                            .path("sys_config.yaml"))
                    .addItemsItem(new V1KeyToPath()
                            .key("job_config")
                            .path("job_config.yaml")));

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
                            "/mnt/config/sys_config.yaml",
                            "--job-config",
                            "/mnt/config/job_config.yaml",
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

            var jobResult = new CompletableFuture<Void>();

            kubeBatchApi.createNamespacedJobAsync(
                    "default", job, null, null, null,
                    basicApiCallback(this::startBatchSucceeded, this::startBatchFailed, jobResult));

            return jobResult;
        }
        catch (ApiException error) {

            error.printStackTrace();

            return CompletableFuture.failedFuture(error);
        }
    }

    public void startBatchSucceeded(
            CompletableFuture<Void> future, V1Job job,
            int statusCode, Map<String, List<String>> responseHeaders) {

        System.out.println(job.toString());
        future.complete(null);
    }

    public void startBatchFailed(
            CompletableFuture<Void> future, ApiException error,
            int statusCode, Map<String, List<String>> responseHeaders) {

        error.printStackTrace();

        System.out.println(error.getResponseBody());

        future.completeExceptionally(error);
    }

    @Override
    public CompletionStage<Void> getBatchStatus() {
        return null;
    }

    @Override
    public CompletionStage<Void> readBatchResult() {
        return null;
    }

    @Override
    public CompletionStage<Void> cancelBatch() {
        return null;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // CALLBACK HANDLING
    // -----------------------------------------------------------------------------------------------------------------

    private <TResult, TFuture> ApiCallback<TResult> basicApiCallback(
            BasicApiCallbackFunc<TResult, TFuture> onSuccess,
            BasicApiCallbackFunc<ApiException, TFuture> onFailure,
            CompletableFuture<TFuture> future) {

        return new BasicApiCallback<>(onSuccess, onFailure, future);
    }

    private static class BasicApiCallback<TResult, TFuture> implements ApiCallback<TResult> {

        private final BasicApiCallbackFunc<TResult, TFuture> onSuccess;
        private final BasicApiCallbackFunc<ApiException, TFuture> onFailure;
        private final CompletableFuture<TFuture> future;

        public BasicApiCallback(
                BasicApiCallbackFunc<TResult, TFuture> onSuccess,
                BasicApiCallbackFunc<ApiException, TFuture> onFailure,
                CompletableFuture<TFuture> future) {

            this.onSuccess = onSuccess;
            this.onFailure = onFailure;
            this.future = future;
        }

        @Override
        public void onSuccess(TResult result, int statusCode, Map<String, List<String>> responseHeaders) {

            onSuccess.callback(future, result, statusCode, responseHeaders);
        }

        @Override
        public void onFailure(ApiException error, int statusCode, Map<String, List<String>> responseHeaders) {

            onFailure.callback(future, error, statusCode, responseHeaders);
        }

        @Override
        public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {

            // NO-OP
        }

        @Override
        public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {

            // NO-OP
        }
    }

    @FunctionalInterface
    private interface BasicApiCallbackFunc<TResult, TFuture> {

        void callback(
                CompletableFuture<TFuture> future, TResult result,
                int statusCode, Map<String, List<String>> responseHeaders);
    }
}
