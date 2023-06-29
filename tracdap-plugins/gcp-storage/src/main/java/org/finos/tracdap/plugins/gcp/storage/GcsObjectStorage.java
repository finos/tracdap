/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.plugins.gcp.storage;

import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.finos.tracdap.common.data.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.storage.CommonFileStorage;
import org.finos.tracdap.common.storage.FileStat;

import com.google.api.core.ApiFuture;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.storage.v2.*;

import io.grpc.CallOptions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.netty.channel.EventLoopGroup;

import org.apache.arrow.memory.ArrowBuf;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;


public class GcsObjectStorage extends CommonFileStorage {

    public static final String REGION_PROPERTY = "region";
    public static final String PROJECT_PROPERTY = "project";
    public static final String BUCKET_PROPERTY = "bucket";
    public static final String PREFIX_PROPERTY = "prefix";

    private final String project;
    private final String bucket;
    private final String prefix;

    private StorageClient storageClient;
    private BucketName bucketName;


    public GcsObjectStorage(String storageKey, Properties properties) {

        super(BUCKET_SEMANTICS, storageKey, properties, new GcsStorageErrors(storageKey));

        var project = properties.getProperty(PROJECT_PROPERTY);
        var bucket = properties.getProperty(BUCKET_PROPERTY);
        var prefix = properties.getProperty(PREFIX_PROPERTY);

        this.project = project;
        this.bucket = bucket;
        this.prefix = normalizePrefix(prefix);
    }

    private String normalizePrefix(String prefix) {

        if (prefix == null)
            return "";

        while (prefix.startsWith(BACKSLASH))
            prefix = prefix.substring(1);

        if (prefix.isBlank())
            return "";

        if (!prefix.endsWith(BACKSLASH))
            prefix = prefix + BACKSLASH;

        return prefix;
    }


    @Override
    public void start(EventLoopGroup eventLoopGroup) {

        try {

            String projectId = this.project;

            bucketName = BucketName.of(projectId, bucket);

//            try (var projects = ProjectsClient.create()){
//
//                log.info("LOOKUP PROJECT [{}]", project);
//
//                var request = GetProjectRequest.newBuilder()
//                        .setName(ProjectName.of(project).toString())Hey
//                        .build();
//
//                var projectInfo = projects.getProject(request);
//                projectId = projectInfo.getProjectId();
//            }



            log.info("INIT [{}], fs = [GCS], bucket = [{}], prefix = [{}]", storageKey, bucket, prefix);

            var transportProvider = InstantiatingGrpcChannelProvider.newBuilder()
                    .setChannelConfigurator(cb -> configureChannel(cb, eventLoopGroup))
                    .build();

            var settings = StorageSettings.newBuilder()
                    .setTransportChannelProvider(transportProvider)
                    .build();

            storageClient = StorageClient.create(settings);

            var request = ListObjectsRequest.newBuilder()
                    .setParent(bucketName.toString())
                    .setPrefix(prefix)
                    .setDelimiter(BACKSLASH)
                    .setPageSize(10)
                    .build();

            var ls = storageClient.listObjects(request);

            var count =
                    ls.getPage().getResponse().getObjectsCount() +
                    ls.getPage().getResponse().getPrefixesCount();

            System.out.println("Got [" + count + "] values using gRPC");
        }
        catch (ApiException e) {

            var cause = e.getCause();

            var statusMessage = cause instanceof StatusRuntimeException
                    ? ((StatusRuntimeException) cause).getStatus().getDescription()
                    : cause.getMessage();

            var message = "GCS storage failed to start: " + statusMessage;
            throw new EStartup(message, e);
        }
        catch (Exception e) {
            var message = "GCS storage failed to start: " + e.getMessage();
            throw new EStartup(message, e);
        }
    }

    private ManagedChannelBuilder<?> configureChannel(ManagedChannelBuilder<?> channelBuilder, EventLoopGroup elg) {

        if (channelBuilder instanceof NettyChannelBuilder)
            return configureNettyChannel((NettyChannelBuilder) channelBuilder, elg);

        return channelBuilder;
    }

    private NettyChannelBuilder configureNettyChannel(NettyChannelBuilder channelBuilder, EventLoopGroup elg) {

        if (elg instanceof NioEventLoopGroup) {

            return channelBuilder
                    .channelType(NioSocketChannel.class)
                    .eventLoopGroup(elg);
        }
        else {
            return channelBuilder;
        }
    }

    @Override
    public void stop() {

        log.info("STOP [{}], fs = [GCS], bucket = [{}], prefix = [{}]", storageKey, bucket, prefix);

        storageClient.shutdown();
        // storageClient.awaitTermination(20, TimeUnit.SECONDS);  todo
    }


    @Override
    protected CompletionStage<Boolean> fsExists(String storagePath, IExecutionContext ctx) {

        var objectKey = resolveObjectKey(storagePath);

        var request = GetObjectRequest.newBuilder()
                .setBucket(bucketName.toString())
                .setObject(objectKey)
                .build();

        var apiCall = storageClient.getObjectCallable();

        var response = unaryCall(apiCall, request, ctx);

        return response.handle((result, error) -> fsExistsCallback(storagePath, result, error));
    }

    protected boolean fsExistsCallback(String storagePath, com.google.storage.v2.Object result, Throwable error) {

        if (error != null) {
            throw errors.handleException("EXISTS", storagePath, error);
        }

        return true;
    }

    @Override
    protected CompletionStage<Boolean> fsDirExists(String storagePath, IExecutionContext ctx) {

        var prefix = resolvePrefix(storagePath);

        var request = ListObjectsRequest.newBuilder()
                .setParent(bucketName.toString())
                .setPrefix(prefix)
                .setPageSize(1)
                .build();

        var apiCall = storageClient.listObjectsCallable();

        var response = unaryCall(apiCall, request, ctx).exceptionally(e -> errorGuard(e));

        // If there are any objects with the dir prefix, then the dir exists
        // This will include the dir itself if it has an object
        // Since no delimiter was specified, it is not necessary to check common prefixes

        return response.thenApply(result -> result.getObjectsCount() > 0);
    }

    @Override
    protected CompletionStage<FileStat> fsGetFileInfo(String objectKey, IExecutionContext ctx) {
        return null;
    }

    @Override
    protected CompletionStage<FileStat> fsGetDirInfo(String prefix, IExecutionContext ctx) {
        return null;
    }

    @Override
    protected CompletionStage<List<FileStat>> fsListContents(String prefix, String startAfter, int maxKeys, boolean recursive, IExecutionContext ctx) {
        return null;
    }

    @Override
    protected CompletionStage<Void> fsCreateDir(String storagePath, IExecutionContext ctx) {

        var prefix = resolvePrefix(storagePath);

        var object = com.google.storage.v2.Object.newBuilder()
                .setBucket(bucketName.toString())
                .setName(prefix)
                .setSize(0);

        var request = ComposeObjectRequest.newBuilder()
                .setDestination(object)
                .build();

        var apiCall = storageClient.composeObjectCallable();
        var response = unaryCall(apiCall, request, ctx).exceptionally(e -> errorGuard(e));

        return response.thenApply(x -> null);
    }

    @Override
    protected CompletionStage<Void> fsDeleteFile(String objectKey, IExecutionContext ctx) {
        return null;
    }

    @Override
    protected CompletionStage<Void> fsDeleteDir(String directoryKey, IExecutionContext ctx) {
        return null;
    }

    @Override
    protected CompletionStage<ArrowBuf> fsReadChunk(String objectKey, long offset, int size, IDataContext ctx) {
        return null;
    }

    @Override
    protected Flow.Publisher<ArrowBuf> fsOpenInputStream(String objectKey, IDataContext ctx) {
        return null;
    }

    @Override
    protected Flow.Subscriber<ArrowBuf> fsOpenOutputStream(String objectKey, CompletableFuture<Long> signal, IDataContext ctx) {
        return null;
    }

    private String resolveObjectKey(String storagePath) {

        if (prefix.isEmpty())
            return storagePath;

        if (storagePath.isEmpty())
            return prefix;

        return prefix + storagePath;
    }

    private String resolvePrefix(String storagePath) {

        if (prefix.isEmpty())
            return storagePath;

        if (storagePath.isEmpty())
            return prefix;

        return prefix + storagePath;
    }

    private <TRequest, TResponse> CompletionStage<TResponse> unaryCall(
            UnaryCallable<TRequest, TResponse> callable,
            TRequest request,
            IExecutionContext dataCtx) {

        var callOptions = CallOptions.DEFAULT
                .withExecutor(dataCtx.eventLoopExecutor());

        var callCtx = GrpcCallContext.createDefault()
                .withCallOptions(callOptions);

        var apiCall = callable.futureCall(request, callCtx);

        return toContext(dataCtx, javaFuture(apiCall));
    }

    private static <T> CompletionStage<T> javaFuture(ApiFuture<T> future) {

        var javaFuture = new CompletableFuture<T>();

        future.addListener(() -> {

            try {

                if (!future.isDone())
                    javaFuture.completeExceptionally(new IllegalStateException());

                else if (future.isCancelled())
                    javaFuture.cancel(true);

                else {
                    var result = future.get();
                    javaFuture.complete(result);
                }
            }
            catch (InterruptedException | CancellationException e) {
                javaFuture.completeExceptionally(new IllegalStateException());
            }
            catch (ExecutionException e) {

                if (e.getCause() != null)
                    javaFuture.completeExceptionally(e.getCause());
                else
                    javaFuture.completeExceptionally(e);
            }
            catch (Throwable e) {
                javaFuture.completeExceptionally(e);
            }

        }, Runnable::run);

        return javaFuture;
    }

    private <T> T errorGuard(Throwable error) {

        throw new ETracInternal("Error marker", error);
    }
}
