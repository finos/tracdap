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

import com.google.storage.v2.Object;
import org.finos.tracdap.common.data.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.storage.CommonFileStorage;
import org.finos.tracdap.common.storage.FileStat;
import org.finos.tracdap.common.storage.FileType;
import org.finos.tracdap.common.storage.StorageErrors;

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.*;
import com.google.storage.v2.*;

import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.apache.arrow.memory.ArrowBuf;

import java.time.Instant;
import java.util.List;
import java.util.Map;
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

        var response = GcpUtils.unaryCall(apiCall, request, ctx.eventLoopExecutor());

        return response.handle((result, error) -> fsExistsCallback(storagePath, result, error));
    }

    protected boolean fsExistsCallback(String storagePath, com.google.storage.v2.Object result, Throwable error) {

        if (error instanceof NotFoundException)
            return false;

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

        var response = GcpUtils.unaryCall(apiCall, request, ctx.eventLoopExecutor());

        return response.handle((result, error) -> fsDirExistsCallback(storagePath, result, error));
    }

    private boolean fsDirExistsCallback(String storagePath, ListObjectsResponse result, Throwable error) {

        if (error != null) {
            throw errors.handleException("EXISTS", storagePath, error);
        }

        // If there are any objects with the dir prefix, then the dir exists
        // This will include the dir itself if it has an object
        // Since no delimiter was specified, it is not necessary to check common prefixes

        return result.getObjectsCount() > 0;
    }

    @Override
    protected CompletionStage<FileStat> fsGetFileInfo(String storagePath, IExecutionContext ctx) {
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

        var prefix = resolvePrefix(storagePath) + BACKSLASH;

        var object = com.google.storage.v2.Object.newBuilder()
                .setBucket(bucketName.toString())
                .setName(prefix);

        var request = WriteObjectRequest.newBuilder()
                .setWriteObjectSpec(WriteObjectSpec.newBuilder()
                .setResource(object)
                .setObjectSize(0))
                .setFinishWrite(true)
                .build();

        var apiCall = addMissingRequestParams(storageClient.writeObjectCallable());

        var response = GcpUtils.clientStreamingCall(apiCall, request, ctx.eventLoopExecutor());

        return response.handle((result, error) -> fsCreateDirCallback(storagePath, result, error));
    }

    private Void fsCreateDirCallback(String storagePath, WriteObjectResponse result, Throwable error) {

        if (error != null) {
            throw errors.handleException("MKDIR", storagePath, error);
        }

        if (!result.hasResource())
            throw errors.explicitError("MKDIR", storagePath, StorageErrors.ExplicitError.UNKNOWN_ERROR, "Directory was not created");

        return null;
    }

    @Override
    protected CompletionStage<Void> fsDeleteFile(String storagePath, IExecutionContext ctx) {

        var objectKey = resolveObjectKey(storagePath);

        var request = DeleteObjectRequest.newBuilder()
                .setBucket(bucketName.toString())
                .setObject(objectKey)
                .build();

        var apiCall = storageClient.deleteObjectCallable();

        var response = GcpUtils.unaryCall(apiCall, request, ctx.eventLoopExecutor());

        return response.handle((result, error) -> fsDeleteFileCallback(storagePath, error));
    }

    private Void fsDeleteFileCallback(String storagePath, Throwable error) {

        if (error != null) {
            throw errors.handleException("RM", storagePath, error);
        }

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
    protected Flow.Subscriber<ArrowBuf> fsOpenOutputStream(String storagePath, CompletableFuture<Long> signal, IDataContext ctx) {

        var objectKey = resolveObjectKey(storagePath);

        return new GcsObjectWriter(storageClient, ctx, bucketName, objectKey, signal);
    }

    private String resolveObjectKey(String storagePath) {

        if (prefix.isEmpty())
            return storagePath;

        if (storagePath.isEmpty())
            return prefix;

        return prefix + storagePath;
    }

    private String resolvePrefix(String storagePath) {

        return resolveObjectKey(storagePath) + BACKSLASH;
    }

    private <TRequest, TResponse>
    ClientStreamingCallable<TRequest, TResponse>
    addMissingRequestParams(ClientStreamingCallable<TRequest, TResponse> callable) {

        // https://github.com/googleapis/java-storage/blob/main/google-cloud-storage/src/main/java/com/google/cloud/storage/WriteFlushStrategy.java#L89

        // GCP SDK adds in this required header
        // For some API calls / usage patterns, the header does not get added

        var callParams = String.format("bucket=%s", bucketName);
        var callMetadata = Map.of("x-goog-request-params", List.of(callParams));

        var defaultContext = GrpcCallContext.createDefault().withExtraHeaders(callMetadata);

        return callable.withDefaultCallContext(defaultContext);
    }
}
