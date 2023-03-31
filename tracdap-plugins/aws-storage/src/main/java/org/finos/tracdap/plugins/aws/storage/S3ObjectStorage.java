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

package org.finos.tracdap.plugins.aws.storage;

import io.netty.channel.EventLoopGroup;
import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.storage.*;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public class S3ObjectStorage implements IFileStorage {

    public static final String BUCKET_PROPERTY = "bucket";
    public static final String PREFIX_PROPERTY = "prefix";
    public static final String REGION_PROPERTY = "region";
    public static final String ENDPOINT_PROPERTY = "endpoint";

    public static final String CREDENTIALS_PROPERTY = "credentials";
    public static final String CREDENTIALS_DEFAULT = "default";
    public static final String CREDENTIALS_STATIC = "static";
    public static final String ACCESS_KEY_ID_PROPERTY = "accessKeyId";
    public static final String SECRET_ACCESS_KEY_PROPERTY = "secretAccessKey";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storageKey;
    private final AwsCredentialsProvider credentials;
    private final String bucket;
    private final StoragePath prefix;
    private final Region region;
    private final URI endpoint;

    private final StorageErrors errors;

    private S3AsyncClient client;


    public S3ObjectStorage(Properties properties) {

        this.storageKey = properties.getProperty(IStorageManager.PROP_STORAGE_KEY);

        var bucket = properties.getProperty(BUCKET_PROPERTY);
        var prefix = properties.getProperty(PREFIX_PROPERTY);
        var region = properties.getProperty(REGION_PROPERTY);
        var endpoint = properties.getProperty(ENDPOINT_PROPERTY);

        this.bucket = bucket;
        this.prefix = prefix != null && !prefix.isBlank() ? StoragePath.forPath(prefix) : StoragePath.root();
        this.region = region != null && !region.isBlank() ? Region.of(region) : null;
        this.endpoint = endpoint != null && !endpoint.isBlank() ? URI.create(endpoint) : null;

        this.credentials = setupCredentials(properties);

        this.errors = new S3StorageErrors(storageKey, log);
    }

    private AwsCredentialsProvider setupCredentials(Properties properties) {

        var mechanism = properties.containsKey(CREDENTIALS_PROPERTY)
                ? properties.getProperty(CREDENTIALS_PROPERTY)
                : CREDENTIALS_DEFAULT;

        if (CREDENTIALS_DEFAULT.equalsIgnoreCase(mechanism)) {
            log.info("Using [{}] credentials mechanism", CREDENTIALS_DEFAULT);
            return DefaultCredentialsProvider.create();
        }

        if (CREDENTIALS_STATIC.equalsIgnoreCase(mechanism)) {

            var accessKeyId = properties.getProperty(ACCESS_KEY_ID_PROPERTY);
            var secretAccessKey = properties.getProperty(SECRET_ACCESS_KEY_PROPERTY);
            var credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);

            log.info("Using [{}] credentials mechanism, access key id = [{}]", CREDENTIALS_STATIC, accessKeyId);

            return StaticCredentialsProvider.create(credentials);
        }

        var message = String.format("Unrecognised credentials mechanism: [%s]", mechanism);
        log.error(message);
        throw new EStartup(message);
    }

    @Override
    public void start(EventLoopGroup eventLoopGroup) {

        // We want to avoid creating an ELG / thread pool for each storage instance
        // The data service sets up one ELG on startup, with a number of ELs according to core / vCPU count
        // We aim for one EL per core, minus one core for OS / system tasks
        // This works with all async processing of small events or data chunks directly in the EL

        // For now, we create one client instance per storage backend
        // The client instance controls connection limits, current AWS default is max 50 concurrent connections
        // So e.g. 3 storage instances = 3 clients => max 150 total connections, all running on the same ELG
        // When all the processing is async, there is no value to add more threads with separate ELGs
        // ATM, we are not trying to prioritize or share clients / connection pools across storage backends

        // The AWS client is entirely async so there is no need to worry about worker pools for blocking tasks
        // If other clients do this, it may be desirable to bring down the thread count in the main ELG

        var httpElg = SdkEventLoopGroup.create(eventLoopGroup);
        var httpClient = NettyNioAsyncHttpClient.builder().eventLoopGroup(httpElg);

        // Do not post events to another thread, callback directly in the EL
        // Anyway we need to post events to the EL for the current request, so there is no point posting twice

        // IMPORTANT: Every async call must explicitly post events back to the right EL for the current request
        // It may make sense to lift this handling into the core data library
        // Although thought is needed in case a backend posts events for a single request on multiple ELs

        var async = ClientAsyncConfiguration.builder()
                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, Runnable::run)
                .build();

        var clientBuilder = S3AsyncClient.builder()
                .httpClientBuilder(httpClient)
                .asyncConfiguration(async)
                .credentialsProvider(credentials);

        if (region != null)
            clientBuilder.region(region);

        if (endpoint != null)
            clientBuilder.endpointOverride(endpoint);

        this.client = clientBuilder.build();

        log.info("Created S3 storage, bucket = [{}], prefix = [{}]", bucket, prefix);
    }

    @Override
    public void stop() {

        this.client.close();
    }

    @Override
    public CompletionStage<Boolean> exists(String storagePath, IExecutionContext execContext) {

        try {

            log.info("STORAGE OPERATION: {} {} [{}]", storageKey, EXISTS_OPERATION, storagePath);

            var fileObjectKey = resolvePath(storagePath, true, EXISTS_OPERATION);
            var dirObjectKey = fileObjectKey + "/";

            return existsImpl(fileObjectKey, execContext).thenCompose(found -> found
                    ? CompletableFuture.completedFuture(true)
                    : existsImpl(dirObjectKey, execContext));
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Boolean> existsImpl(String objectKey, IExecutionContext execContext) {

        var request = HeadObjectRequest.builder()
                .bucket(this.bucket)
                .key(objectKey)
                .build();

        return client.headObject(request)
                .thenApplyAsync(Function.identity(), execContext.eventLoopExecutor())

                // Object exists if the call completes
                .thenApply(x -> true)

                .exceptionally(error -> {

                    S3Exception s3Error;

                    if (error instanceof S3Exception)
                        s3Error = (S3Exception) error;
                    else if (error.getCause() instanceof S3Exception)
                        s3Error = (S3Exception) error.getCause();
                    else
                        s3Error = null;

                    if (s3Error != null && s3Error.statusCode() == HttpStatusCode.NOT_FOUND)
                        return false;

                    throw errors.handleException(error, objectKey, SIZE_OPERATION);
                });
    }

    @Override
    public CompletionStage<Long> size(String storagePath, IExecutionContext execContext) {

        try {

            log.info("STORAGE OPERATION: {} {} [{}]", storageKey, SIZE_OPERATION, storagePath);

            var objectKey = resolvePath(storagePath, true, SIZE_OPERATION);

            var request = HeadObjectRequest.builder()
                    .bucket(this.bucket)
                    .key(objectKey)
                    .build();

            return client.headObject(request)
                    .thenApplyAsync(Function.identity(), execContext.eventLoopExecutor())

                    // Take size from the content-length header
                    .thenApply(HeadObjectResponse::contentLength)

                    .exceptionally(error -> {
                        throw errors.handleException(error, objectKey, SIZE_OPERATION);
                    });
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<FileStat> stat(String storagePath, IExecutionContext execContext) {

        try {

            log.info("STORAGE OPERATION: {} {} [{}]", storageKey, STAT_OPERATION, storagePath);

            var objectKey = resolvePath(storagePath, true, STAT_OPERATION);

            return dirProcessing(objectKey, STAT_OPERATION, k_ -> statImpl(k_, execContext), execContext);

//        var request = GetObjectAttributesRequest.builder()
//            .bucket(bucket)
//            .key(objectKey)
//            .objectAttributes(ObjectAttributes.OBJECT_SIZE)
//            .build();
//
//        return client.getObjectAttributes(request).handleAsync((response, error) -> {
//
//            if (error != null) {
//                throw errors.handleException(error, storagePath, STAT_OPERATION);
//            }
//
//            var path = storagePath;
//            var name = storagePath.substring(storagePath.lastIndexOf("/") + 1);
//            var fileType = FileType.FILE;
//            var size = response.objectSize();
//
//            var ctime = response.lastModified();
//            var mtime = response.lastModified();
//            var atime = (Instant) null;
//
//            return new FileStat(path, name, fileType, size, ctime, mtime, atime);
//
//        }, execContext.eventLoopExecutor());

        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletionStage<FileStat> statImpl(String objectKey, IExecutionContext execContext) {

        var request = GetObjectAttributesRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .objectAttributes(ObjectAttributes.OBJECT_SIZE)
                .build();

        return client.getObjectAttributes(request).handleAsync((response, error) -> {

            if (error != null) {
                throw errors.handleException(error, objectKey, STAT_OPERATION);
            }

            var name = objectKey.substring(objectKey.lastIndexOf("/", objectKey.length() - 2) + 1);
            var fileType = FileType.FILE;
            var size = response.objectSize();
            var mtime = response.lastModified();

            return new FileStat(objectKey, name, fileType, size, mtime, /* atime = */ null);

        }, execContext.eventLoopExecutor());
    }

    @Override
    public CompletionStage<List<FileStat>> ls(String storagePath, IExecutionContext execContext) {

        try {

            var dirPath = storagePath.endsWith(BACKSLASH) ? storagePath : storagePath + BACKSLASH;

            log.info("STORAGE OPERATION: {} {} [{}]", storageKey, LS_OPERATION, dirPath);

            var objectKey = resolvePath(dirPath, true, SIZE_OPERATION);

            var request = ListObjectsRequest.builder()
                    .bucket(bucket)
                    .prefix(objectKey)
                    .delimiter(BACKSLASH)
                    .build();

            return client.listObjects(request).handleAsync((response, error) -> {

                if (error != null) {
                    throw errors.handleException(error, storagePath, LS_OPERATION);
                } else {
                    return lsResult(objectKey, response);
                }

            }, execContext.eventLoopExecutor());
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private List<FileStat> lsResult(String dirObjectKey, ListObjectsResponse response) {

        log.info(response.commonPrefixes().toString());

        var stats = new ArrayList<FileStat>();
        var rootPrefix = prefix.toString();

        // First entry should always be the directory being listed
        if (response.contents().isEmpty()) {
            throw errors.explicitError(DIRECTORY_NOT_FOUND_EXCEPTION, dirObjectKey, LS_OPERATION);
        }

        var folderContents = response.contents().subList(1, response.contents().size());

        for (var obj : folderContents) {

            var path = obj.key().substring(rootPrefix.length());
            var name = obj.key().substring(obj.key().lastIndexOf("/") + 1);
            var fileType = FileType.FILE;
            var size = obj.size();

            var mtime = obj.lastModified();
            var atime = obj.lastModified();  // todo

            var stat = new FileStat(path, name, fileType, size, mtime, atime);

            stats.add(stat);
        }

        for (var prefix : response.commonPrefixes()) {

            var path = prefix.prefix().substring(rootPrefix.length(), prefix.prefix().length() - 1);
            var name = path.substring(path.lastIndexOf("/") + 1);
            var fileType = FileType.DIRECTORY;
            var size = 0;

            var stat = new FileStat(path, name, fileType, size, null, null);

            stats.add(stat);
        }

        return stats;
    }

    @Override
    public CompletionStage<Void> mkdir(String storagePath, boolean recursive, IExecutionContext execContext) {

        try {

            log.info("STORAGE OPERATION: {} {} [{}]", storageKey, MKDIR_OPERATION, storagePath);

            var bucketKey = resolvePath(storagePath, false, MKDIR_OPERATION) + "/";

            var request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(bucketKey)
                    .contentLength(0L)
                    .build();

            var content = AsyncRequestBody.empty();

            return client.putObject(request, content).handleAsync((response, error) -> {

                if (error != null) {
                    throw errors.handleException(error, storagePath, LS_OPERATION);
                } else {
                    return null;
                }

            }, execContext.eventLoopExecutor());
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<Void> rm(String storagePath, boolean recursive, IExecutionContext execContext) {

        try {

            log.info("STORAGE OPERATION: {} {} [{}]", storageKey, RM_OPERATION, storagePath);

            var fileKey = resolvePath(storagePath, false, RM_OPERATION);
            var dirKey = fileKey + "/";

            return existsImpl(fileKey, execContext).thenComposeAsync(exists -> exists
                            ? rmSingle(fileKey, execContext)
                            : rmDir(dirKey, recursive, execContext),
                    execContext.eventLoopExecutor());
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletionStage<Void> rmDir(String dirKey, boolean recursive, IExecutionContext execContext) {

        return ls(dirKey, execContext).thenComposeAsync(stat -> {

            if (stat.isEmpty())
                return rmSingle(dirKey, execContext);

            throw new ETracInternal("RM recursive not implemented yet");
        }, execContext.eventLoopExecutor());
    }

    private CompletionStage<Void> rmSingle(String objectKey, IExecutionContext execContext) {

        var request = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build();

        return client.deleteObject(request).thenApplyAsync(x -> null, execContext.eventLoopExecutor());
    }

    @Override
    public Flow.Publisher<ByteBuf> reader(String storagePath, IDataContext dataContext) {

        log.info("STORAGE OPERATION: {} {} [{}]", storageKey, READ_OPERATION, storagePath);

        var objectKey = resolvePath(storagePath, false, READ_OPERATION);

        return new S3ObjectReader(
                storageKey, storagePath, bucket, objectKey,
                client, dataContext.eventLoopExecutor(), errors);
    }

    @Override
    public Flow.Subscriber<ByteBuf> writer(String storagePath, CompletableFuture<Long> signal, IDataContext dataContext) {

        log.info("STORAGE OPERATION: {} {} [{}]", storageKey, WRITE_OPERATION, storagePath);

        var objectKey = resolvePath(storagePath, false, WRITE_OPERATION);

        return new S3ObjectWriter(
                storageKey, storagePath, bucket, objectKey,
                client, signal, dataContext.eventLoopExecutor(), errors);
    }

    private String resolvePath(String requestedPath, boolean allowRootDir, String operationName) {

        try {

            if (requestedPath == null || requestedPath.isBlank())
                throw errors.explicitError(STORAGE_PATH_NULL_OR_BLANK, requestedPath, operationName);

            var storagePath = StoragePath.forPath(requestedPath).normalize();

            if (storagePath.isAbsolute())
                throw errors.explicitError(STORAGE_PATH_NOT_RELATIVE, requestedPath, operationName);

            if (storagePath.startsWith(".."))
                throw errors.explicitError(STORAGE_PATH_OUTSIDE_ROOT, requestedPath, operationName);

            var absolutePath = prefix.resolve(storagePath).normalize();

            if (!prefix.contains(absolutePath))
                throw errors.explicitError(STORAGE_PATH_OUTSIDE_ROOT, requestedPath, operationName);

            if (absolutePath.equals(prefix) && !allowRootDir)
                throw errors.explicitError(STORAGE_PATH_IS_ROOT, requestedPath, operationName);

            log.info("root: {}, requested: {}, absolute: {}", prefix, requestedPath, absolutePath);

            // For bucket storage, do not use "/" for the root path
            // Otherwise everything gets put in a folder called "/"

            var objectKey = absolutePath.toString();

            return objectKey.startsWith("/")
                    ? objectKey.substring(1)
                    : objectKey;
        }
        catch (StoragePathException e) {

            throw errors.explicitError(STORAGE_PATH_INVALID, requestedPath, operationName);
        }
    }

    private CompletionStage<FileType> fileType(String objectKey, IExecutionContext execCtx) {

        if (objectKey.endsWith(BACKSLASH))
            return CompletableFuture.completedFuture(FileType.DIRECTORY);

        var dirKey = objectKey + BACKSLASH;

        return existsImpl(dirKey, execCtx).thenApplyAsync(
                exists -> exists ? FileType.DIRECTORY : FileType.FILE);
    }


    private <TResult> CompletionStage<TResult>
    dirProcessing(
            String requestedKey, String operation,
            Function<String, CompletionStage<TResult>> func,
            IExecutionContext execCtx) {

        var storageKey = StoragePath.forPath(requestedKey);

        if (storageKey.isDirectory()) {
            return func.apply(requestedKey);
        }
        else {

            var fileKey = requestedKey;
            var dirKey = requestedKey + BACKSLASH;

            var dirReq = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(dirKey)
                    .maxKeys(1)
                    .build();

            return client.listObjectsV2(dirReq)
                    .thenComposeAsync(response -> func.apply(dirKey), execCtx.eventLoopExecutor())
                    .exceptionally(error -> dirProcessingHandler(error, requestedKey, operation))
                    .thenComposeAsync(response -> func.apply(fileKey), execCtx.eventLoopExecutor());
        }
    }

    private <TResult> TResult
    dirProcessingHandler(Throwable error, String fileKey, String operation) {

        if (error instanceof CompletionException)
            error = error.getCause();

        if (error instanceof S3Exception) {
            var s3Error = (S3Exception) error;
            if (s3Error.statusCode() == HttpStatusCode.NOT_FOUND)
                return null;
        }

        throw errors.handleException(error, fileKey, operation);
    }
}
