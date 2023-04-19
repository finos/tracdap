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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public class S3ObjectStorage extends CommonFileStorage {

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
    private final Properties properties;

    private final String bucket;
    private final String prefix;
    private final Region region;
    private final URI endpoint;

    private final StorageErrors errors;

    // private final AwsCredentialsProvider credentials;
    private S3AsyncClient client;


    public S3ObjectStorage(String storageKey, Properties properties) {

        super("s3", storageKey, new S3StorageErrors(storageKey, LoggerFactory.getLogger(S3ObjectStorage.class)));

        this.storageKey = storageKey;
        this.properties = properties;

        var bucket = properties.getProperty(BUCKET_PROPERTY);
        var prefix = properties.getProperty(PREFIX_PROPERTY);
        var region = properties.getProperty(REGION_PROPERTY);
        var endpoint = properties.getProperty(ENDPOINT_PROPERTY);

        this.bucket = bucket;
        this.prefix = normalizePrefix(prefix);
        this.region = region != null && !region.isBlank() ? Region.of(region) : null;
        this.endpoint = endpoint != null && !endpoint.isBlank() ? URI.create(endpoint) : null;

        this.errors = new S3StorageErrors(storageKey, log);
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

        log.info("INIT [{}], fs = [S3], bucket = [{}], prefix = [{}]", storageKey, bucket, prefix);

        var credentials = setupCredentials(properties);

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
    }

    @Override
    public void stop() {

        log.info("STOP [{}], fs = [S3], bucket = [{}], prefix = [{}]", storageKey, bucket, prefix);

        client.close();
    }

    @Override
    protected CompletionStage<Boolean>
    objectExists(String objectKey, IExecutionContext ctx) {

        var absoluteKey = usePrefix(objectKey);

        var request = HeadObjectRequest.builder()
                .bucket(this.bucket)
                .key(absoluteKey)
                .build();

        var response = useContext(ctx, client.headObject(request));

        return response
                .thenApply(x -> true)
                .exceptionally(this::objectExistsError);
    }

    private boolean
    objectExistsError(Throwable e) {

        var s3Error =
                (e instanceof S3Exception) ? (S3Exception) e :
                (e.getCause() instanceof S3Exception) ? (S3Exception) e.getCause() :
                null;

        if (s3Error != null && s3Error.statusCode() == HttpStatusCode.NOT_FOUND)
            return false;

        if (e instanceof CompletionException)
            throw (CompletionException) e;
        else
            throw new CompletionException(e);
    }

    @Override
    protected CompletionStage<Boolean>
    prefixExists(String directoryKey, IExecutionContext ctx) {

        var absoluteDir = usePrefix(directoryKey);

        var request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(absoluteDir)
                .maxKeys(1)
                .build();

        var response = useContext(ctx, client.listObjectsV2(request));

        return response.thenApply(ListObjectsV2Response::hasContents);
    }

    @Override
    protected CompletionStage<FileStat>
    objectStat(String objectKey, IExecutionContext ctx) {

        var absoluteKey = usePrefix(objectKey);

        var request = GetObjectAttributesRequest.builder()
                .bucket(bucket)
                .key(absoluteKey)
                .objectAttributes(ObjectAttributes.OBJECT_SIZE)
                .build();

        var response = useContext(ctx, client.getObjectAttributes(request));

        return response.thenApply(attrs -> attrsToFileStat(objectKey, attrs));
    }

    @Override
    protected CompletionStage<FileStat>
    prefixStat(String directoryKey, IExecutionContext ctx) {

        var storagePath = directoryKey.isEmpty() ? "." : directoryKey.substring(0, directoryKey.length() - 1);

        var name = storagePath.contains(BACKSLASH)
                ? storagePath.substring(storagePath.lastIndexOf(BACKSLASH) + 1)
                : storagePath;

        var fileType = FileType.DIRECTORY;
        var size = 0;

        var stat = new FileStat(storagePath, name, fileType, size, /* mtime = */ null, /* atime = */ null);

        return CompletableFuture.completedFuture(stat);
    }

    @Override
    protected CompletionStage<List<FileStat>>
    prefixLs(String directoryKey, String startAfter, int maxKeys, boolean recursive, IExecutionContext ctx) {

        var absoluteDir = usePrefix(directoryKey);

        var request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(absoluteDir)
                .maxKeys(maxKeys);

        if (startAfter != null)
            request.startAfter(startAfter);

        if (!recursive)
            request.delimiter(BACKSLASH);

        // Send request and get response onto the EL for execContext
        var response = useContext(ctx, client.listObjectsV2(request.build()));

        return response.thenApply(result -> prefixLsResult(directoryKey, absoluteDir, result));
    }

    private List<FileStat>
    prefixLsResult(String directoryKey, String absoluteDir, ListObjectsV2Response response) {

        // If no objects / prefixes are matched, the "directory" doesn't exist
        if (response.contents().isEmpty() && response.commonPrefixes().isEmpty()) {
            throw errors.explicitError(OBJECT_NOT_FOUND, directoryKey, LS_OPERATION);
        }

        var objects = response.contents();
        var prefixes = response.commonPrefixes();

        // Do not include the directory being listed, if it shows up as the first entry in the list

        if (!objects.isEmpty() && objects.get(0).key().equals(absoluteDir))
            objects = objects.subList(1, objects.size());

        if (!prefixes.isEmpty() && prefixes.get(0).prefix().equals(absoluteDir))
            prefixes = prefixes.subList(1, prefixes.size());

        // Create FileStat for both objects and prefixes

        var objectStats = objects.stream().map(this::objectToFileStat);
        var prefixStats = prefixes.stream().map(this::prefixToFileStat);

        return Stream
                .concat(objectStats, prefixStats)
                .collect(Collectors.toList());
    }

    @Override
    protected CompletionStage<Void>
    prefixMkdir(String directoryKey, IExecutionContext execContext) {

        var absoluteDir = usePrefix(directoryKey);

        var request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(absoluteDir)
                .contentLength(0L)
                .build();

        var content = AsyncRequestBody.empty();

        // Send request and get response onto the EL for execContext
        var response = useContext(execContext, client.putObject(request, content));

        return response.thenAccept(result -> {});
    }


    @Override
    protected CompletionStage<Void> fsDeleteFile(String objectKey, IExecutionContext ctx) {

        var absoluteKey = usePrefix(objectKey);

        var request = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(absoluteKey)
                .build();

        var response = useContext(ctx, client.deleteObject(request));

        return response.thenApply(x -> null);
    }

    @Override
    protected CompletionStage<Void> fsDeleteDir(String directoryKey, IExecutionContext ctx) {

        return fsDeleteDir(directoryKey, true, ctx);
    }

    protected CompletionStage<Void> fsDeleteDir(String directoryKey, boolean firstPass, IExecutionContext ctx) {

        var absoluteDir = usePrefix(directoryKey);

        var listRequest = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(absoluteDir)
                .build();

        // Send request and get response onto the EL for execContext
        var listResponse = useContext(ctx, client.listObjectsV2(listRequest));

        // TODO: Handle contents more than one page, use list objects iterator, or post back to event loop

        return listResponse.thenCompose(list -> {

            if (!list.hasContents()) {
                if (firstPass)
                    throw errors.explicitError(OBJECT_NOT_FOUND, directoryKey, "RMDIR");
                else
                    return CompletableFuture.completedFuture(0).thenApply(x -> null);
            }

            return fsDeleteDirContents(list, ctx);
        });
    }

    private CompletionStage<Void>
    fsDeleteDirContents(ListObjectsV2Response contents, IExecutionContext ctx) {

        var objIds = contents.contents()
                .stream()
                .map(obj -> ObjectIdentifier.builder().key(obj.key()).build())
                .collect(Collectors.toList());

        var request = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(del -> del.objects(objIds))
                .build();

        var response = useContext(ctx, client.deleteObjects(request));

        return response.thenApply(x -> {

            var y = 1;
            return null;
        });
    }

    private FileStat
    attrsToFileStat(String objectKey, GetObjectAttributesResponse objectAttrs) {

        var storagePath =
                objectKey.isEmpty() ? "." :
                objectKey.endsWith(BACKSLASH) ? objectKey.substring(0, objectKey.length() - 1) :
                objectKey;

        var name = storagePath.contains(BACKSLASH)
                ? storagePath.substring(storagePath.lastIndexOf(BACKSLASH) + 1)
                : storagePath;

        var fileType = objectKey.endsWith(BACKSLASH) ? FileType.DIRECTORY : FileType.FILE;
        var size = objectAttrs.objectSize();
        var mtime = objectAttrs.lastModified();

        return new FileStat(storagePath, name, fileType, size, mtime, /* atime = */ null);
    }

    private FileStat
    objectToFileStat(S3Object s3Object) {

        var path = s3Object.key().substring(prefix.length());
        var name = path.contains(BACKSLASH) ? path.substring(path.lastIndexOf(BACKSLASH) + 1) : path;
        var fileType = s3Object.key().endsWith(BACKSLASH) ? FileType.DIRECTORY : FileType.FILE;
        var size = fileType == FileType.FILE ? s3Object.size() : 0;
        var mtime = fileType == FileType.FILE ? s3Object.lastModified() : null;

        return new FileStat(path, name, fileType, size, mtime, /* atime = */ null);
    }

    private FileStat
    prefixToFileStat(CommonPrefix s3Directory) {

        // Remove the last character from directory names, which is the trailing backslash
        var path = s3Directory.prefix().substring(prefix.length(), s3Directory.prefix().length() - 1);
        var name = path.contains(BACKSLASH) ? path.substring(path.lastIndexOf(BACKSLASH) + 1) : path;
        var fileType = FileType.DIRECTORY;
        var size = 0;

        return new FileStat(path, name, fileType, size, /* mtime = */ null, /* atime = */ null);
    }

    @Override
    public Flow.Publisher<ByteBuf> reader(String storagePath, IDataContext dataContext) {

        log.info("{} {} [{}]", READ_OPERATION, storageKey, storagePath);

        var objectKey = usePrefix(storagePath);

        return new S3ObjectReader(
                storageKey, storagePath, bucket, objectKey,
                client, dataContext.eventLoopExecutor(), errors);
    }

    @Override
    public Flow.Subscriber<ByteBuf> writer(String storagePath, CompletableFuture<Long> signal, IDataContext dataContext) {

        log.info("{} {} [{}]", WRITE_OPERATION, storageKey, storagePath);

        var objectKey = usePrefix(storagePath);

        return new S3ObjectWriter(
                storageKey, storagePath, bucket, objectKey,
                client, signal, dataContext.eventLoopExecutor(), errors);
    }

    private String usePrefix(String relativeKey) {

        if (prefix.isEmpty())
            return relativeKey;

        if (relativeKey.isEmpty())
            return prefix;

        return prefix + relativeKey;
    }
}
