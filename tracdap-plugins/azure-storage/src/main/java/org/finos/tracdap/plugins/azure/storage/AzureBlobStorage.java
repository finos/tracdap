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

package org.finos.tracdap.plugins.azure.storage;

import org.finos.tracdap.common.async.Flows;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.data.IExecutionContext;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETrac;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.storage.CommonFileStorage;
import org.finos.tracdap.common.storage.FileStat;
import org.finos.tracdap.common.storage.FileType;

import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.BinaryData;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.blob.models.*;

import io.netty.channel.EventLoopGroup;
import org.apache.arrow.memory.ArrowBuf;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public class AzureBlobStorage extends CommonFileStorage {

    public static final String STORAGE_ACCOUNT_PROPERTY = "storageAccount";
    public static final String CONTAINER_PROPERTY = "container";
    public static final String PREFIX_PROPERTY = "prefix";

    public static final String CREDENTIALS_PROPERTY = "credentials";
    public static final String CREDENTIALS_DEFAULT = "default";
    public static final String CREDENTIALS_ACCESS_KEY = "accessKey";
    public static final String ACCESS_KEY_PROPERTY = "accessKey";

    public static final String BLOB_ENDPOINT_TEMPLATE = "https://%s.blob.core.windows.net/";
    public static final Duration STARTUP_TIMEOUT = Duration.of(1, ChronoUnit.MINUTES);

    private static final boolean ALWAYS_OVERWRITE = true;

    // Azure puts a default limit on the size of delete batch operations
    // The limit can be changed, but we want the storage implementation to work out-of-the-box
    // So, use a batch size for delete operations that is inside the default limit

    private static final int DELETE_BATCH_SIZE = 250;

    private final String storageAccount;
    private final String container;
    private final String prefix;

    private final CredentialsProvider credentialsProvider;

    private BlobContainerAsyncClient containerClient;
    private BlobBatchAsyncClient batchClient;

    public AzureBlobStorage(String storageKey, Properties properties) {

        super(BUCKET_SEMANTICS, storageKey, properties, new AzureStorageErrors(storageKey));

        var storageAccount = properties.getProperty(STORAGE_ACCOUNT_PROPERTY);
        var container = properties.getProperty(CONTAINER_PROPERTY);
        var prefix = properties.getProperty(PREFIX_PROPERTY);

        this.storageAccount = storageAccount;
        this.container = container;
        this.prefix = normalizePrefix(prefix);

        this.credentialsProvider = prepareCredentials(properties);
    }

    private CredentialsProvider prepareCredentials(Properties properties) {

        var mechanism = properties.containsKey(CREDENTIALS_PROPERTY)
                ? properties.getProperty(CREDENTIALS_PROPERTY)
                : CREDENTIALS_DEFAULT;

        if (CREDENTIALS_DEFAULT.equalsIgnoreCase(mechanism)) {

            log.info("Using [{}] credentials mechanism", CREDENTIALS_DEFAULT);
            var credentials = new DefaultAzureCredentialBuilder().build();

            return builder -> builder.credential(credentials);
        }

        if (CREDENTIALS_ACCESS_KEY.equalsIgnoreCase(mechanism)) {

            log.info("Using [{}] credentials mechanism", CREDENTIALS_ACCESS_KEY);

            var accessKey = properties.getProperty(ACCESS_KEY_PROPERTY);
            var credentials = new StorageSharedKeyCredential(storageAccount, accessKey);

            return builder -> builder.credential(credentials);
        }

        var message = String.format("Unrecognised credentials mechanism: [%s]", mechanism);
        log.error(message);
        throw new EStartup(message);
    }

    @Override
    public void start(EventLoopGroup eventLoopGroup) {

        var endpoint = String.format(BLOB_ENDPOINT_TEMPLATE, storageAccount);

        var httpClient = new NettyAsyncHttpClientBuilder()
                .eventLoopGroup(eventLoopGroup)
                .build();

        var serviceClientBuilder = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .httpClient(httpClient);

        var serviceClient = credentialsProvider
                .setCredentials(serviceClientBuilder)
                .buildAsyncClient();

        containerClient = serviceClient.getBlobContainerAsyncClient(container);
        batchClient = new BlobBatchClientBuilder(serviceClient).buildAsyncClient();

        checkRootExists();
    }

    private void checkRootExists() {

        // todo use fsDirExists(".")

        boolean rootExists;

        if (prefix == null || prefix.isEmpty()) {

            var existsResult = containerClient.exists();
            var exists0 = existsResult.blockOptional(STARTUP_TIMEOUT);
            rootExists = exists0.isPresent() && exists0.get();
        }
        else {

            var listOptions = new ListBlobsOptions()
                    .setPrefix(prefix)
                    .setMaxResultsPerPage(1);

            var listResult = containerClient.listBlobs(listOptions);
            var listResult0 = listResult.blockFirst(STARTUP_TIMEOUT);
            rootExists = listResult0 != null;
        }

        if (!rootExists) {

            var message = String.format(
                    "Storage location does not exist: container = [%s], prefix = [%s]",
                    container, prefix);

            log.info(message);

            throw new EStartup(message);
        }
    }

    @Override
    public void stop() {

        // TODO: Azure client classes have no shutdown methods
    }

    @Override
    protected CompletionStage<Boolean> fsExists(String storagePath, IExecutionContext ctx) {

        var blob = usePrefix(storagePath);

        var blobClient = containerClient.getBlobAsyncClient(blob);

        var existsCall = blobClient.exists();

        return handle(existsCall, ctx,
                this::fsExistsCallback,
                error -> fsExistsError(storagePath, error));
    }

    private boolean fsExistsCallback(boolean exists) {

        return exists;
    }

    private ETrac fsExistsError(String storagePath, Throwable error) {

        return errors.handleException("EXISTS", storagePath, error);
    }

    @Override
    protected CompletionStage<Boolean> fsDirExists(String storagePath, IExecutionContext ctx) {

        var dirPrefix = usePrefix(storagePath);

        var listOptions = new ListBlobsOptions()
                .setPrefix(dirPrefix)
                .setMaxResultsPerPage(1);

        var listCall = containerClient.listBlobs(listOptions).any(blob -> true);

        return handle(listCall, ctx,
                this::fsExistsCallback,
                error -> fsExistsError(storagePath, error));
    }

    @Override
    protected CompletionStage<FileStat> fsGetFileInfo(String storagePath, IExecutionContext ctx) {

        var blobName = usePrefix(storagePath);
        var blobClient = containerClient.getBlobAsyncClient(blobName);

        var propertiesCall = blobClient.getProperties();

        return handle(propertiesCall, ctx,
                properties -> fsGetFileInfoCallback(storagePath, properties),
                error -> fsGetFileInfoError(storagePath, error));
    }

    private FileStat fsGetFileInfoCallback(String storagePath, BlobProperties properties) {

        var blobName = usePrefix(storagePath);

        return buildFileStat(blobName, properties);
    }

    private ETrac fsGetFileInfoError(String storagePath, Throwable error) {

        return errors.handleException("STAT", storagePath, error);
    }

    @Override
    protected CompletionStage<FileStat> fsGetDirInfo(String storagePath, IExecutionContext ctx) {

        var absolutePrefix = usePrefix(storagePath);

        var stat = buildDirStat(absolutePrefix);

        return CompletableFuture.completedFuture(stat);
    }

    @Override
    protected CompletionStage<List<FileStat>> fsListContents(String storagePath, String startAfter, int maxKeys, boolean recursive, IExecutionContext ctx) {

        var dirPrefix = usePrefix(storagePath);

        var listDetails = new BlobListDetails()
                .setRetrieveMetadata(true);

        var listOptions = new ListBlobsOptions()
                .setPrefix(dirPrefix)
                .setMaxResultsPerPage(maxKeys)
                .setDetails(listDetails);

        var listCall = recursive
                ? containerClient.listBlobs(listOptions).byPage().next()
                : containerClient.listBlobsByHierarchy(dirPrefix, listOptions).byPage().next();

        var excludeKey = startAfter != null
                ? startAfter : dirPrefix;

        return handle(listCall, ctx,
                page -> fsListContentsCallback(page, excludeKey),
                error -> fsListContentsError(storagePath, error));
    }

    private List<FileStat> fsListContentsCallback(PagedResponse<BlobItem> page, String excludeKey) {

        return page.getValue()
                .stream()
                .filter(blob -> ! blob.getName().equals(excludeKey))
                .map(blob -> buildFileStat(blob.getName(), blob.getProperties()))
                .collect(Collectors.toList());
    }

    private ETrac fsListContentsError(String storagePath, Throwable error) {

        return errors.handleException("LS", storagePath, error);
    }

    private FileStat buildFileStat(String blobName, BlobProperties properties) {

        var relativeName = removePrefix(blobName);

        var path = relativeName.endsWith(BACKSLASH)
                ? relativeName.substring(0, relativeName.length() - 1)
                : relativeName;

        var name = path.contains(BACKSLASH)
                ? path.substring(path.lastIndexOf(BACKSLASH) + 1)
                : path;

        var fileType = relativeName.endsWith(BACKSLASH)
                ? FileType.DIRECTORY
                : FileType.FILE;

        var size = fileType == FileType.FILE
                ? properties.getBlobSize()
                : 0;

        var mtime = properties.getLastModified() != null ? properties.getLastModified().toInstant() : null;
        var atime = properties.getLastAccessedTime() != null ? properties.getLastAccessedTime().toInstant() : null;

        return new FileStat(path, name, fileType, size, mtime, atime);
    }

    private FileStat buildFileStat(String blobName, BlobItemProperties properties) {

        var relativeName = removePrefix(blobName);

        var path = relativeName.endsWith(BACKSLASH)
                ? relativeName.substring(0, relativeName.length() - 1)
                : relativeName;

        var name = path.contains(BACKSLASH)
                ? path.substring(path.lastIndexOf(BACKSLASH) + 1)
                : path;

        var fileType = relativeName.endsWith(BACKSLASH)
                ? FileType.DIRECTORY
                : FileType.FILE;

        var size = fileType == FileType.FILE
                ? properties.getContentLength()
                : 0;

        var mtime = properties.getLastModified() != null ? properties.getLastModified().toInstant() : null;
        var atime = properties.getLastAccessedTime() != null ? properties.getLastAccessedTime().toInstant() : null;

        return new FileStat(path, name, fileType, size, mtime, atime);
    }

    private FileStat buildDirStat(String dirPrefix) {

        var relativePrefix = removePrefix(dirPrefix);

        var storagePath = relativePrefix.isEmpty()
                ? "."
                : relativePrefix.endsWith(BACKSLASH)
                ? relativePrefix.substring(0, relativePrefix.length() - 1)
                : relativePrefix;

        var name = storagePath.contains(BACKSLASH)
                ? storagePath.substring(storagePath.lastIndexOf(BACKSLASH) + 1)
                : storagePath;

        var fileType = FileType.DIRECTORY;
        var size = 0;

        return new FileStat(storagePath, name, fileType, size, /* mtime = */ null, /* atime = */ null);
    }

    @Override
    protected CompletionStage<Void> fsCreateDir(String storagePath, IExecutionContext ctx) {

        var dirPrefix = usePrefix(storagePath);

        var blobClient = containerClient.getBlobAsyncClient(dirPrefix);
        var blobData = BinaryData.fromBytes(new byte[0]);

        var uploadCall = blobClient.upload(blobData, ALWAYS_OVERWRITE);

        return handle(uploadCall, ctx, error -> fsCreateDirError(storagePath, error));
    }

    private ETrac fsCreateDirError(String storagePath, Throwable error) {

        return errors.handleException("MKDIR", storagePath, error);
    }

    @Override
    protected CompletionStage<Void> fsDeleteFile(String storagePath, IExecutionContext ctx) {

        var blobName = usePrefix(storagePath);
        var blobClient = containerClient.getBlobAsyncClient(blobName);

        var deleteCall = blobClient.delete();

        return handle(deleteCall, ctx, error -> fsDeleteFileError(storagePath, error));
    }

    private ETrac fsDeleteFileError(String storagePath, Throwable error) {

        return errors.handleException("RM", storagePath, error);
    }

    @Override
    protected CompletionStage<Void> fsDeleteDir(String storagePath, IExecutionContext ctx) {

        var deleteAllPages = fsDeleteDirPage(storagePath, null, ctx);

        return handle(deleteAllPages, ctx, error -> fsDeleteDirError(storagePath, error));
    }

    private Mono<Void> fsDeleteDirPage(String storagePath, String continuation, IExecutionContext ctx) {

        var dirPrefix = usePrefix(storagePath);

        var listOptions = new ListBlobsOptions()
                .setPrefix(dirPrefix)
                .setMaxResultsPerPage(DELETE_BATCH_SIZE);

        var listCall = continuation != null
                ? containerClient.listBlobs(listOptions, continuation).byPage()
                : containerClient.listBlobs(listOptions).byPage();

        var scheduler = AzureScheduling.schedulerFor(ctx.eventLoopExecutor());

        return listCall
                .publishOn(scheduler)
                .next()
                .flatMap(page -> fsDeleteDirContent(storagePath, continuation, page, ctx));
    }

    private Mono<Void> fsDeleteDirContent(String storagePath, String continuation, PagedResponse<BlobItem> blobs, IExecutionContext ctx) {

        if (blobs.getStatusCode() != 200) {
            if (continuation == null)
                throw errors.explicitError("RMDIR", storagePath, OBJECT_NOT_FOUND);
            else
                throw errors.explicitError("RMDIR", storagePath, IO_ERROR, "Expected more objects during delete");
        }

        if (blobs.getValue().isEmpty())
            return Mono.empty();

        var blobUrls = blobs.getValue()
                .stream()
                .map(BlobItem::getName)
                .map(containerClient::getBlobAsyncClient)
                .map(BlobAsyncClient::getBlobUrl)
                .collect(Collectors.toList());

        var deleteCall = batchClient.deleteBlobs(blobUrls, DeleteSnapshotsOptionType.INCLUDE).collectList();

        if (blobs.getContinuationToken() == null)
            return deleteCall.mapNotNull(result -> null);

        var scheduler = AzureScheduling.schedulerFor(ctx.eventLoopExecutor());

        return deleteCall
                .publishOn(scheduler)
                .then(fsDeleteDirPage(storagePath, blobs.getContinuationToken(), ctx));
    }

    private ETrac fsDeleteDirError(String storagePath, Throwable error) {

        return errors.handleException("RMDIR", storagePath, error);
    }

    @Override
    protected CompletionStage<ArrowBuf> fsReadChunk(String storagePath, long offset, int size, IDataContext ctx) {

        var blobName = usePrefix(storagePath);
        var blobClient = containerClient.getBlobAsyncClient(blobName);

        var readStream = new AzureBlobReader(
                blobClient, ctx, errors,
                storageKey, storagePath,
                offset, size, /* chunkSize = */ size);

        // TODO: This is common logic that could be moved into CommonFileStorage
        // The abstract API should include a method to request a read stream for a range

        var list = new ArrayList<ArrowBuf>(1);
        var collect = Flows.fold(readStream, (xs, x) -> { xs.add(x); return xs; }, list);

        return collect.thenApply(xs -> {

            // Should never happen, since chunk size = request size is set for the object reader
            if (xs.size() != 1)
                throw new EUnexpected();

            var chunk = (ArrowBuf) xs.get(0);

            // On GCP the read call uses offset and limit, it may return fewer bytes than requested
            if (chunk.readableBytes() < size)
                throw errors.explicitError(READ_OPERATION, storagePath, OBJECT_SIZE_TOO_SMALL);

            return chunk;

        }).exceptionally(e -> {

            list.forEach(ArrowBuf::close);
            throw e instanceof CompletionException ? (CompletionException) e : new CompletionException(e);
        });
    }

    @Override
    protected Flow.Publisher<ArrowBuf> fsOpenInputStream(String storagePath, IDataContext ctx) {

        var blobName = usePrefix(storagePath);
        var blobClient = containerClient.getBlobAsyncClient(blobName);

        return new AzureBlobReader(blobClient, ctx, errors, storageKey, storagePath);
    }

    @Override
    protected Flow.Subscriber<ArrowBuf> fsOpenOutputStream(String storagePath, CompletableFuture<Long> signal, IDataContext ctx) {

        var blobName = usePrefix(storagePath);
        var blobClient = containerClient.getBlobAsyncClient(blobName);

        return new AzureBlobWriter(blobClient, signal, ctx);
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

    private String usePrefix(String storagePath) {

        if (prefix.isEmpty())
            return storagePath;

        if (storagePath.isEmpty())
            return prefix;

        return prefix + storagePath;
    }

    private String removePrefix(String blobName) {

        // This method is only called on results of get/list operations using the prefix
        // It should never happen that those results don't start with the prefix

        if (!blobName.startsWith(prefix))
            throw new EUnexpected();

        if (prefix.isEmpty())
            return blobName;

        return blobName.substring(prefix.length());
    }

    @FunctionalInterface
    private interface CredentialsProvider {

        BlobServiceClientBuilder setCredentials(BlobServiceClientBuilder builder);
    }

    private <T, S> CompletionStage<S> handle(
            Mono<T> clientCall, IExecutionContext ctx,
            Function<T, S> handler,
            Function<Throwable, ETrac> errorHandler) {

        var scheduler = AzureScheduling.schedulerFor(ctx.eventLoopExecutor());

        return clientCall
                .publishOn(scheduler)
                .map(handler)
                .onErrorMap(errorHandler)
                .toFuture();
    }

    private <T> CompletionStage<Void> handle(
            Mono<T> clientCall, IExecutionContext ctx,
            Function<Throwable, ETrac> errorHandler) {

        var scheduler = AzureScheduling.schedulerFor(ctx.eventLoopExecutor());

        return clientCall
                .publishOn(scheduler)
                .mapNotNull(result -> (Void) null)
                .onErrorMap(errorHandler)
                .toFuture();
    }
}
