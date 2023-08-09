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

import com.azure.storage.common.StorageSharedKeyCredential;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.data.IExecutionContext;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.storage.CommonFileStorage;
import org.finos.tracdap.common.storage.FileStat;
import org.finos.tracdap.common.storage.FileType;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.util.BinaryData;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

import io.netty.channel.EventLoopGroup;
import org.apache.arrow.memory.ArrowBuf;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


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

    private final String storageAccount;
    private final String container;
    private final String prefix;

    private final CredentialsProvider credentialsProvider;

    private BlobContainerAsyncClient containerClient;

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

        return existsCall
                .toFuture()
                .handleAsync(this::fsExistsCallback, ctx.eventLoopExecutor());
    }

    private boolean fsExistsCallback(boolean exists, Throwable error) {

        if (error != null) {
            throw errors.handleException("EXISTS", "", error);  // todo
        }

        return exists;
    }

    @Override
    protected CompletionStage<Boolean> fsDirExists(String storagePath, IExecutionContext ctx) {

        var dirPrefix = usePrefix(storagePath);

        var listOptions = new ListBlobsOptions()
                .setPrefix(dirPrefix)
                .setMaxResultsPerPage(1);

        var listCall = containerClient.listBlobs(listOptions);

        return listCall
                .any(blob -> true)
                .toFuture()
                .handleAsync(this::fsExistsCallback, ctx.eventLoopExecutor());
    }

    @Override
    protected CompletionStage<FileStat> fsGetFileInfo(String storagePath, IExecutionContext ctx) {

        var blobName = usePrefix(storagePath);

        var blobClient = containerClient.getBlobAsyncClient(blobName);

        var propsCall = blobClient.getProperties();

        return propsCall
                .toFuture()
                .handleAsync((props, error) -> fsGetFileInfoCallback(storagePath, props, error));
    }

    private FileStat fsGetFileInfoCallback(String storagePath, BlobProperties properties, Throwable error) {

        if (error != null) {
            throw errors.handleException("STAT", storagePath, error);
        }

        var blobName = usePrefix(storagePath);

        return buildFileStat(blobName, properties);
    }

    @Override
    protected CompletionStage<FileStat> fsGetDirInfo(String prefix, IExecutionContext ctx) {
        return null;
    }

    @Override
    protected CompletionStage<List<FileStat>> fsListContents(String prefix, String startAfter, int maxKeys, boolean recursive, IExecutionContext ctx) {
        return null;
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

        var mtime = properties.getLastModified().toInstant();
        var atime = properties.getLastAccessedTime().toInstant();

        return new FileStat(path, name, fileType, size, mtime, atime);
    }

    @Override
    protected CompletionStage<Void> fsCreateDir(String storagePath, IExecutionContext ctx) {

        var dirPrefix = usePrefix(storagePath);

        var blobClient = containerClient.getBlobAsyncClient(dirPrefix);
        var blobData = BinaryData.fromBytes(new byte[0]);

        var uploadCall = blobClient.upload(blobData);

        return uploadCall
                .toFuture()
                .handleAsync(this::fsCreateDirCallback, ctx.eventLoopExecutor());
    }

    private Void fsCreateDirCallback(BlockBlobItem blobItem, Throwable error) {

        if (error != null) {
            throw errors.handleException("MKDIR", "", error);  // todo
        }

        return null;
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
}
