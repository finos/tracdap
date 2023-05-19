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

package org.finos.tracdap.webserver;

import io.netty.buffer.NettyArrowBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.finos.tracdap.common.async.Flows;
import org.finos.tracdap.common.data.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.*;
import org.finos.tracdap.common.storage.FileStat;
import org.finos.tracdap.common.storage.FileType;
import org.finos.tracdap.common.storage.IFileStorage;
import org.finos.tracdap.config.WebServerConfig;
import org.finos.tracdap.config.WebServerRedirect;
import org.finos.tracdap.config.WebServerRewriteRule;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.regex.Pattern;


public class ContentServer {

    private static final String INDEX_DOC = "index.html";
    private static final Pattern EXTENSION_PATTERN = Pattern.compile(".*\\.([^/?#]+\\Z)");

    private static final long CACHE_CONTROL_MAX_AGE = 3600;

    private final IFileStorage storage;

    private final Map<String, String> mimeTypes;
    private final List<Map.Entry<Pattern, WebServerRedirect>> redirects;
    private final List<Map.Entry<Pattern, WebServerRewriteRule>> rewriteRules;

    public ContentServer(WebServerConfig config, IFileStorage storage) {

        this.storage = storage;

        this.mimeTypes = MimeTypes.loadMimeTypeMap();

        this.redirects = new ArrayList<>();
        this.rewriteRules = new ArrayList<>();

        for (var redirect : config.getRedirectsList()) {
            var source = Pattern.compile(redirect.getSource());
            redirects.add(Map.entry(source, redirect));
        }

        for (var rule : config.getRewriteRulesList()) {
            var source = Pattern.compile(rule.getSource());
            rewriteRules.add(Map.entry(source, rule));
        }
    }

    public CompletionStage<ContentResponse> headRequest(String requestUri, IExecutionContext execCtx) {

        var redirect = processRedirects(requestUri);

        if (redirect != null)
            return CompletableFuture.completedFuture(redirect);

        var storagePath = translateStoragePath(requestUri);

        return headRequestForPath(storagePath, execCtx);
    }

    private CompletionStage<ContentResponse> headRequestForPath(String storagePath, IExecutionContext execCtx) {

        return storage.stat(storagePath, execCtx)
                .thenCompose(fileStat -> resolveDirectories(fileStat, execCtx))
                .thenApply(this::buildHeadResponse)
                .exceptionally(this::buildErrorResponse)
                .thenApplyAsync(Function.identity(), execCtx.eventLoopExecutor());
    }

    public CompletionStage<ContentResponse> getRequest(String requestUri, IDataContext dataCtx) {

        var redirect = processRedirects(requestUri);

        if (redirect != null)
            return CompletableFuture.completedFuture(redirect);

        var storagePath = translateStoragePath(requestUri);

        return storage.stat(storagePath, dataCtx)
                .thenCompose(fileStat -> resolveDirectories(fileStat, dataCtx))
                .thenApply(fileStat -> buildContentResponse(fileStat, dataCtx))
                .exceptionally(this::buildErrorResponse)
                .thenApplyAsync(Function.identity(), dataCtx.eventLoopExecutor());
    }

    private ContentResponse processRedirects(String requestUri) {

        try {
            var uri = new URI(requestUri);
            var path = uri.getPath();

            for (var redirect : redirects) {

                var pattern = redirect.getKey();
                var match = pattern.matcher(path);

                if (match.matches())
                    return buildRedirectResponse(redirect.getValue());
            }

            return null;
        }
        catch (URISyntaxException e) {
            throw new ENetworkHttp(HttpResponseStatus.BAD_REQUEST.code(), "Invalid URL: " + e.getMessage(), e);
        }
    }

    private String translateStoragePath(String requestUri) {

        try {
            var uri = new URI(requestUri);
            var path = uri.getPath();

            for (var rule : rewriteRules) {

                var pattern = rule.getKey();
                var match = pattern.matcher(path);
                var rewrite = rule.getValue();

                if (match.matches())
                    path = match.replaceFirst(rewrite.getTarget());
            }

            if (path.equals("/"))
                return INDEX_DOC;

            if (path.endsWith("/"))
                return path.substring(1) + INDEX_DOC;

            return path.substring(1);
        }
        catch (URISyntaxException e) {
            throw new ENetworkHttp(HttpResponseStatus.BAD_REQUEST.code(), "Invalid URL: " + e.getMessage(), e);
        }
    }

    private CompletionStage<FileStat> resolveDirectories(FileStat fileStat, IExecutionContext execCtx) {

        if (fileStat.fileType == FileType.FILE)
            return CompletableFuture.completedFuture(fileStat);

        var dirPath = fileStat.storagePath;
        var indexPath = dirPath.endsWith("/") ? dirPath + INDEX_DOC : dirPath + "/" + INDEX_DOC;

        return storage.stat(indexPath, execCtx);
    }

    private ContentResponse buildHeadResponse(FileStat fileStat) {

        if (fileStat.fileType != FileType.FILE)
            throw new EUnexpected();

        var response = new ContentResponse();

        response.statusCode = HttpResponseStatus.OK;
        response.headers.set(HttpHeaderNames.CONTENT_LENGTH, fileStat.size);

        // Try to match the file extension and set the content-type header

        var extensionMatch = EXTENSION_PATTERN.matcher(fileStat.storagePath);

        if (extensionMatch.matches()) {

            var extension = extensionMatch.group(1);
            var mimeType = mimeTypes.get(extension);

            if (mimeType != null)
                response.headers.set(HttpHeaderNames.CONTENT_TYPE, mimeType);
        }

        // Common headers that need to be set on every response

        addStandardHeaders(response.headers);

        return response;
    }

    private ContentResponse buildContentResponse(FileStat fileStat, IDataContext dataCtx) {

        var response = buildHeadResponse(fileStat);
        var reader = storage.reader(fileStat.storagePath, dataCtx);

        response.reader = Flows.map(reader, NettyArrowBuf::unwrapBuffer);

        return response;
    }

    private ContentResponse buildErrorResponse(Throwable e) {

        var response = new ContentResponse();

        // Unwrap concurrent completion errors
        if (e instanceof CompletionException)
            e = e.getCause();

        if (e instanceof EStorageRequest) {
            response.statusCode = HttpResponseStatus.NOT_FOUND;
            return response;
        }

        if (e instanceof EStorageCommunication) {
            response.statusCode = HttpResponseStatus.SERVICE_UNAVAILABLE;
            return response;
        }

        response.statusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        return response;
    }

    private ContentResponse buildRedirectResponse(WebServerRedirect redirect) {

        var response = new ContentResponse();
        response.statusCode = HttpResponseStatus.valueOf(redirect.getStatus());
        response.headers.set(HttpHeaderNames.LOCATION, redirect.getTarget());

        return response;
    }

    private void addStandardHeaders(HttpHeaders headers) {

        var date = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now());
        headers.set(HttpHeaderNames.DATE, date);

        var cacheControl = String.format("max-age=%d", CACHE_CONTROL_MAX_AGE);
        headers.set(HttpHeaderNames.CACHE_CONTROL, cacheControl);
    }
}
