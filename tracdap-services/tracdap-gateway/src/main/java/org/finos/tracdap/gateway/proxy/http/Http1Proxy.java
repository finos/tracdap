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

package org.finos.tracdap.gateway.proxy.http;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.GwRoute;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;

import java.net.URI;
import java.util.regex.Pattern;


public class Http1Proxy extends ChannelDuplexHandler {

    private final GwRoute routeConfig;
    private final Pattern matchPath;

    public Http1Proxy(GwRoute routeConfig) {
        this.routeConfig = routeConfig;
        this.matchPath = Pattern.compile("^" + routeConfig.getMatch().getPath());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        //log.info("intercept client write call for msg type {}", msg.getClass().getName());

        if (msg instanceof HttpRequest) {

            var sourceRequest = (HttpRequest) msg;
            var targetRequest = proxyRequest(sourceRequest);

            ctx.write(targetRequest, promise);
        }
        else if (msg instanceof HttpContent) {

            ctx.write(msg, promise);
        }
        else
            throw new EUnexpected();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        //log.info("intercept client read call for msg type {}", msg.getClass().getName());

        if (msg instanceof HttpResponse) {

            var serverResponse = (HttpResponse) msg;
            var proxyResponse = proxyResponse(serverResponse);

            ctx.fireChannelRead(proxyResponse);
        }
        else if (msg instanceof HttpContent) {

            ctx.fireChannelRead(msg);
        }
        else
            throw new EUnexpected();
    }

    // No-op proxy translation to get things working

    private HttpRequest proxyRequest(HttpRequest sourceRequest) {

        var sourceUri = URI.create(sourceRequest.uri());
        var sourcePath = sourceUri.getPath();
        // var sourceMatch = matchPath.matcher(sourcePath);

        // Match should already be checked before a request is sent to this handler
        if (!sourcePath.startsWith(routeConfig.getMatch().getPath()))
            throw new EUnexpected();

        //var targetPath = sourceMatch.replaceFirst(routeConfig.getTarget().getPath());
        var targetPath = sourcePath.replaceFirst(
                routeConfig.getMatch().getPath(),
                routeConfig.getTarget().getPath());

        var targetHeaders = new DefaultHttpHeaders();
        targetHeaders.add(sourceRequest.headers());
        targetHeaders.remove(HttpHeaderNames.HOST);
        targetHeaders.add(HttpHeaderNames.HOST, routeConfig.getTarget().getHost());

        if (sourceRequest instanceof FullHttpRequest) {

            var fullRequest = (FullHttpRequest) sourceRequest;

            return new DefaultFullHttpRequest(
                    sourceRequest.protocolVersion(),
                    sourceRequest.method(),
                    targetPath,
                    fullRequest.content(),
                    targetHeaders,
                    fullRequest.trailingHeaders());
        }
        else {

            return new DefaultHttpRequest(
                    sourceRequest.protocolVersion(),
                    sourceRequest.method(),
                    targetPath,
                    targetHeaders);
        }
    }

    private HttpResponse proxyResponse(HttpResponse serverResponse) {

        return serverResponse;
    }
}
