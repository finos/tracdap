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

package com.accenture.trac.gateway.routing;

import com.accenture.trac.common.exception.EUnexpected;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;


public class Http1Proxy extends ChannelDuplexHandler implements IServerRoute {

    @Override
    public void forwardMessage(Object msg, int seqId) {

    }

    @Override
    public void forwardError(Throwable error, int seqId) {

    }

    @Override
    public void channelError(Throwable error, int routeId) {

    }

    @Override
    public void prepareServerChannel() {

    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        //log.info("intercept client write call for msg type {}", msg.getClass().getName());

        if (msg instanceof HttpRequest) {

            var clientRequest = (HttpRequest) msg;
            var proxyRequest = proxyRequest(clientRequest);

            ctx.write(proxyRequest, promise);
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

    private HttpRequest proxyRequest(HttpRequest clientRequest) {

        return clientRequest;
    }

    private HttpResponse proxyResponse(HttpResponse serverResponse) {

        return serverResponse;
    }
}
