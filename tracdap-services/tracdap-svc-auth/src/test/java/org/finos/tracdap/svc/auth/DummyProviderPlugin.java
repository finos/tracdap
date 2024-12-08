/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.svc.auth;

import org.finos.tracdap.auth.provider.IAuthProvider;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.http.CommonHttpRequest;
import org.finos.tracdap.common.http.CommonHttpResponse;
import org.finos.tracdap.common.http.Http1Headers;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.ReferenceCountUtil;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class DummyProviderPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "DUMMY_AUTH";
    private static final String DUMMY_AUTH_PROVIDER = "DUMMY_AUTH_PROVIDER";

    private static final List<PluginServiceInfo> serviceInfo = List.of(
            new PluginServiceInfo(IAuthProvider.class, DUMMY_AUTH_PROVIDER, List.of("dummy")));

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return serviceInfo;
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T createService(String serviceName, Properties properties, ConfigManager configManager) {

        if (serviceName.equals(DUMMY_AUTH_PROVIDER)) {
            return (T) new DummyAuthProvider();
        }

        var message = String.format("Plugin [%s] does not support the service [%s]", pluginName(), serviceName);
        throw new EPluginNotAvailable(message);
    }

    private static class DummyAuthProvider implements IAuthProvider {

        public static String DUMMY_PATH_PREFIX = "/dummy/";

        @Override
        public boolean canHandleHttp1(HttpRequest request) {
            return request.uri().startsWith(DUMMY_PATH_PREFIX);
        }

        @Override
        public ChannelInboundHandler createHttp1Handler() {
            return new DummyAuthHandler();
        }

        @Override
        public boolean canHandleHttp2(Http2Headers headers) {
            return false;
        }

        @Override
        public ChannelInboundHandler createHttp2Handler() {
            return null;
        }
    }

    private static class DummyAuthHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {

            try {

                if (!(msg instanceof HttpObject)) {
                    ctx.close();
                    throw new EUnexpected();
                }

                if (msg instanceof HttpRequest) {

                    var request = (HttpRequest) msg;
                    var commonRequest = CommonHttpRequest.fromHttpRequest(request);
                    var commonResponse = DummyAuthLogic.processRequest(commonRequest);

                    var responseHeaders = Http1Headers.fromGenericHeaders(commonResponse.headers());

                    var response = new DefaultFullHttpResponse(
                            request.protocolVersion(),
                            commonResponse.status(),
                            commonResponse.content(),
                            responseHeaders.toHttpHeaders(),
                            EmptyHttpHeaders.INSTANCE);

                    ctx.writeAndFlush(response);
                }
            }
            finally {
                ReferenceCountUtil.release(msg);
            }
        }
    }

    private static class DummyAuthLogic {

        public static final String GET_TOKEN_ENDPOINT = "/dummy/get-token";

        static CommonHttpResponse processRequest(CommonHttpRequest request) {

            var uri = URI.create(request.path());
            var path = uri.getPath();
            var query = uri.getQuery();

            if (path.equals(GET_TOKEN_ENDPOINT)) {

                var status = HttpResponseStatus.OK;
                var headers = new Http1Headers();
                headers.add("x-dummy-token", "DUMMY_TOKEN");

                if (query != null) {

                    var parts = query.split("&");

                    var dummyStatus = Arrays.stream(parts)
                            .filter(p -> p.startsWith("dummy-status="))
                            .map(s -> s.replace("dummy-status=", ""))
                            .findFirst();

                    if (dummyStatus.isPresent()) {

                        var suppliedStatusCode = Integer.parseInt(dummyStatus.get());
                        var suppliedStatus = HttpResponseStatus.valueOf(suppliedStatusCode);

                        return new CommonHttpResponse(
                                suppliedStatus,
                                new Http1Headers(),
                                Unpooled.EMPTY_BUFFER);
                    }

                    var dummyParam = Arrays.stream(parts)
                            .filter(p -> p.startsWith("dummy-param="))
                            .map(s -> s.replace("dummy-param=", ""))
                            .findFirst();

                    if (dummyParam.isPresent()) {
                        headers.add("x-dummy-return", "true");
                        headers.add("x-dummy-param", dummyParam.get());
                    }
                }

                return new CommonHttpResponse(status, headers, Unpooled.EMPTY_BUFFER);
            }
            else {

                return new CommonHttpResponse(
                        HttpResponseStatus.NOT_FOUND,
                        new Http1Headers(),
                        Unpooled.EMPTY_BUFFER);
            }
        }
    }
}