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

package org.finos.tracdap.common.netty;

import org.finos.tracdap.common.exception.ETracInternal;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;

import java.util.function.Function;
import java.util.function.Supplier;


public class ProtocolHandler {

    private final Supplier<ChannelHandler> httpSupplier;
    private final Supplier<ChannelHandler> http2Supplier;
    private final Supplier<ChannelHandler> wsSupplier;
    private final Function<HttpRequest, WebSocketServerProtocolConfig> wsConfigSupplier;

    public static ProtocolHandler create() {
        return new ProtocolHandler(null, null, null, null);
    }

    public ProtocolHandler withHttp(Supplier<ChannelHandler> httpSupplier) {
        return new ProtocolHandler(httpSupplier, this.http2Supplier, this.wsSupplier, this.wsConfigSupplier);
    }

    public ProtocolHandler withHttp2(Supplier<ChannelHandler> http2Supplier) {
        return new ProtocolHandler(this.httpSupplier, http2Supplier, this.wsSupplier, this.wsConfigSupplier);
    }

    public ProtocolHandler withWebsocket(Supplier<ChannelHandler> wsSupplier, Function<HttpRequest, WebSocketServerProtocolConfig> wsConfigSupplier) {
        return new ProtocolHandler(this.httpSupplier, this.http2Supplier, wsSupplier, wsConfigSupplier);
    }

    private ProtocolHandler(
            Supplier<ChannelHandler> httpSupplier,
            Supplier<ChannelHandler> http2Supplier,
            Supplier<ChannelHandler> wsSupplier,
            Function<HttpRequest, WebSocketServerProtocolConfig> wsConfigSupplier) {

        this.httpSupplier = httpSupplier;
        this.http2Supplier = http2Supplier;
        this.wsSupplier = wsSupplier;
        this.wsConfigSupplier = wsConfigSupplier;
    }

    boolean httpSupported() {
        return httpSupplier != null;
    }

    ChannelHandler createHttpHandler() {

        if (!httpSupported())
            throw new ETracInternal("Protocol not supported: HTTP / 1");

        return httpSupplier.get();
    }

    boolean http2Supported() {
        return http2Supplier != null;
    }

    ChannelHandler createHttp2Handler() {

        if (!http2Supported())
            throw new ETracInternal("Protocol not supported: HTTP / 2");

        return http2Supplier.get();
    }

    boolean websocketSupported() {
        return wsSupplier != null;
    }

    ChannelHandler createWebsocketHandler() {

        if (!websocketSupported())
            throw new ETracInternal("Protocol not supported: WS");

        return wsSupplier.get();
    }

    WebSocketServerProtocolConfig createWebSocketConfig(HttpRequest request) {

        if (!websocketSupported())
            throw new ETracInternal("Protocol not supported: WS");

        return wsConfigSupplier.apply(request);
    }
}
