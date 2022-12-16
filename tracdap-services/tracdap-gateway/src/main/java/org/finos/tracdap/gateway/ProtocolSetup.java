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

package org.finos.tracdap.gateway;

import io.netty.channel.ChannelInboundHandler;

import java.util.function.Function;


public class ProtocolSetup<TConfig> {

    private final Function<Integer, ChannelInboundHandler> supplier;
    private final TConfig config;

    public static <T> ProtocolSetup<T> setup(Function<Integer, ChannelInboundHandler> supplier) {
        return new ProtocolSetup<>(supplier);
    }

    public static <T> ProtocolSetup<T> setup(Function<Integer, ChannelInboundHandler> supplier, T config) {
        return new ProtocolSetup<>(supplier, config);
    }

    public ProtocolSetup(Function<Integer, ChannelInboundHandler> supplier) {
        this(supplier, null);
    }

    public ProtocolSetup(Function<Integer, ChannelInboundHandler> supplier, TConfig config) {
        this.supplier = supplier;
        this.config = config;
    }

    public ChannelInboundHandler create(int connId) {
        return this.supplier.apply(connId);
    }

    public TConfig config() {
        return this.config;
    }
}