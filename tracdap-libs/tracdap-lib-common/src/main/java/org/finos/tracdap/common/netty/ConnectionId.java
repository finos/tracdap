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

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.concurrent.atomic.AtomicLong;

public class ConnectionId {

    public static final AttributeKey<Long> CONNECTION_ID = AttributeKey.newInstance("conn-id");

    private final AtomicLong nextId = new AtomicLong();

    public void assign(Channel channel) {
        channel.attr(CONNECTION_ID).set(nextId.getAndIncrement());
    }

    public static long get(Channel channel) {
        var condId = channel.attr(CONNECTION_ID).get();
        return condId != null ? condId : -1L;
    }
}
