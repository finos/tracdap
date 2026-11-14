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

package org.finos.tracdap.svc.data.api;

import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.api.internal.ReceivedCode;
import org.finos.tracdap.api.internal.ReceivedStatus;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.IPlatformConfigListener;
import org.finos.tracdap.metadata.PlatformConfigEntry;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;


class MessageProcessorTest {

    private static final String OFFLOAD_THREAD = "test-offload";

    private ExecutorService offloadExecutor;

    @BeforeEach
    void setup() {
        offloadExecutor = Executors.newSingleThreadExecutor(task -> new Thread(task, OFFLOAD_THREAD));
    }

    @AfterEach
    void shutdown() {
        offloadExecutor.shutdownNow();
    }

    private static PlatformConfigUpdate update(String configClass) {

        return PlatformConfigUpdate.newBuilder()
                .setUpdateType(ConfigUpdateType.UPDATE)
                .setConfigEntry(PlatformConfigEntry.newBuilder()
                        .setConfigClass(configClass)
                        .setConfigKey("license")
                        .build())
                .build();
    }

    @Test
    void platformConfigUpdate_listenerRunsOnOffloadExecutor() throws Exception {

        var registry = new PluginRegistry();
        var listenerThread = new CompletableFuture<String>();

        IPlatformConfigListener listener = receivedUpdate -> {
            listenerThread.complete(Thread.currentThread().getName());
            return ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();
        };

        registry.addNamedInstance(IPlatformConfigListener.class, "license", listener);

        var processor = new MessageProcessor(null, offloadExecutor, registry);
        var result = new FutureObserver();

        processor.platformConfigUpdate(update("license"), result);

        assertEquals(ReceivedCode.OK, result.reply.get(10, TimeUnit.SECONDS).getCode());
        assertEquals(OFFLOAD_THREAD, listenerThread.get(10, TimeUnit.SECONDS));
    }

    @Test
    void platformConfigUpdate_unregisteredClassIgnored() throws Exception {

        var processor = new MessageProcessor(null, offloadExecutor, new PluginRegistry());
        var result = new FutureObserver();

        processor.platformConfigUpdate(update("some_other_class"), result);

        assertEquals(ReceivedCode.IGNORED, result.reply.get(10, TimeUnit.SECONDS).getCode());
    }

    @Test
    void platformConfigUpdate_failingListenerRepliesWithError() {

        var registry = new PluginRegistry();

        IPlatformConfigListener listener = receivedUpdate -> {
            throw new IllegalStateException("listener failed");
        };

        registry.addNamedInstance(IPlatformConfigListener.class, "license", listener);

        var processor = new MessageProcessor(null, offloadExecutor, registry);
        var result = new FutureObserver();

        processor.platformConfigUpdate(update("license"), result);

        var error = assertThrows(Exception.class, () -> result.reply.get(10, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, error.getCause());
    }

    private static class FutureObserver implements StreamObserver<ReceivedStatus> {

        private final CompletableFuture<ReceivedStatus> reply = new CompletableFuture<>();
        private ReceivedStatus value;

        @Override public void onNext(ReceivedStatus value) { this.value = value; }
        @Override public void onError(Throwable t) { reply.completeExceptionally(t); }
        @Override public void onCompleted() { reply.complete(value); }
    }
}
