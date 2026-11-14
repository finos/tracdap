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

package org.finos.tracdap.svc.meta.api;

import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.api.internal.ReceivedCode;
import org.finos.tracdap.api.internal.ReceivedStatus;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.IPlatformConfigListener;
import org.finos.tracdap.metadata.PlatformConfigEntry;

import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.finos.tracdap.api.PlatformConfigListRequest;
import org.finos.tracdap.api.PlatformConfigListResponse;
import org.finos.tracdap.api.internal.InternalMessagingApiGrpc;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for MessageProcessor.platformConfigUpdate()'s generic dispatch: a registered
 * IPlatformConfigListener for the update's configClass receives it on the listener executor, an
 * unregistered configClass falls back to IGNORED, and a listener that throws gets an error reply.
 */
class MessageProcessorTest {

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
    void registeredListener_receivesUpdateOnListenerExecutor() {

        var registry = new PluginRegistry();
        var listenerThreads = new ArrayList<Thread>();

        IPlatformConfigListener listener = receivedUpdate -> {
            listenerThreads.add(Thread.currentThread());
            return ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();
        };

        registry.addNamedInstance(IPlatformConfigListener.class, "license", listener);

        var executor = new RecordingExecutor();
        var processor = new MessageProcessor(registry, executor);
        var result = new CapturingObserver();

        processor.platformConfigUpdate(update("license"), result);

        assertTrue(listenerThreads.isEmpty());
        assertNull(result.value);

        executor.runAll();

        assertEquals(1, listenerThreads.size());
        assertEquals(ReceivedCode.OK, result.value.getCode());
        assertTrue(result.completed);
    }

    @Test
    void unregisteredConfigClass_fallsBackToIgnored() {

        var registry = new PluginRegistry();
        var executor = new RecordingExecutor();
        var processor = new MessageProcessor(registry, executor);
        var result = new CapturingObserver();

        processor.platformConfigUpdate(update("some_other_class"), result);
        executor.runAll();

        assertEquals(ReceivedCode.IGNORED, result.value.getCode());
    }

    @Test
    void failingListener_repliesWithError() {

        var registry = new PluginRegistry();

        IPlatformConfigListener listener = receivedUpdate -> {
            throw new IllegalStateException("listener failed");
        };

        registry.addNamedInstance(IPlatformConfigListener.class, "license", listener);

        var executor = new RecordingExecutor();
        var processor = new MessageProcessor(registry, executor);
        var result = new CapturingObserver();

        processor.platformConfigUpdate(update("license"), result);
        executor.runAll();

        assertNull(result.value);
        assertInstanceOf(IllegalStateException.class, result.error);
    }

    @Test
    void moreUpdatesThanServerThreads_listenersCallingBackAllComplete() throws Exception {

        var serverExecutor = Executors.newFixedThreadPool(2);
        var listenerExecutor = Executors.newSingleThreadExecutor();
        var registry = new PluginRegistry();

        var server = ServerBuilder.forPort(0)
                .executor(serverExecutor)
                .addService(new MessageProcessor(registry, listenerExecutor))
                .addService(new LoopbackMetadataApi())
                .build().start();

        var channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();

        try {

            var loopback = InternalMetadataApiGrpc.newBlockingStub(channel);

            IPlatformConfigListener listener = receivedUpdate -> {
                loopback.listPlatformConfigEntries(PlatformConfigListRequest.newBuilder().setConfigClass("license").build());
                return ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();
            };

            registry.addNamedInstance(IPlatformConfigListener.class, "license", listener);

            var messaging = InternalMessagingApiGrpc.newFutureStub(channel);
            var replies = new ArrayList<com.google.common.util.concurrent.ListenableFuture<ReceivedStatus>>();

            for (var i = 0; i < 6; i++)
                replies.add(messaging.platformConfigUpdate(update("license")));

            for (var reply : replies)
                assertEquals(ReceivedCode.OK, reply.get(10, TimeUnit.SECONDS).getCode());
        }
        finally {
            channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            listenerExecutor.shutdownNow();
            serverExecutor.shutdownNow();
        }
    }

    private static class LoopbackMetadataApi extends InternalMetadataApiGrpc.InternalMetadataApiImplBase {

        @Override
        public void listPlatformConfigEntries(PlatformConfigListRequest request, StreamObserver<PlatformConfigListResponse> response) {
            response.onNext(PlatformConfigListResponse.getDefaultInstance());
            response.onCompleted();
        }
    }

    private static class RecordingExecutor implements Executor {

        private final List<Runnable> tasks = new ArrayList<>();

        @Override public void execute(Runnable task) { tasks.add(task); }

        void runAll() {
            var pending = List.copyOf(tasks);
            tasks.clear();
            pending.forEach(Runnable::run);
        }
    }

    private static class CapturingObserver implements StreamObserver<ReceivedStatus> {

        private ReceivedStatus value;
        private Throwable error;
        private boolean completed;

        @Override public void onNext(ReceivedStatus value) { this.value = value; }
        @Override public void onError(Throwable t) { this.error = t; }
        @Override public void onCompleted() { this.completed = true; }
    }
}
