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

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for MessageProcessor.platformConfigUpdate()'s generic dispatch: a registered
 * IPlatformConfigListener for the update's configClass receives it, an unregistered configClass
 * falls back to today's IGNORED behaviour unchanged.
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
    void registeredListener_receivesUpdate() {

        var registry = new PluginRegistry();

        IPlatformConfigListener listener = receivedUpdate ->
                ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();

        registry.addNamedInstance(IPlatformConfigListener.class, "license", listener);

        var processor = new MessageProcessor(registry);
        var result = new CapturingObserver();

        processor.platformConfigUpdate(update("license"), result);

        assertEquals(ReceivedCode.OK, result.value.getCode());
    }

    @Test
    void unregisteredConfigClass_fallsBackToIgnored() {

        var registry = new PluginRegistry();
        var processor = new MessageProcessor(registry);
        var result = new CapturingObserver();

        processor.platformConfigUpdate(update("some_other_class"), result);

        assertEquals(ReceivedCode.IGNORED, result.value.getCode());
    }

    private static class CapturingObserver implements StreamObserver<ReceivedStatus> {

        private ReceivedStatus value;

        @Override public void onNext(ReceivedStatus value) { this.value = value; }
        @Override public void onError(Throwable t) { throw new RuntimeException(t); }
        @Override public void onCompleted() {}
    }
}
