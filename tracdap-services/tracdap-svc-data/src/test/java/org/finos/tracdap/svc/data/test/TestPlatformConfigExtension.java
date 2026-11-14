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

package org.finos.tracdap.svc.data.test;

import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.api.internal.ReceivedCode;
import org.finos.tracdap.api.internal.ReceivedStatus;
import org.finos.tracdap.common.plugin.ITracExtension;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.IPlatformConfigListener;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class TestPlatformConfigExtension implements ITracExtension {

    public static final String TEST_CONFIG_CLASS = "test_platform_config";

    private static final List<PlatformConfigUpdate> receivedUpdates = new CopyOnWriteArrayList<>();

    public static List<PlatformConfigUpdate> receivedUpdates() {
        return List.copyOf(receivedUpdates);
    }

    @Override
    public String extensionName() {
        return "TEST_PLATFORM_CONFIG";
    }

    @Override
    public void runStartupLogic(PluginRegistry registry) {

        IPlatformConfigListener listener = update -> {
            receivedUpdates.add(update);
            return ReceivedStatus.newBuilder().setCode(ReceivedCode.OK).build();
        };

        registry.addNamedInstance(IPlatformConfigListener.class, TEST_CONFIG_CLASS, listener);
    }
}
