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

package org.finos.tracdap.svc.admin.services;

import org.finos.tracdap.api.internal.ConfigUpdate;
import org.finos.tracdap.api.internal.InternalMessagingApiGrpc;
import org.finos.tracdap.api.internal.ReceivedStatus;
import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.util.RoutingUtils;
import org.finos.tracdap.config.PlatformConfig;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;


public class NotifierService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ExecutorService executor;
    private final Map<String, InternalMessagingApiGrpc.InternalMessagingApiFutureStub> services;

    public NotifierService(PlatformConfig platformConfig, GrpcConcern commonConcerns, ExecutorService executor) {
        this.executor = executor;
        this.services = buildServiceNotifierMap(platformConfig, commonConcerns);
    }

    private Map<String, InternalMessagingApiGrpc.InternalMessagingApiFutureStub>
    buildServiceNotifierMap(PlatformConfig platformConfig, GrpcConcern commonConcerns) {

        var services = new HashMap<String, InternalMessagingApiGrpc.InternalMessagingApiFutureStub>();

        for (var serviceEntry : platformConfig.getServicesMap().entrySet()) {

            var serviceKey = serviceEntry.getKey();
            var serviceConfig = serviceEntry.getValue();

            // Never send notifications to the gateway
            if (serviceKey.equals(ConfigKeys.GATEWAY_SERVICE_KEY))
                continue;

            // Do not send notifications to disabled services
            if (serviceConfig.hasEnabled() && !serviceConfig.getEnabled())
                continue;

            var serviceProps = new Properties();
            serviceProps.putAll(serviceConfig.getPropertiesMap());

            var wantMessages =  ConfigHelpers.optionalBoolean(serviceKey, serviceProps, "messaging.enabled", true);

            if (wantMessages) {
                var serviceNotifier = buildServiceNotifier(platformConfig, serviceKey, commonConcerns);
                services.put(serviceKey, serviceNotifier);
            }
        }

        return services;
    }

    private InternalMessagingApiGrpc.InternalMessagingApiFutureStub
    buildServiceNotifier(PlatformConfig platformConfig, String serviceKey, GrpcConcern commonConcerns) {

        var target = RoutingUtils.serviceTarget(platformConfig, serviceKey);

        var clientChannel = ManagedChannelBuilder
                .forAddress(target.getHost(), target.getPort())
                .usePlaintext()
                .build();

        var notifier = InternalMessagingApiGrpc.newFutureStub(clientChannel);

        log.info("Build notifier: {} service, address = {}, port = {}", serviceKey, target.getHost(), target.getPort());

        return commonConcerns.configureClient(notifier);
    }

    public void configUpdate(ConfigUpdate update) {

        // Offload notifications to run as a separate event, fire and forget
        // Once the metadata store is updated the change is "committed"
        // In the worst case if hot updates fail, services will see the new config on next refresh

        // Client calls started in the current (blocking) context use the gRPC context from the server call
        // So calls started inside the server's stack frame will appear cancelled when the stack unwinds

        executor.execute(() -> configUpdateOffloaded(update));
    }

    private void configUpdateOffloaded(ConfigUpdate update) {

        for (var serviceEntry : services.entrySet()) {

            var serviceKey = serviceEntry.getKey();
            var service = serviceEntry.getValue();

            var result = service.configUpdate(update);

            result.addListener(() -> configUpdateResult(serviceKey, update, result), Runnable::run);
        }
    }

    private void configUpdateResult(String serviceKey, ConfigUpdate update, ListenableFuture<ReceivedStatus> result) {

        try  {
            var status = result.get();

            log.info("NOTIFY CONFIG UPDATE: tenant = {} class = {}, key = {}, service = {}, result = {}",
                    update.getTenant(),
                    update.getConfigEntry().getConfigClass(),
                    update.getConfigEntry().getConfigKey(),
                    serviceKey, status.getCode().name());
        }
        catch (Exception error) {

            log.error("NOTIFY CONFIG UPDATE: tenant = {} class = {}, key = {}, service = {}, error = {}",
                    update.getTenant(),
                    update.getConfigEntry().getConfigClass(),
                    update.getConfigEntry().getConfigKey(),
                    serviceKey, error.getMessage(),
                    error);
        }
    }
}
