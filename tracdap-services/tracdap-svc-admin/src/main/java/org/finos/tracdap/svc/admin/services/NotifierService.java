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
import io.grpc.Context;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class NotifierService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    GrpcConcern commonConcerns;
    private final Map<String, InternalMessagingApiGrpc.InternalMessagingApiFutureStub> services;

    public NotifierService(PlatformConfig platformConfig, GrpcConcern commonConcerns) {
        this.commonConcerns = commonConcerns;
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

        // The current context will close and cancel pending requests when the server call returns
        // So, fork the context and use that fork to execute the async client calls

        var callCtx = Context.current().fork();

        // Client state only depends on the context, so can be prepared once for all notifiers

        var clientState = commonConcerns.prepareClientCall(callCtx);

        for (var serviceEntry : services.entrySet()) {

            var serviceKey = serviceEntry.getKey();
            var service = serviceEntry.getValue();

            // The notifier typically sends out a lot of requests all at once
            // Although they are small, waiting on them all could still starve the thread pool
            // Instead, use a future for each update and call back on the same forked context

            callCtx.run(() -> {

                var client = clientState.configureClient(service);
                var result = client.configUpdate(update);

                result.addListener(() -> configUpdateResult(serviceKey, update, result), callCtx::run);
            });
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
