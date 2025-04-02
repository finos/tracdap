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

package org.finos.tracdap.svc.admin;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.finos.tracdap.api.AdminServiceProto;
import org.finos.tracdap.api.internal.InternalMessagingProto;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.netty.NettyHelpers;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.service.TracServiceBase;
import org.finos.tracdap.common.service.TracServiceConfig;
import org.finos.tracdap.common.util.RoutingUtils;
import org.finos.tracdap.common.validation.ValidationConcern;
import org.finos.tracdap.config.PlatformConfig;
import org.finos.tracdap.svc.admin.api.MessageProcessor;
import org.finos.tracdap.svc.admin.api.TracAdminApi;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.finos.tracdap.svc.admin.services.ConfigService;
import org.finos.tracdap.svc.admin.services.NotifierService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


public class TracAdminService extends TracServiceBase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PluginManager pluginManager;
    private final ConfigManager configManager;

    private ExecutorService executor;
    private ManagedChannel clientChannel;
    private NotifierService notifier;
    private Server server;


    public static void main(String[] args) {
        TracServiceBase.svcMain(TracAdminService.class, args);
    }

    public TracAdminService(PluginManager pluginManager, ConfigManager configManager) {
        this.pluginManager = pluginManager;
        this.configManager = configManager;
    }

    @Override
    protected void doStartup(Duration startupTimeout) {

        try {
            var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);
            var serviceConfig = platformConfig.getServicesOrThrow(ConfigKeys.ADMIN_SERVICE_KEY);
            var servicePort = serviceConfig.getPort();

            // Common framework for cross-cutting concerns
            var commonConcerns = buildCommonConcerns();

            var executor = NettyHelpers.threadPoolExecutor("admin-svc", 10, 10);
            this.executor = executor;
            executor.prestartAllCoreThreads();
            executor.allowCoreThreadTimeOut(false);

            var clientChannel = prepareClientChannel(platformConfig);
            var metadataClient = prepareMetadataClient(commonConcerns, clientChannel);

            var notifierService = new NotifierService(platformConfig, commonConcerns);
            var configService = new ConfigService(metadataClient, commonConcerns, notifierService);

            var adminApi = new TracAdminApi(configService);

            var serverBuilder = ServerBuilder
                    .forPort(servicePort)
                    .executor(executor)
                    .addService(adminApi)
                    .addService(new MessageProcessor());

            // Apply common concerns
            this.server = commonConcerns
                    .configureServer(serverBuilder)
                    .build();

            this.clientChannel = clientChannel;
            this.notifier = notifierService;

            // Good to go, let's start!
            this.server.start();
        }
        catch (IOException e) {

            // Wrap startup errors in an EStartup
            throw new EStartup(e.getMessage(), e);
        }
    }

    @Override
    protected int doShutdown(Duration shutdownTimeout) throws InterruptedException {

        var deadline = Instant.now().plus(shutdownTimeout);

        var serverDown = shutdownResource("Data service server", deadline, remaining -> {

            server.shutdown();
            return server.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var clientDown = shutdownResource("Metadata client", deadline, remaining -> {

            clientChannel.shutdown();
            return clientChannel.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        var notifierDown = shutdownResource("Notifier service", deadline, remainingTime -> {

            notifier.shutdown();
            return notifier.awaitTermination(remainingTime.toMillis(), TimeUnit.MILLISECONDS);
        });

        var executorDown = shutdownResource("Executor thread pool)", deadline, remaining -> {

            executor.shutdown();
            return executor.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS);
        });

        if (serverDown && clientDown && notifierDown && executorDown)
            return 0;

        if (!serverDown)
            server.shutdownNow();

        return -1;
    }

    private GrpcConcern buildCommonConcerns() {

        var commonConcerns = TracServiceConfig.coreConcerns(TracAdminService.class);

        // Validation concern for the APIs being served
        var validationConcern = new ValidationConcern(
                AdminServiceProto.getDescriptor(),
                InternalMessagingProto.getDescriptor());

        commonConcerns = commonConcerns.addAfter(TracServiceConfig.TRAC_PROTOCOL, validationConcern);

        // Additional cross-cutting concerns configured by extensions
        for (var extension : pluginManager.getExtensions()) {
            commonConcerns = extension.addServiceConcerns(commonConcerns, configManager, ConfigKeys.ADMIN_SERVICE_KEY);
        }

        return commonConcerns.build();
    }

    private ManagedChannel
    prepareClientChannel(PlatformConfig platformConfig) {

        var metadataTarget = RoutingUtils.serviceTarget(platformConfig, ConfigKeys.METADATA_SERVICE_KEY);

        log.info("Using (blocking) metadata service at [{}:{}]",
                metadataTarget.getHost(), metadataTarget.getPort());

        return ManagedChannelBuilder
                .forAddress(metadataTarget.getHost(), metadataTarget.getPort())
                .usePlaintext()
                .build();
    }

    private InternalMetadataApiGrpc.InternalMetadataApiBlockingStub
    prepareMetadataClient(GrpcConcern commonConcerns, ManagedChannel separateChannel) {

        var client = InternalMetadataApiGrpc.newBlockingStub(separateChannel);
        return commonConcerns.configureClient(client);
    }
}
