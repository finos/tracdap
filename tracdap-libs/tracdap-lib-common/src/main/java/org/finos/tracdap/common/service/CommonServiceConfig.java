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

package org.finos.tracdap.common.service;

import org.finos.tracdap.common.auth.*;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.middleware.CommonConcerns;
import org.finos.tracdap.common.middleware.GrpcClientConfig;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.grpc.*;
import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.config.PlatformConfig;

import io.grpc.Context;
import io.grpc.ServerBuilder;
import io.grpc.stub.AbstractStub;

import java.time.Duration;
import java.util.*;


public class CommonServiceConfig extends CommonConcerns<GrpcConcern> implements GrpcConcern {

    private static final String TRAC_SERVICE_CONFIG = "trac_service_config";

    public static CommonServiceConfig newConfig() {
        return new CommonServiceConfig();
    }

    @Override
    public GrpcConcern build() {

        var stageOrder = Collections.unmodifiableList(this.stageOrder);
        var stages = Collections.unmodifiableMap(this.stages);

        return new CommonServiceConfig(stageOrder, stages);
    }

    private CommonServiceConfig() {
        super();
    }

    private CommonServiceConfig(List<String> stageOrder, Map<String, GrpcConcern> stages) {
        super(stageOrder, stages);
    }

    @Override
    public String concernName() {
        return TRAC_SERVICE_CONFIG;
    }

    // Implement GrpcConcern by applying registered concerns in order

    @Override
    public ServerBuilder<? extends ServerBuilder<?>> configureServer(ServerBuilder<? extends ServerBuilder<?>> serverBuilder) {

        // Server concerns can contain interceptors, so they need to be added in reverse order
        // The last interceptor added is the first one fired

        for (var i = stageOrder.size() - 1; i >= 0; i--) {
            var stageName = stageOrder.get(i);
            var stage = stages.get(stageName);
            serverBuilder = stage.configureServer(serverBuilder);
        }

        return serverBuilder;
    }

    @Override
    public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {

        for (var stageName : stageOrder) {
            var stage = stages.get(stageName);
            clientStub = stage.configureClient(clientStub);
        }

        return clientStub;
    }

    @Override
    public GrpcClientConfig prepareClientCall(Context callContext) {

        var clientConfigs = new ArrayList<GrpcClientConfig>();

        for (var stageName : stageOrder) {
            var stage = stages.get(stageName);
            var stageCallConfig = stage.prepareClientCall(callContext);
            if (stageCallConfig != GrpcConcern.NOOP_CLIENT_CONFIG)
                clientConfigs.add(stageCallConfig);
        }

        return new ClientCallConcerns(clientConfigs);
    }

    private static class ClientCallConcerns implements GrpcClientConfig {

        private final List<GrpcClientConfig> clientConfigs;

        public ClientCallConcerns(List<GrpcClientConfig> clientConfigs) {
            this.clientConfigs = clientConfigs;
        }

        @Override
        public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {

            for (var config : clientConfigs)
                clientStub = config.configureClient(clientStub);

            return clientStub;
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // CORE CONCERNS
    // -----------------------------------------------------------------------------------------------------------------

    // gRPC config for core concerns that are part of the common library


    public static class TracProtocol implements GrpcConcern {

        @Override
        public String concernName() {
            return TRAC_PROTOCOL;
        }

        @Override
        public ServerBuilder<? extends ServerBuilder<?>> configureServer(ServerBuilder<? extends ServerBuilder<?>> serverBuilder) {

            return serverBuilder
                    .intercept(new RequestMetadataInterceptor())
                    .intercept(new CompressionServerInterceptor())
                    .intercept(new DelayedExecutionInterceptor());
        }

        @Override
        public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {

            return clientStub
                    .withCompression(CompressionClientInterceptor.COMPRESSION_TYPE)
                    .withInterceptors(new CompressionClientInterceptor());
        }
    }

    public static class Authentication implements GrpcConcern {

        private final AuthenticationConfig authConfig;
        private final JwtProcessor jwtProcessor;
        private final InternalAuthProvider internalAuthProvider;

        private final Duration sessionTimeout;

        public Authentication(ConfigManager configManager, Duration sessionTimeout) {

            var platformConfig = configManager.loadRootConfigObject(PlatformConfig.class);

            this.authConfig = platformConfig.getAuthentication();
            this.jwtProcessor = JwtSetup.createProcessor(platformConfig, configManager);
            this.internalAuthProvider = new InternalAuthProvider(jwtProcessor, authConfig);

            this.sessionTimeout = sessionTimeout;
        }

        @Override
        public String concernName() {
            return TRAC_AUTHENTICATION;
        }

        @Override
        public ServerBuilder<? extends ServerBuilder<?>> configureServer(ServerBuilder<? extends ServerBuilder<?>> serverBuilder) {

            return serverBuilder.intercept(new GrpcAuthValidator(authConfig, jwtProcessor));
        }

        @Override
        public GrpcClientConfig prepareClientCall(Context callContext) {

            var userInfo = GrpcAuthHelpers.currentUser(callContext);
            return new ClientConfig(userInfo);
        }

        private class ClientConfig implements GrpcClientConfig {

            private final UserInfo userInfo;

            public ClientConfig(UserInfo userInfo) {
                this.userInfo = userInfo;
            }

            @Override
            public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {

                var credentials = internalAuthProvider.createDelegateSession(userInfo, sessionTimeout);
                return clientStub.withCallCredentials(credentials);
            }
        }
    }

    public static class Logging implements GrpcConcern {

        private final Class<?> serviceClass;

        public Logging(Class<?> serviceClass) {
            this.serviceClass = serviceClass;
        }

        @Override
        public String concernName() {
            return TRAC_LOGGING;
        }

        @Override
        public ServerBuilder<? extends ServerBuilder<?>> configureServer(ServerBuilder<? extends ServerBuilder<?>> serverBuilder) {

            return serverBuilder.intercept(new LoggingServerInterceptor(serviceClass));
        }

        @Override
        public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {

            return clientStub.withInterceptors(new LoggingClientInterceptor(serviceClass));
        }
    }

    public static class ErrorHandling implements GrpcConcern {

        @Override
        public String concernName() {
            return TRAC_ERROR_HANDLING;
        }

        @Override
        public ServerBuilder<? extends ServerBuilder<?>> configureServer(ServerBuilder<? extends ServerBuilder<?>> serverBuilder) {

            return serverBuilder.intercept(new ErrorMappingInterceptor());
        }
    }

}
