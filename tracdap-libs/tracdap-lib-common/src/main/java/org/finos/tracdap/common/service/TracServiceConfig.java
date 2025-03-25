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

import io.grpc.ManagedChannelBuilder;
import org.finos.tracdap.common.middleware.CommonConcerns;
import org.finos.tracdap.common.middleware.CommonGrpcConcerns;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.grpc.*;

import io.grpc.ServerBuilder;
import io.grpc.stub.AbstractStub;


public class TracServiceConfig {

    public static final String TRAC_SERVICE_CONFIG = "trac_service_config";
    public static final String TRAC_PROTOCOL = "trac_protocol";
    public static final String TRAC_VALIDATION = "trac_validation";
    public static final String TRAC_LOGGING = "trac_logging";
    public static final String TRAC_ERROR_HANDLING = "trac_error_handling";

    public static CommonConcerns<GrpcConcern> emptyConfig() {
        return new CommonGrpcConcerns(TRAC_SERVICE_CONFIG);
    }

    public static CommonConcerns<GrpcConcern> coreConcerns(Class<?> serviceClass) {

        return emptyConfig()
                .addLast(new TracServiceConfig.TracProtocol())
                .addLast(new TracServiceConfig.Logging(serviceClass))
                .addLast(new TracServiceConfig.ErrorHandling());
    }

    public static class TracProtocol implements GrpcConcern {

        @Override
        public String concernName() {
            return TRAC_PROTOCOL;
        }

        @Override
        public ServerBuilder<? extends ServerBuilder<?>> configureServer(ServerBuilder<? extends ServerBuilder<?>> serverBuilder) {

            return serverBuilder
                    .intercept(new RequestMetadataInterceptor())
                    .intercept(new CompressionInterceptor())
                    .intercept(new DelayedExecutionInterceptor());
        }

        @Override
        public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {

            return clientStub
                    .withCompression(ClientCompressionInterceptor.COMPRESSION_TYPE)
                    .withInterceptors(new ClientCompressionInterceptor());
        }

        @Override
        public ManagedChannelBuilder<? extends ManagedChannelBuilder<?>>
        configureClientChannel(ManagedChannelBuilder<? extends ManagedChannelBuilder<?>> channelBuilder) {

            // Fallback default - use common concerns to set up transport security
            return channelBuilder.usePlaintext();
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

            return serverBuilder.intercept(new LoggingInterceptor(serviceClass));
        }

        @Override
        public <TStub extends AbstractStub<TStub>> TStub configureClient(TStub clientStub) {

            return clientStub.withInterceptors(new ClientLoggingInterceptor(serviceClass));
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
