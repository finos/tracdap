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

package org.finos.tracdap.common.validation;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.grpc.GrpcServiceRegister;
import org.finos.tracdap.common.service.TracServiceConfig;

import io.grpc.ServerBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class ValidationConcern implements GrpcConcern {

    public static final String CONFIG_NAME = TracServiceConfig.TRAC_VALIDATION;

    private final List<Descriptors.ServiceDescriptor> serviceDescriptors;

    public ValidationConcern(Descriptors.FileDescriptor... fileDescriptors) {

        this(Arrays.asList(fileDescriptors));
    }

    public ValidationConcern(List<Descriptors.FileDescriptor> fileDescriptors) {

        this.serviceDescriptors = fileDescriptors.stream()
                .map(Descriptors.FileDescriptor::getServices)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Override
    public String concernName() {
        return CONFIG_NAME;
    }

    @Override
    public ServerBuilder<? extends ServerBuilder<?>> configureServer(ServerBuilder<? extends ServerBuilder<?>> serverBuilder) {

        var serviceRegister = GrpcServiceRegister.newBuilder()
                .registerServices(serviceDescriptors)
                .build();

        var validationInterceptor = new ValidationInterceptor(serviceRegister);

        return serverBuilder.intercept(validationInterceptor);
    }
}
