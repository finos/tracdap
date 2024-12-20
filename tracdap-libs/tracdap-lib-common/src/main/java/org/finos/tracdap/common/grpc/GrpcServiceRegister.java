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

package org.finos.tracdap.common.grpc;

import org.finos.tracdap.common.exception.ETracInternal;

import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GrpcServiceRegister {

    protected final Map<String, Descriptors.ServiceDescriptor> serviceMap;
    protected final Map<String, Descriptors.MethodDescriptor> methodMap;

    private GrpcServiceRegister(
            Map<String, Descriptors.ServiceDescriptor> serviceMap,
            Map<String, Descriptors.MethodDescriptor> methodMap) {

        this.serviceMap = serviceMap;
        this.methodMap = methodMap;
    }

    public static Builder newBuilder() {

        return new Builder();
    }

    public static class Builder extends GrpcServiceRegister {

        private Builder() {
            super(new HashMap<>(), new HashMap<>());
        }

        public Builder registerServices(List<Descriptors.ServiceDescriptor> serviceDescriptors) {

            return serviceDescriptors.stream().reduce(this, Builder::registerService, (x, y) -> x);
        }

        public Builder registerService(Descriptors.ServiceDescriptor serviceDescriptor) {

            serviceMap.put(serviceDescriptor.getFullName(), serviceDescriptor);

            for (var methodDescriptor : serviceDescriptor.getMethods()) {

                var qualifiedMethodName = String.format("%s/%s",
                        serviceDescriptor.getFullName(),
                        methodDescriptor.getName());

                methodMap.put(qualifiedMethodName, methodDescriptor);
            }

            return this;
        }

        public GrpcServiceRegister build() {

            return new GrpcServiceRegister(serviceMap, methodMap);
        }
    }

    public Descriptors.ServiceDescriptor getServiceDescriptor(String serviceName) {

        var descriptor = serviceMap.get(serviceName);

        if (descriptor == null)
            throw new ETracInternal("Service " + serviceName + " not found");

        return descriptor;
    }

    public Descriptors.MethodDescriptor getMethodDescriptor(String methodName) {

        var descriptor = methodMap.get(methodName);

        if (descriptor == null)
            throw new ETracInternal("Method " + methodName + " not found");

        return descriptor;
    }
}
