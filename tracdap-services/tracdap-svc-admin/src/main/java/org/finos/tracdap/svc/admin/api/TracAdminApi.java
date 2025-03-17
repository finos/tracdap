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

package org.finos.tracdap.svc.admin.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.metadata.ConfigEntry;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.svc.admin.services.ConfigService;

import io.grpc.stub.StreamObserver;


public class TracAdminApi extends TracAdminApiGrpc.TracAdminApiImplBase {

    private final ConfigService configService;

    public TracAdminApi(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public void createConfigObject(ConfigWriteRequest request, StreamObserver<ConfigEntry> response) {

        try {
            var result = configService.createConfigObject(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception e) {
            response.onError(e);
        }
    }

    @Override
    public void updateConfigObject(ConfigWriteRequest request, StreamObserver<ConfigEntry> response) {

        try {
            var result = configService.updateConfigObject(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception e) {
            response.onError(e);
        }
    }

    @Override
    public void deleteConfigObject(ConfigWriteRequest request, StreamObserver<ConfigEntry> response) {

        try {
            var result = configService.deleteConfigObject(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception e) {
            response.onError(e);
        }
    }

    @Override
    public void readConfigObject(ConfigReadRequest request, StreamObserver<Tag> response) {

        try {
            var result = configService.readConfigObject(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception e) {
            response.onError(e);
        }
    }

    @Override
    public void readConfigBatch(ConfigReadBatchRequest request, StreamObserver<ConfigReadBatchResponse> response) {

        try {
            var result = configService.readConfigBatch(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception e) {
            response.onError(e);
        }
    }

    @Override
    public void listConfigKeys(ConfigListRequest request, StreamObserver<ConfigListResponse> response) {

        try {
            var result = configService.listConfigKeys(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception e) {
            response.onError(e);
        }
    }
}
