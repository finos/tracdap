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

package org.finos.tracdap.common.validation.api;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.core.ValidatorUtils;
import org.finos.tracdap.common.validation.static_.CommonValidators;


@Validator(type = ValidationType.STATIC, serviceFile = StorageServiceProto.class, serviceName = TracStorageApiGrpc.SERVICE_NAME)
public class StorageApiValidator {

    private static final Descriptors.Descriptor STORAGE_REQUEST;
    private static final Descriptors.FieldDescriptor SR_TENANT;
    private static final Descriptors.FieldDescriptor SR_STORAGE_KEY;
    private static final Descriptors.FieldDescriptor SR_STORAGE_PATH;

    private static final Descriptors.Descriptor STORAGE_READ_REQUEST;
    private static final Descriptors.FieldDescriptor SRR_TENANT;
    private static final Descriptors.FieldDescriptor SRR_STORAGE_KEY;
    private static final Descriptors.FieldDescriptor SRR_STORAGE_PATH;

    private static final Descriptors.Descriptor STORAGE_WRITE_REQUEST;
    private static final Descriptors.FieldDescriptor SWR_TENANT;
    private static final Descriptors.FieldDescriptor SWR_STORAGE_KEY;
    private static final Descriptors.FieldDescriptor SWR_STORAGE_PATH;

    static {

        STORAGE_REQUEST = StorageRequest.getDescriptor();
        SR_TENANT = ValidatorUtils.field(STORAGE_REQUEST, StorageRequest.TENANT_FIELD_NUMBER);
        SR_STORAGE_KEY = ValidatorUtils.field(STORAGE_REQUEST, StorageRequest.STORAGEKEY_FIELD_NUMBER);
        SR_STORAGE_PATH = ValidatorUtils.field(STORAGE_REQUEST, StorageRequest.STORAGEPATH_FIELD_NUMBER);

        STORAGE_READ_REQUEST = StorageReadRequest.getDescriptor();
        SRR_TENANT = ValidatorUtils.field(STORAGE_READ_REQUEST, StorageReadRequest.TENANT_FIELD_NUMBER);
        SRR_STORAGE_KEY = ValidatorUtils.field(STORAGE_READ_REQUEST, StorageReadRequest.STORAGEKEY_FIELD_NUMBER);
        SRR_STORAGE_PATH = ValidatorUtils.field(STORAGE_READ_REQUEST, StorageReadRequest.STORAGEPATH_FIELD_NUMBER);

        STORAGE_WRITE_REQUEST = StorageWriteRequest.getDescriptor();
        SWR_TENANT = ValidatorUtils.field(STORAGE_WRITE_REQUEST, StorageWriteRequest.TENANT_FIELD_NUMBER);
        SWR_STORAGE_KEY = ValidatorUtils.field(STORAGE_WRITE_REQUEST, StorageWriteRequest.STORAGEKEY_FIELD_NUMBER);
        SWR_STORAGE_PATH = ValidatorUtils.field(STORAGE_WRITE_REQUEST, StorageWriteRequest.STORAGEPATH_FIELD_NUMBER);
    }

    @Validator(method = "exists")
    public static ValidationContext exists(StorageRequest msg, ValidationContext ctx) {

        return storageRequest(msg, ctx);
    }

    @Validator(method = "size")
    public static ValidationContext size(StorageRequest msg, ValidationContext ctx) {

        return storageRequest(msg, ctx);
    }

    @Validator(method = "stat")
    public static ValidationContext stat(StorageRequest msg, ValidationContext ctx) {

        return storageRequest(msg, ctx);
    }

    @Validator(method = "ls")
    public static ValidationContext ls(StorageRequest msg, ValidationContext ctx) {

        return storageRequest(msg, ctx);
    }

    @Validator(method = "readFile")
    public static ValidationContext readFile(StorageReadRequest msg, ValidationContext ctx) {

        return storageReadRequest(msg, ctx);
    }

    @Validator(method = "readSmallFile")
    public static ValidationContext readSmallFile(StorageReadRequest msg, ValidationContext ctx) {

        return storageReadRequest(msg, ctx);
    }

    private static ValidationContext storageRequest(StorageRequest msg, ValidationContext ctx) {

        ctx = ctx.push(SR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(SR_STORAGE_KEY)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(SR_STORAGE_PATH)
                .apply(CommonValidators::required)
                .apply(CommonValidators::relativePath)
                .pop();

        return ctx;
    }

    private static ValidationContext storageReadRequest(StorageReadRequest msg, ValidationContext ctx) {

        ctx = ctx.push(SRR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(SRR_STORAGE_KEY)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(SRR_STORAGE_PATH)
                .apply(CommonValidators::required)
                .apply(CommonValidators::relativePath)
                .pop();

        return ctx;
    }
}
