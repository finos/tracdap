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

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.api.internal.PlatformConfigUpdate;
import org.finos.tracdap.common.config.ISecretService;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.metadata.CredentialDefinition;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.PlatformConfigEntry;

import io.grpc.Context;


public class PlatformConfigService {

    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient;
    private final GrpcConcern commonConcerns;
    private final ISecretService secretService;
    private final NotifierService notifier;

    public PlatformConfigService(
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient,
            GrpcConcern commonConcerns, ISecretService secretService,
            NotifierService notifier) {

        this.metadataClient = metadataClient;
        this.commonConcerns = commonConcerns;
        this.secretService = secretService;
        this.notifier = notifier;
    }

    public PlatformConfigWriteResponse createPlatformConfigObject(PlatformConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        // Create an empty prior for comparison
        var prior = PlatformConfigReadResponse.newBuilder().build();

        // Put secrets into the secret service and replace with aliases
        var secrets = secretScope(request);
        var secretsUpdated = new SimpleResult<Boolean>();
        var secureRequest = processSecrets(request, prior, secrets, secretsUpdated);

        var result = client.createPlatformConfigObject(secureRequest);

        notifyUpdate(ConfigUpdateType.CREATE, result.getEntry(), secretsUpdated.getResult());

        return result;
    }

    public PlatformConfigWriteResponse updatePlatformConfigObject(PlatformConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        // Obtain the prior object for comparison
        var priorRequest = PlatformConfigReadRequest.newBuilder()
                .setEntry(request.getPriorEntry())
                .build();

        var prior = client.readPlatformConfigEntry(priorRequest);

        // Put modified secrets into the secret service and replace with aliases
        var secrets = secretScope(request);
        var secretsUpdated = new SimpleResult<Boolean>();
        var secureRequest = processSecrets(request, prior, secrets, secretsUpdated);

        var result = client.updatePlatformConfigObject(secureRequest);

        notifyUpdate(ConfigUpdateType.UPDATE, result.getEntry(), secretsUpdated.getResult());

        return result;
    }

    public PlatformConfigWriteResponse deletePlatformConfigObject(PlatformConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        // Obtain the prior object for comparison
        var priorRequest = PlatformConfigReadRequest.newBuilder()
                .setEntry(request.getPriorEntry())
                .build();

        var prior = client.readPlatformConfigEntry(priorRequest);

        // Remove any secrets for this object from the secret store
        var secrets = secretScope(request);
        var secretsUpdated = new SimpleResult<Boolean>();
        var secureRequest = processSecrets(request, prior, secrets, secretsUpdated);

        var result = client.deletePlatformConfigObject(secureRequest);

        notifyUpdate(ConfigUpdateType.DELETE, result.getEntry(), secretsUpdated.getResult());

        return result;
    }

    public PlatformConfigReadResponse readPlatformConfigObject(PlatformConfigReadRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var response = client.readPlatformConfigEntry(request);

        // Apply config masking logic (for API usability, config objects do not hold sensitive data)
        var maskedDefinition = MetadataUtil.applyConfigMasking(response.getDefinition());

        return response.toBuilder()
                .setDefinition(maskedDefinition)
                .build();
    }

    public PlatformConfigReadBatchResponse readPlatformConfigBatch(PlatformConfigReadBatchRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var response = client.readPlatformConfigBatch(request);

        var batchResponse = PlatformConfigReadBatchResponse.newBuilder();

        for (var entry : response.getEntriesList()) {

            // Apply config masking logic (for API usability, config objects do not hold sensitive data)
            var maskedDefinition = MetadataUtil.applyConfigMasking(entry.getDefinition());

            batchResponse.addEntries(entry.toBuilder().setDefinition(maskedDefinition));
        }

        return batchResponse.build();
    }

    public PlatformConfigListResponse listPlatformConfigEntries(PlatformConfigListRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        return client.listPlatformConfigEntries(request);
    }

    private PlatformConfigWriteRequest processSecrets(
            PlatformConfigWriteRequest request, PlatformConfigReadResponse prior,
            ISecretService secrets, SimpleResult<Boolean> secretsUpdated) {

        var objectType = request.hasDefinition()
                ? request.getDefinition().getObjectType()
                : prior.getDefinition().getObjectType();

        if (objectType != ObjectType.CREDENTIAL) {

            // Other object types are not processed for secrets
            secretsUpdated.setResult(false);

            return request;
        }

        var newCredential = request.hasDefinition()
                ? request.getDefinition().getCredential()
                : CredentialDefinition.getDefaultInstance();

        var secureCredential = processCredentialSecrets(
                newCredential, prior.getDefinition().getCredential(),
                secrets, secretsUpdated);

        // Delete requests carry no definition, and it must stay omitted for validation to pass
        // The secret store cleanup above still applies as a side effect
        if (!request.hasDefinition())
            return request;

        var secureObject = request.getDefinition().toBuilder().setCredential(secureCredential);

        return request.toBuilder().setDefinition(secureObject).build();
    }

    private CredentialDefinition processCredentialSecrets(
            CredentialDefinition newCredential, CredentialDefinition oldCredential,
            ISecretService secrets, SimpleResult<Boolean> secretsUpdated) {

        var secureSecrets = SecretMapProcessor.processSecrets(
                newCredential.getSecretsMap(), oldCredential.getSecretsMap(),
                secrets, secretsUpdated);

        return newCredential.toBuilder()
                .putAllSecrets(secureSecrets)
                .build();
    }

    private ISecretService secretScope(PlatformConfigWriteRequest request) {

        return secretService
                .scope(request.getConfigClass())
                .scope(request.getConfigKey());
    }

    private void notifyUpdate(ConfigUpdateType updateType, PlatformConfigEntry entry, boolean secretsUpdated) {

        var update = PlatformConfigUpdate.newBuilder()
                .setUpdateType(updateType)
                .setConfigEntry(entry)
                .setSecretsUpdated(secretsUpdated)
                .build();

        notifier.platformConfigUpdate(update);
    }
}
