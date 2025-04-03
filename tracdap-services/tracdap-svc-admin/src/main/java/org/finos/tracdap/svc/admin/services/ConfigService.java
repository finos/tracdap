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
import org.finos.tracdap.api.internal.ConfigUpdate;
import org.finos.tracdap.api.internal.ConfigUpdateType;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.middleware.GrpcConcern;

import io.grpc.Context;
import org.finos.tracdap.common.config.ISecretService;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.ResourceDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;


public class ConfigService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient;
    private final GrpcConcern commonConcerns;
    private final ISecretService secretService;
    private final NotifierService notifier;

    public ConfigService(
            InternalMetadataApiGrpc.InternalMetadataApiBlockingStub metadataClient,
            GrpcConcern commonConcerns, ISecretService secretService,
            NotifierService notifier) {

        this.metadataClient = metadataClient;
        this.commonConcerns = commonConcerns;
        this.secretService = secretService;
        this.notifier = notifier;
    }

    public ConfigWriteResponse createConfigObject(ConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        // Create an empty prior for comparison
        var prior = ConfigReadResponse.newBuilder().build();

        // Put secrets into the secret service and replace with aliases
        var secrets = secretScope(request);
        var secretsUpdated = new SimpleResult<Boolean>();
        var secureRequest = processSecrets(request, prior, secrets, secretsUpdated);

        // Create the metadata object
        var result = client.createConfigObject(secureRequest);

        // Send a notification
        var update = ConfigUpdate.newBuilder()
                .setTenant(request.getTenant())
                .setUpdateType(ConfigUpdateType.CREATE)
                .setConfigEntry(result.getEntry())
                .setSecretsUpdated(secretsUpdated.getResult())
                .build();

        notifier.configUpdate(update);

        return result;
    }

    public ConfigWriteResponse updateConfigObject(ConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        // Obtain the prior object for comparison
        var priorRequest = ConfigReadRequest.newBuilder()
                .setTenant(request.getTenant())
                .setEntry(request.getPriorEntry())
                .build();

        var prior = client.readConfigEntry(priorRequest);

        // Put modified secrets into the secret service and replace with aliases
        var secrets = secretScope(request);
        var secretsUpdated = new SimpleResult<Boolean>();
        var secureRequest = processSecrets(request, prior, secrets, secretsUpdated);

        // Update the metadata object
        var result = client.updateConfigObject(secureRequest);

        // Send a notification
        var update = ConfigUpdate.newBuilder()
                .setTenant(request.getTenant())
                .setUpdateType(ConfigUpdateType.UPDATE)
                .setConfigEntry(result.getEntry())
                .setSecretsUpdated(secretsUpdated.getResult())
                .build();

        notifier.configUpdate(update);

        return result;
    }

    public ConfigWriteResponse deleteConfigObject(ConfigWriteRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        // Obtain the prior object for comparison
        var priorRequest = ConfigReadRequest.newBuilder()
                .setTenant(request.getTenant())
                .setEntry(request.getPriorEntry())
                .build();

        var prior = client.readConfigEntry(priorRequest);

        // Remove any secrets for this object from the config service
        var secrets = secretScope(request);
        var secretsUpdated = new SimpleResult<Boolean>();
        var secureRequest = processSecrets(request, prior, secrets, secretsUpdated);

        // Delete the metadata object
        var result = client.deleteConfigObject(secureRequest);

        // Send a notification
        var update = ConfigUpdate.newBuilder()
                .setTenant(request.getTenant())
                .setUpdateType(ConfigUpdateType.DELETE)
                .setConfigEntry(result.getEntry())
                .setSecretsUpdated(secretsUpdated.getResult())
                .build();

        notifier.configUpdate(update);

        return result;
    }

    public ConfigReadResponse readConfigObject(ConfigReadRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var entry = client.readConfigEntry(request);
        var selector = entry.getEntry().getDetails().getObjectSelector();

        var objectRequest = MetadataReadRequest.newBuilder()
                .setTenant(request.getTenant())
                .setSelector(selector)
                .build();

        var object = client.readObject(objectRequest);

        // Apply config masking logic (for API usability, config objects do not hold sensitive data)
        var maskedObject = MetadataUtil.applyConfigMasking(object.getDefinition());

        return ConfigReadResponse.newBuilder()
                .setEntry(entry.getEntry())
                .setDefinition(maskedObject)
                .putAllAttrs(object.getAttrsMap())
                .build();
    }

    public ConfigReadBatchResponse readConfigBatch(ConfigReadBatchRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        var entries = client.readConfigBatch(request);
        var selectors = entries.getEntriesList().stream()
                .map(entry -> entry.getEntry().getDetails().getObjectSelector())
                .collect(Collectors.toList());

        var objectRequest = MetadataBatchRequest.newBuilder()
                .setTenant(request.getTenant())
                .addAllSelector(selectors)
                .build();

        var objects = client.readBatch(objectRequest);

        var batchResponse = ConfigReadBatchResponse.newBuilder();

        for (int i = 0; i < request.getEntriesCount(); i++) {

            // Apply config masking logic (for API usability, config objects do not hold sensitive data)
            var maskedObject = MetadataUtil.applyConfigMasking(objects.getTag(i).getDefinition());

            var entry = ConfigReadResponse.newBuilder()
                    .setEntry(entries.getEntries(i).getEntry())
                    .setDefinition(maskedObject)
                    .putAllAttrs(objects.getTag(i).getAttrsMap());

            batchResponse.addEntries(entry);
        }

        return batchResponse.build();
    }

    public ConfigListResponse listConfigEntries(ConfigListRequest request) {

        var client = commonConcerns
                .prepareClientCall(Context.current())
                .configureClient(metadataClient);

        return client.listConfigEntries(request);
    }

    private ConfigWriteRequest processSecrets(
            ConfigWriteRequest request, ConfigReadResponse prior,
            ISecretService secrets, SimpleResult<Boolean> secretsUpdated) {

        var objectType = request.hasDefinition()
                ? request.getDefinition().getObjectType()
                : request.getPriorEntry().getDetails().getObjectType();

        if (objectType == ObjectType.RESOURCE) {

            var secureResource = processSecrets(
                    request.getDefinition().getResource(),
                    prior.getDefinition().getResource(),
                    secrets, secretsUpdated);

            var secureObject = request.getDefinition().toBuilder().setResource(secureResource);

            return request.toBuilder().setDefinition(secureObject).build();
        }

        return request;
    }

    private ResourceDefinition processSecrets(
            ResourceDefinition newResource, ResourceDefinition oldResource,
            ISecretService secrets, SimpleResult<Boolean> secretsUpdated) {

        var secureResource = newResource.toBuilder();

        for (var secret : newResource.getSecretsMap().entrySet()) {

            var secretKey = secret.getKey();
            var secretValue = secret.getValue();

            if (secretValue != null && !secretValue.isEmpty()) {

                var secretAlias = secrets.storePassword(secretKey, secretValue);
                secureResource.putSecrets(secretKey, secretAlias);

                if (!secretsUpdated.isDone())
                    secretsUpdated.setResult(true);
            }
        }

        for (var secretKey : oldResource.getSecretsMap().keySet()) {
            if (!newResource.containsSecrets(secretKey)) {

                secrets.deleteSecret(secretKey);

                if (!secretsUpdated.isDone())
                    secretsUpdated.setResult(true);
            }
        }

        if (secretsUpdated.isDone())
            secrets.commit();
        else
            secretsUpdated.setResult(false);

        return secureResource.build();
    }

    private ISecretService secretScope(ConfigWriteRequest request) {

        return secretService
                .namedScope(ConfigKeys.TENANT_SCOPE, request.getTenant())
                .scope(request.getConfigClass())
                .scope(request.getConfigKey());
    }

    private static class SimpleResult<T> {

        private T result;
        private boolean done;

        public SimpleResult() {
            this.result = null;
            this.done = false;
        }

        public void setResult(T result) {
            if (done)
                throw new IllegalStateException("Result has already been set");
            this.result = result;
            this.done = true;
        }

        public T getResult() {
            if (!done)
                throw new IllegalStateException("Result has not been set");
            return result;
        }

        public boolean isDone() {
            return done;
        }
    }
}
