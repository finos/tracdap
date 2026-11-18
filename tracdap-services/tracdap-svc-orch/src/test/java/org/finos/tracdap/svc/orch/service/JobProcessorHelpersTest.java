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

package org.finos.tracdap.svc.orch.service;

import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.api.internal.RuntimeJobStatus;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.ResourceBundle;
import org.finos.tracdap.common.middleware.GrpcConcern;
import org.finos.tracdap.common.plugin.PluginRegistry;
import org.finos.tracdap.common.service.TenantConfigManager;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.meta.SampleMetadata;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JobProcessorHelpersTest {

    private JobProcessorHelpers helpers;

    private TagHeader jobId;
    private TagHeader resultId;
    private TagHeader logFileId;
    private TagHeader logStorageId;

    private static final TagHeader MODEL_ID = SampleMetadata.newHeader(ObjectType.MODEL);
    private static final TagHeader CONTEXT_ID = SampleMetadata.newHeader(ObjectType.CUSTOM);
    private static final TagSelector MODEL_SELECTOR = MetadataUtil.selectorFor(MODEL_ID);
    private static final TagSelector CONTEXT_SELECTOR = MetadataUtil.selectorFor(CONTEXT_ID);

    @BeforeEach
    void setup() {

        var tenantState = Mockito.mock(TenantConfigManager.class);
        Mockito.when(tenantState.getTenantConfig(Mockito.anyString())).thenReturn(TenantConfig.getDefaultInstance());

        var registry = new PluginRegistry();
        registry.addSingleton(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class,
                Mockito.mock(InternalMetadataApiGrpc.InternalMetadataApiBlockingStub.class));
        registry.addSingleton(ConfigManager.class, Mockito.mock(ConfigManager.class));

        var commonConcerns = Mockito.mock(GrpcConcern.class, Answers.CALLS_REAL_METHODS);

        helpers = new JobProcessorHelpers(PluginConfig.getDefaultInstance(), tenantState, commonConcerns, registry);

        jobId = SampleMetadata.newHeader(ObjectType.JOB);
        resultId = SampleMetadata.newHeader(ObjectType.RESULT);
        logFileId = SampleMetadata.newHeader(ObjectType.FILE);
        logStorageId = SampleMetadata.newHeader(ObjectType.STORAGE);
    }

    @Test
    void processJobResult_missingResult() {

        var jobState = jobState(JobStatusCode.FAILED, "Executor message", null);

        helpers.processJobResult(jobState);

        Assertions.assertEquals(JobStatusCode.FAILED, jobState.tracStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, jobState.jobResult.getResult().getStatusCode());
        Assertions.assertEquals("Executor message", jobState.jobResult.getResult().getStatusMessage());
        Assertions.assertFalse(jobState.jobResult.getResult().hasLogFileId());
        Assertions.assertEquals(0, jobState.jobResult.getObjectIdsCount());
    }

    @Test
    void processJobResult_missingResultNoMessage() {

        var jobState = jobState(JobStatusCode.FAILED, null, null);

        helpers.processJobResult(jobState);

        Assertions.assertEquals(JobStatusCode.FAILED, jobState.jobResult.getResult().getStatusCode());
        Assertions.assertEquals("No details available", jobState.jobResult.getResult().getStatusMessage());
    }

    @Test
    void processJobResult_failedResult() {

        var runtimeResult = runtimeResult(JobStatusCode.FAILED, "Model failed\nsecond line", true);
        var jobState = jobState(JobStatusCode.FAILED, "Local batch terminated with non-zero exit code [1]", runtimeResult);
        jobState.runtimeStatus = executorStatus(JobStatusCode.FAILED);

        helpers.processJobResult(jobState);

        var result = jobState.jobResult.getResult();

        Assertions.assertEquals(JobStatusCode.FAILED, jobState.tracStatus);
        Assertions.assertEquals("Model failed\nsecond line", jobState.statusMessage);
        Assertions.assertEquals(JobStatusCode.FAILED, result.getStatusCode());
        Assertions.assertEquals("Model failed\nsecond line", result.getStatusMessage());
        Assertions.assertEquals(resultId, jobState.jobResult.getResultId());

        assertLogRegistered(jobState);
    }

    @Test
    void processJobResult_failedResultContextOnLog() {

        var contextId = SampleMetadata.newHeader(ObjectType.CUSTOM);

        var runtimeResult = runtimeResult(JobStatusCode.FAILED, "Model failed", true);
        var jobState = jobState(JobStatusCode.FAILED, "Executor message", runtimeResult);
        jobState.runtimeStatus = executorStatus(JobStatusCode.FAILED);
        jobState.definition = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setContext(MetadataUtil.selectorFor(contextId))
                .build();

        helpers.processJobResult(jobState);

        assertLogRegistered(jobState);

        for (var objectId : List.of(logFileId, logStorageId)) {

            var attrs = jobState.jobResult.getAttrsOrThrow(MetadataUtil.objectKey(objectId)).getAttrsList();

            Assertions.assertTrue(attrs.stream().anyMatch(attr ->
                    attr.getAttrName().equals(MetadataConstants.TRAC_JOB_CONTEXT_ATTR) &&
                    MetadataCodec.decodeStringValue(attr.getValue()).equals(contextId.getObjectId())));

            Assertions.assertTrue(attrs.stream().anyMatch(attr ->
                    attr.getAttrName().equals(MetadataConstants.TRAC_JOB_CONTEXT_VERSION_ATTR) &&
                    MetadataCodec.decodeIntegerValue(attr.getValue()) == contextId.getObjectVersion()));
        }
    }

    @Test
    void processJobResult_failedResultNoMessage() {

        var runtimeResult = runtimeResult(JobStatusCode.FAILED, "", true);
        var jobState = jobState(JobStatusCode.FAILED, "Executor message", runtimeResult);
        jobState.runtimeStatus = executorStatus(JobStatusCode.FAILED);

        helpers.processJobResult(jobState);

        Assertions.assertEquals(JobStatusCode.FAILED, jobState.jobResult.getResult().getStatusCode());
        Assertions.assertEquals("Executor message", jobState.jobResult.getResult().getStatusMessage());

        assertLogRegistered(jobState);
    }

    @Test
    void processJobResult_failedResultWithoutLog() {

        var runtimeResult = runtimeResult(JobStatusCode.FAILED, "Model failed", false);
        var jobState = jobState(JobStatusCode.FAILED, "Executor message", runtimeResult);
        jobState.runtimeStatus = executorStatus(JobStatusCode.FAILED);

        helpers.processJobResult(jobState);

        Assertions.assertEquals(JobStatusCode.FAILED, jobState.jobResult.getResult().getStatusCode());
        Assertions.assertEquals("Model failed", jobState.jobResult.getResult().getStatusMessage());
        Assertions.assertFalse(jobState.jobResult.getResult().hasLogFileId());
        Assertions.assertEquals(0, jobState.jobResult.getObjectIdsCount());
    }

    @Test
    void processJobResult_succeededResultExecutorFailed() {

        var outputId = SampleMetadata.newHeader(ObjectType.FILE);
        var placedDataId = SampleMetadata.newHeader(ObjectType.DATA);

        var placement = PlacementRecord.newBuilder()
                .setDataId(MetadataUtil.selectorFor(placedDataId))
                .setLocation(ExternalLocation.newBuilder()
                .setStorageKey("EXTERNAL_STORAGE")
                .setStoragePath("exports/loans.csv"))
                .setFormat("CSV")
                .setMimeType("text/csv")
                .setSize(1024)
                .setContentHash("sha256:0000");

        var runtimeResult = runtimeResult(JobStatusCode.SUCCEEDED, "", true).toBuilder()
                .addObjectIds(outputId)
                .putObjects(MetadataUtil.objectKey(outputId), fileDef(logStorageId))
                .build();

        runtimeResult = runtimeResult.toBuilder()
                .setResult(runtimeResult.getResult().toBuilder()
                .putOutputs("report", MetadataUtil.selectorFor(outputId))
                .putPlacements("loans_export", placement.build()))
                .build();

        var jobState = jobState(JobStatusCode.FAILED, "Local batch terminated with non-zero exit code [1]", runtimeResult);
        jobState.runtimeStatus = executorStatus(JobStatusCode.FAILED);

        helpers.processJobResult(jobState);

        var result = jobState.jobResult.getResult();

        Assertions.assertEquals(JobStatusCode.FAILED, jobState.tracStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, result.getStatusCode());
        Assertions.assertEquals("Local batch terminated with non-zero exit code [1]", result.getStatusMessage());
        Assertions.assertFalse(jobState.jobResult.containsObjects(MetadataUtil.objectKey(outputId)));
        Assertions.assertEquals(0, result.getOutputsCount());
        Assertions.assertEquals(0, result.getPlacementsCount());

        assertLogRegistered(jobState);
    }

    @Test
    void processJobResult_cancelledResult() {

        var runtimeResult = runtimeResult(JobStatusCode.CANCELLED, "Job cancelled", true);
        var jobState = jobState(JobStatusCode.FAILED, "Executor message", runtimeResult);
        jobState.runtimeStatus = executorStatus(JobStatusCode.FAILED);

        helpers.processJobResult(jobState);

        Assertions.assertEquals(JobStatusCode.CANCELLED, jobState.tracStatus);
        Assertions.assertEquals(JobStatusCode.CANCELLED, jobState.jobResult.getResult().getStatusCode());
        Assertions.assertEquals("Job cancelled", jobState.jobResult.getResult().getStatusMessage());

        assertLogRegistered(jobState);
    }

    @Test
    void processJobResult_failedResultInvalid() {

        // A RESULT ID with the wrong object type fails validation
        var runtimeResult = runtimeResult(JobStatusCode.FAILED, "Model failed", true).toBuilder()
                .setResultId(SampleMetadata.newHeader(ObjectType.DATA))
                .build();

        var jobState = jobState(JobStatusCode.FAILED, "Executor message", runtimeResult);
        jobState.runtimeStatus = executorStatus(JobStatusCode.FAILED);

        helpers.processJobResult(jobState);

        Assertions.assertEquals(JobStatusCode.FAILED, jobState.tracStatus);
        Assertions.assertEquals(resultId, jobState.jobResult.getResultId());
        Assertions.assertEquals(JobStatusCode.FAILED, jobState.jobResult.getResult().getStatusCode());
        Assertions.assertEquals("Model failed", jobState.jobResult.getResult().getStatusMessage());
        Assertions.assertFalse(jobState.jobResult.getResult().hasLogFileId());
        Assertions.assertEquals(0, jobState.jobResult.getObjectIdsCount());
    }

    @Test
    void processJobResult_failedResultMissingLogDefinition() {

        var runtimeResult = runtimeResult(JobStatusCode.FAILED, "Model failed", true).toBuilder()
                .removeObjects(MetadataUtil.objectKey(logStorageId))
                .build();

        var jobState = jobState(JobStatusCode.FAILED, "Executor message", runtimeResult);
        jobState.runtimeStatus = executorStatus(JobStatusCode.FAILED);

        helpers.processJobResult(jobState);

        Assertions.assertEquals(JobStatusCode.FAILED, jobState.tracStatus);
        Assertions.assertEquals(JobStatusCode.FAILED, jobState.jobResult.getResult().getStatusCode());
        Assertions.assertEquals("Model failed", jobState.jobResult.getResult().getStatusMessage());
        Assertions.assertFalse(jobState.jobResult.getResult().hasLogFileId());
        Assertions.assertEquals(0, jobState.jobResult.getObjectIdsCount());
    }

    @Test
    void executorFailed() {

        var jobState = jobState(JobStatusCode.FAILED, null, null);

        jobState.runtimeStatus = null;
        Assertions.assertFalse(JobProcessorHelpers.executorFailed(jobState));

        jobState.runtimeStatus = executorStatus(JobStatusCode.SUCCEEDED);
        Assertions.assertFalse(JobProcessorHelpers.executorFailed(jobState));

        jobState.runtimeStatus = executorStatus(JobStatusCode.FINISHING);
        Assertions.assertFalse(JobProcessorHelpers.executorFailed(jobState));

        jobState.runtimeStatus = executorStatus(JobStatusCode.FAILED);
        Assertions.assertTrue(JobProcessorHelpers.executorFailed(jobState));

        jobState.runtimeStatus = executorStatus(JobStatusCode.JOB_STATUS_CODE_NOT_SET);
        Assertions.assertTrue(JobProcessorHelpers.executorFailed(jobState));
    }

    @Test
    void truncateErrorMessage_short() {

        var message = "Model failed\nsecond line";

        Assertions.assertEquals(message, JobProcessorHelpers.truncateErrorMessage(message));
    }

    @Test
    void truncateErrorMessage_exactLimit() {

        var message = "x".repeat(JobProcessorHelpers.ERROR_MESSAGE_MAX_BYTES);

        Assertions.assertEquals(message, JobProcessorHelpers.truncateErrorMessage(message));
    }

    @Test
    void truncateErrorMessage_long() {

        var message = "x".repeat(JobProcessorHelpers.ERROR_MESSAGE_MAX_BYTES + 1);
        var truncated = JobProcessorHelpers.truncateErrorMessage(message);

        Assertions.assertEquals(JobProcessorHelpers.ERROR_MESSAGE_MAX_BYTES, utf8Length(truncated));
        Assertions.assertTrue(truncated.endsWith(JobProcessorHelpers.ERROR_MESSAGE_TRUNCATED));
        Assertions.assertTrue(message.startsWith(truncated.substring(0, truncated.length() - JobProcessorHelpers.ERROR_MESSAGE_TRUNCATED.length())));
    }

    @Test
    void truncateErrorMessage_multiByteCharacters() {

        // 3-byte and 4-byte (surrogate pair) characters must not be split
        var message = "你好𝜌".repeat(1000);
        var truncated = JobProcessorHelpers.truncateErrorMessage(message);
        var kept = truncated.substring(0, truncated.length() - JobProcessorHelpers.ERROR_MESSAGE_TRUNCATED.length());

        Assertions.assertTrue(utf8Length(truncated) <= JobProcessorHelpers.ERROR_MESSAGE_MAX_BYTES);
        Assertions.assertTrue(message.startsWith(kept));
        Assertions.assertFalse(Character.isHighSurrogate(kept.charAt(kept.length() - 1)));
    }

    @Test
    void buildJobConfig_contextNotSentToRuntime() {

        var jobState = configJobState(CONTEXT_SELECTOR);
        var newState = helpers.buildJobConfig(jobState);

        var jobConfig = newState.jobConfig;
        var contextKey = MetadataUtil.objectKey(CONTEXT_ID);
        var modelKey = MetadataUtil.objectKey(MODEL_ID);

        Assertions.assertFalse(jobConfig.getJob().hasContext());
        Assertions.assertFalse(jobConfig.containsObjectMapping(MetadataUtil.objectKey(CONTEXT_SELECTOR)));
        Assertions.assertFalse(jobConfig.containsObjects(contextKey));
        Assertions.assertFalse(jobConfig.containsTags(contextKey));

        Assertions.assertEquals(MODEL_ID, jobConfig.getObjectMappingOrThrow(MetadataUtil.objectKey(MODEL_SELECTOR)));
        Assertions.assertTrue(jobConfig.containsObjects(modelKey));
        Assertions.assertTrue(jobConfig.containsTags(modelKey));
        Assertions.assertEquals(jobState.definition.getRunModel(), jobConfig.getJob().getRunModel());

        // The job state keeps the context, it is needed to stamp the job outputs
        Assertions.assertEquals(CONTEXT_SELECTOR, newState.definition.getContext());
        Assertions.assertTrue(newState.metadata.hasObject(CONTEXT_SELECTOR));
    }

    @Test
    void buildJobConfig_noContextUnchanged() {

        var jobState = configJobState(null);
        var newState = helpers.buildJobConfig(jobState);

        Assertions.assertEquals(jobState.definition, newState.jobConfig.getJob());
        Assertions.assertEquals(jobState.metadata.getObjects(), newState.jobConfig.getObjectsMap());
        Assertions.assertEquals(jobState.metadata.getObjectMapping(), newState.jobConfig.getObjectMappingMap());
    }

    @Test
    void jobState_contextKeptThroughCache() throws Exception {

        var jobState = helpers.buildJobConfig(configJobState(CONTEXT_SELECTOR));

        var buffer = new ByteArrayOutputStream();

        try (var out = new ObjectOutputStream(buffer)) {
            out.writeObject(jobState);
        }

        JobState restored;

        try (var in = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
            restored = (JobState) in.readObject();
        }

        // The bundle is rebuilt from the job config, without the context, the definition keeps it
        Assertions.assertEquals(CONTEXT_SELECTOR, restored.definition.getContext());
        Assertions.assertFalse(restored.metadata.hasObject(CONTEXT_SELECTOR));
    }

    private void assertLogRegistered(JobState jobState) {

        var jobResult = jobState.jobResult;
        var logFileKey = MetadataUtil.objectKey(logFileId);
        var logStorageKey = MetadataUtil.objectKey(logStorageId);

        Assertions.assertEquals(MetadataUtil.selectorFor(logFileId), jobResult.getResult().getLogFileId());
        Assertions.assertEquals(2, jobResult.getObjectIdsCount());
        Assertions.assertTrue(jobResult.getObjectIdsList().contains(logFileId));
        Assertions.assertTrue(jobResult.getObjectIdsList().contains(logStorageId));
        Assertions.assertTrue(jobResult.containsObjects(logFileKey));
        Assertions.assertTrue(jobResult.containsObjects(logStorageKey));

        for (var objectKey : List.of(logFileKey, logStorageKey)) {

            var attrs = jobResult.getAttrsOrThrow(objectKey).getAttrsList();

            Assertions.assertTrue(attrs.stream().anyMatch(attr ->
                    attr.getAttrName().equals(MetadataConstants.TRAC_CREATE_JOB) &&
                    MetadataCodec.decodeStringValue(attr.getValue()).equals(jobState.jobKey)));
        }
    }

    private JobState jobState(JobStatusCode tracStatus, String statusMessage, JobResult runtimeResult) {

        var jobState = new JobState();
        jobState.jobId = jobId;
        jobState.jobKey = MetadataUtil.objectKey(jobId);
        jobState.jobType = JobType.RUN_MODEL;
        jobState.resultId = resultId;
        jobState.tracStatus = tracStatus;
        jobState.statusMessage = statusMessage;
        jobState.runtimeResult = runtimeResult;

        return jobState;
    }

    private JobResult runtimeResult(JobStatusCode statusCode, String statusMessage, boolean withLog) {

        var result = ResultDefinition.newBuilder()
                .setJobId(MetadataUtil.selectorFor(jobId))
                .setStatusCode(statusCode)
                .setStatusMessage(statusMessage);

        var jobResult = JobResult.newBuilder()
                .setJobId(jobId)
                .setResultId(resultId);

        if (withLog) {

            result.setLogFileId(MetadataUtil.selectorFor(logFileId));

            jobResult.addObjectIds(logFileId);
            jobResult.addObjectIds(logStorageId);
            jobResult.putObjects(MetadataUtil.objectKey(logFileId), fileDef(logStorageId));
            jobResult.putObjects(MetadataUtil.objectKey(logStorageId), SampleMetadata.dummyStorageDef());
        }

        return jobResult.setResult(result).build();
    }

    private static ObjectDefinition fileDef(TagHeader storageId) {

        var fileDef = SampleMetadata.dummyFileDef();

        return fileDef.toBuilder()
                .setFile(fileDef.getFile().toBuilder()
                .setStorageId(MetadataUtil.selectorForLatest(storageId)))
                .build();
    }

    private static RuntimeJobStatus executorStatus(JobStatusCode statusCode) {

        return RuntimeJobStatus.newBuilder()
                .setStatusCode(statusCode)
                .build();
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private JobState configJobState(TagSelector context) {

        var definition = JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(RunModelJob.newBuilder().setModel(MODEL_SELECTOR));

        if (context != null)
            definition.setContext(context);

        var objects = new HashMap<String, ObjectDefinition>();
        var tags = new HashMap<String, Tag>();
        var mapping = new HashMap<String, TagHeader>();

        addObject(MODEL_SELECTOR, MODEL_ID, ObjectType.MODEL, mapping, objects, tags);

        if (context != null)
            addObject(context, CONTEXT_ID, ObjectType.CUSTOM, mapping, objects, tags);

        var jobId = SampleMetadata.newHeader(ObjectType.JOB);

        var jobState = new JobState();
        jobState.tenant = SampleMetadata.TEST_TENANT;
        jobState.jobId = jobId;
        jobState.jobKey = MetadataUtil.objectKey(jobId);
        jobState.jobType = JobType.RUN_MODEL;
        jobState.definition = definition.build();
        jobState.resultId = SampleMetadata.newHeader(ObjectType.RESULT);
        jobState.metadata = new MetadataBundle(mapping, objects, tags);
        jobState.resources = ResourceBundle.empty();

        return jobState;
    }

    private static void addObject(
            TagSelector selector, TagHeader header, ObjectType objectType,
            Map<String, TagHeader> mapping, Map<String, ObjectDefinition> objects, Map<String, Tag> tags) {

        var objectKey = MetadataUtil.objectKey(header);

        mapping.put(MetadataUtil.objectKey(selector), header);
        objects.put(objectKey, ObjectDefinition.newBuilder().setObjectType(objectType).build());
        tags.put(objectKey, Tag.newBuilder().setHeader(header).build());
    }
}
