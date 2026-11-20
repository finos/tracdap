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

package org.finos.tracdap.svc.orch.jobs;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.service.ObjectWrite;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.RunModelJob;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.svc.orch.test.TestJobHooks;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JobContextTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";

    private static final Set<JobStatusCode> COMPLETED_STATES = Set.of(
            JobStatusCode.SUCCEEDED, JobStatusCode.FAILED, JobStatusCode.CANCELLED);

    private static final Set<String> CONTEXT_ATTRS = Set.of(
            MetadataConstants.TRAC_JOB_CONTEXT_ATTR,
            MetadataConstants.TRAC_JOB_CONTEXT_VERSION_ATTR);

    private static final SchemaDefinition ROUND_TRIP_SCHEMA = SchemaDefinition.newBuilder()
            .setSchemaType(SchemaType.TABLE)
            .setTable(TableSchema.newBuilder()
                    .addFields(field("boolean_field", BasicType.BOOLEAN, 0))
                    .addFields(field("integer_field", BasicType.INTEGER, 1))
                    .addFields(field("float_field", BasicType.FLOAT, 2))
                    .addFields(field("decimal_field", BasicType.DECIMAL, 3))
                    .addFields(field("string_field", BasicType.STRING, 4))
                    .addFields(field("categorical_field", BasicType.STRING, 5).setCategorical(true))
                    .addFields(field("date_field", BasicType.DATE, 6))
                    .addFields(field("datetime_field", BasicType.DATETIME, 7)))
            .build();

    private static final String ROUND_TRIP_CSV =
            "boolean_field,integer_field,float_field,decimal_field,string_field,categorical_field,date_field,datetime_field\n" +
            "true,1,1.5,1.5,a,A,2026-01-01,2026-01-01T00:00:00\n";

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(E2E_CONFIG, List.of(E2E_TENANTS))
            .runDbDeploy(true)
            .runCacheDeploy(true)
            .addTenant(TEST_TENANT)
            .prepareLocalExecutor(true)
            .startService(TracMetadataService.class)
            .startService(TracDataService.class)
            .startService(TracOrchestratorService.class)
            .startService(TracAdminService.class)
            .build();

    static TagHeader modelId;
    static TagHeader inputDataId;
    static TagHeader contextV1;
    static TagHeader contextV2;
    static TagHeader outputV1;
    static TagHeader outputV2;

    @Test @Order(101)
    void importModel() throws Exception {

        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("tracdap-services/tracdap-svc-orch/src/test/resources")
                .setEntryPoint("data_round_trip.DataRoundTripModel")
                .setVersion(GitHelpers.getCurrentCommit())
                .build();

        modelId = Helpers.doModelImport(platform, TEST_TENANT, modelStub, List.of(), List.of()).getHeader();
    }

    @Test @Order(102)
    void saveInputData() {

        inputDataId = platform.dataClientBlocking().createSmallDataset(DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(ROUND_TRIP_SCHEMA)
                .setFormat("text/csv")
                .setContent(ByteString.copyFromUtf8(ROUND_TRIP_CSV))
                .build());
    }

    @Test @Order(103)
    void createContext() {

        var metaClient = platform.metaClientBlocking();

        contextV1 = metaClient.createObject(MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setDefinition(contextDef(1))
                .build());

        contextV2 = metaClient.updateObject(MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.CUSTOM)
                .setPriorVersion(MetadataUtil.selectorFor(contextV1))
                .setDefinition(contextDef(2))
                .build());
    }

    @Test @Order(201)
    void runModel_contextOnJobAndOutputs() {

        var metaClient = platform.metaClientBlocking();

        var jobId = startJob(runModel("pandas").setContext(contextSelector(contextV1)));
        var jobStatus = Helpers.waitForJob(platform.orchClientBlocking(), TEST_TENANT, jobId);

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode());

        var jobTag = readLatest(jobId);
        checkContext(jobTag, contextV1);
        Assertions.assertEquals(contextSelector(contextV1), jobTag.getDefinition().getJob().getContext());

        var outputs = searchJobObjects(ObjectType.DATA, jobId);
        Assertions.assertEquals(1, outputs.size());

        var outputTag = readLatest(outputs.get(0).getHeader());
        checkContext(outputTag, contextV1);
        Helpers.checkJobOutputAttrs(metaClient, TEST_TENANT, outputTag, "round_trip_output", Set.of(), CONTEXT_ATTRS);

        checkLogFileContext(jobTag, contextV1);
        checkOrchestratorWritesHooked(jobId, contextV1);

        outputV1 = outputTag.getHeader();
    }

    @Test @Order(202)
    void runModel_failedJobLogHasContext() {

        var jobId = startJob(runModel("not_a_framework").setContext(contextSelector(contextV1)));
        var jobStatus = waitForCompletion(jobId);

        Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());

        var jobTag = readLatest(jobId);
        checkContext(jobTag, contextV1);
        checkLogFileContext(jobTag, contextV1);
    }

    @Test @Order(301)
    void runModel_oneOffPriorOutputsHasNoContext() {

        var job = runModel("pandas");
        job.getRunModelBuilder().putPriorOutputs("round_trip_output", MetadataUtil.selectorFor(outputV1));

        var jobId = startJob(job);
        Helpers.waitForJob(platform.orchClientBlocking(), TEST_TENANT, jobId);

        var outputTag = readLatest(outputV1);

        Assertions.assertEquals(2, outputTag.getHeader().getObjectVersion());
        Assertions.assertEquals(MetadataUtil.objectKey(jobId), MetadataCodec.decodeStringValue(outputTag.getAttrsOrThrow("trac_update_job")));
        Assertions.assertFalse(outputTag.containsAttrs(MetadataConstants.TRAC_JOB_CONTEXT_ATTR));
        Assertions.assertFalse(outputTag.containsAttrs(MetadataConstants.TRAC_JOB_CONTEXT_VERSION_ATTR));

        // The run's own version keeps its context
        checkContext(readObject(MetadataUtil.selectorFor(outputV1)), contextV1);

        outputV2 = outputTag.getHeader();
    }

    @Test @Order(302)
    void runModel_contextPriorOutputsHasOwnContext() {

        var job = runModel("pandas").setContext(contextSelector(contextV2));
        job.getRunModelBuilder().putPriorOutputs("round_trip_output", MetadataUtil.selectorFor(outputV2));

        var jobId = startJob(job);
        Helpers.waitForJob(platform.orchClientBlocking(), TEST_TENANT, jobId);

        var outputTag = readLatest(outputV2);

        Assertions.assertEquals(3, outputTag.getHeader().getObjectVersion());
        checkContext(outputTag, contextV2);
    }

    @Test @Order(303)
    void searchOutputsByContext() {

        // The latest version was written by a job with the context
        var latest = searchByContext(contextV1.getObjectId(), null, false);
        Assertions.assertEquals(List.of(3), matchingVersions(latest));

        // The latest version written under the first settings version is the first run's output
        var firstSettings = searchByContext(contextV1.getObjectId(), contextV1.getObjectVersion(), true);
        Assertions.assertEquals(List.of(1), matchingVersions(firstSettings));
    }

    private static List<org.finos.tracdap.metadata.Tag> searchByContext(String contextId, Integer contextVersion, boolean priorVersions) {

        var contextTerm = SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(MetadataConstants.TRAC_JOB_CONTEXT_ATTR)
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(MetadataCodec.encodeValue(contextId)))
                .build();

        var expression = contextVersion == null
                ? contextTerm
                : SearchExpression.newBuilder()
                        .setLogical(LogicalExpression.newBuilder()
                                .setOperator(LogicalOperator.AND)
                                .addExpr(contextTerm)
                                .addExpr(SearchExpression.newBuilder()
                                        .setTerm(SearchTerm.newBuilder()
                                                .setAttrName(MetadataConstants.TRAC_JOB_CONTEXT_VERSION_ATTR)
                                                .setAttrType(BasicType.INTEGER)
                                                .setOperator(SearchOperator.EQ)
                                                .setSearchValue(MetadataCodec.encodeValue(contextVersion)))))
                        .build();

        return platform.metaClientBlocking().search(MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setPriorVersions(priorVersions)
                        .setSearch(expression))
                .build())
                .getSearchResultList();
    }

    private static List<Integer> matchingVersions(List<org.finos.tracdap.metadata.Tag> results) {

        return results.stream()
                .filter(tag -> tag.getHeader().getObjectId().equals(outputV1.getObjectId()))
                .map(tag -> tag.getHeader().getObjectVersion())
                .collect(Collectors.toList());
    }

    private static void checkContext(org.finos.tracdap.metadata.Tag tag, TagHeader context) {

        Assertions.assertEquals(
                context.getObjectId(),
                MetadataCodec.decodeStringValue(tag.getAttrsOrThrow(MetadataConstants.TRAC_JOB_CONTEXT_ATTR)),
                MetadataUtil.objectKey(tag.getHeader()));

        Assertions.assertEquals(
                context.getObjectVersion(),
                MetadataCodec.decodeIntegerValue(tag.getAttrsOrThrow(MetadataConstants.TRAC_JOB_CONTEXT_VERSION_ATTR)),
                MetadataUtil.objectKey(tag.getHeader()));
    }

    private static void checkOrchestratorWritesHooked(TagHeader jobId, TagHeader context) {

        // The metadata write hook registered for this suite sees the orchestrator's own writes, and lets them through

        var jobWrites = TestJobHooks.OBJECT_WRITES.stream()
                .filter(write -> write.objectType() == ObjectType.JOB)
                .filter(write -> write.definition().getJob().getContext().equals(contextSelector(context)))
                .collect(Collectors.toList());

        Assertions.assertFalse(jobWrites.isEmpty(), "Job definition write");
        Assertions.assertTrue(jobWrites.stream().allMatch(write -> write.operation() == ObjectWrite.Operation.CREATE && !write.inBatch()));

        var resultWrites = TestJobHooks.OBJECT_WRITES.stream()
                .filter(write -> write.objectType() == ObjectType.RESULT)
                .filter(write -> write.definition().getResult().getJobId().getObjectId().equals(jobId.getObjectId()))
                .collect(Collectors.toList());

        Assertions.assertEquals(1, resultWrites.size(), "Result batch write");
        Assertions.assertTrue(resultWrites.get(0).inBatch());
    }

    private static void checkLogFileContext(org.finos.tracdap.metadata.Tag jobTag, TagHeader context) {

        var resultTag = readObject(jobTag.getDefinition().getJob().getResultId());
        var logFileTag = readObject(resultTag.getDefinition().getResult().getLogFileId());
        var logStorageTag = readObject(logFileTag.getDefinition().getFile().getStorageId());

        checkContext(logFileTag, context);
        checkContext(logStorageTag, context);
    }

    private static JobDefinition.Builder runModel(String dataFramework) {

        return JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(RunModelJob.newBuilder()
                        .setModel(MetadataUtil.selectorFor(modelId))
                        .putParameters("data_framework", MetadataCodec.encodeValue(dataFramework))
                        .putInputs("round_trip_input", MetadataUtil.selectorFor(inputDataId)));
    }

    private static ObjectDefinition contextDef(int schemaVersion) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setCustom(CustomDefinition.newBuilder()
                        .setCustomSchemaType("test_job_context")
                        .setCustomSchemaVersion(schemaVersion)
                        .setCustomData(ByteString.copyFromUtf8("{}")))
                .build();
    }

    private static TagSelector contextSelector(TagHeader context) {

        return TagSelector.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setObjectId(context.getObjectId())
                .setObjectVersion(context.getObjectVersion())
                .setLatestTag(true)
                .build();
    }

    private static TagHeader startJob(JobDefinition.Builder job) {

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(job)
                .build();

        return Helpers.startJob(platform.orchClientBlocking(), jobRequest).getJobId();
    }

    private static JobStatus waitForCompletion(TagHeader jobId) {

        var statusRequest = JobStatusRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(jobId))
                .build();

        var jobStatus = platform.orchClientBlocking().checkJob(statusRequest);

        while (!COMPLETED_STATES.contains(jobStatus.getStatusCode())) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            jobStatus = platform.orchClientBlocking().checkJob(statusRequest);
        }

        return jobStatus;
    }

    private static org.finos.tracdap.metadata.Tag readLatest(TagHeader objectId) {

        return readObject(MetadataUtil.selectorForLatest(objectId));
    }

    private static org.finos.tracdap.metadata.Tag readObject(TagSelector selector) {

        return platform.metaClientBlocking().readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selector)
                .build());
    }

    private static List<org.finos.tracdap.metadata.Tag> searchJobObjects(ObjectType objectType, TagHeader jobId) {

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                        .setObjectType(objectType)
                        .setSearch(SearchExpression.newBuilder()
                                .setTerm(SearchTerm.newBuilder()
                                        .setAttrName("trac_create_job")
                                        .setAttrType(BasicType.STRING)
                                        .setOperator(SearchOperator.EQ)
                                        .setSearchValue(MetadataCodec.encodeValue(MetadataUtil.objectKey(jobId))))))
                .build();

        return platform.metaClientBlocking().search(searchRequest).getSearchResultList();
    }

    private static FieldSchema.Builder field(String fieldName, BasicType fieldType, int fieldOrder) {

        return FieldSchema.newBuilder()
                .setFieldName(fieldName)
                .setFieldType(fieldType)
                .setFieldOrder(fieldOrder);
    }
}
