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
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.metadata.RunFlowJob;
import org.finos.tracdap.metadata.RunModelJob;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.svc.orch.TracOrchestratorService;
import org.finos.tracdap.test.helpers.GitHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


@Tag("integration")
@Tag("int-e2e")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FailedJobLogTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";

    private static final Set<JobStatusCode> COMPLETED_STATES = Set.of(
            JobStatusCode.SUCCEEDED, JobStatusCode.FAILED, JobStatusCode.CANCELLED);

    private static final SchemaDefinition ROUND_TRIP_SCHEMA = SchemaDefinition.newBuilder()
            .setSchemaType(SchemaType.TABLE)
            .setTable(TableSchema.newBuilder()
                    .addFields(field("boolean_field", BasicType.BOOLEAN, 0, false))
                    .addFields(field("integer_field", BasicType.INTEGER, 1, false))
                    .addFields(field("float_field", BasicType.FLOAT, 2, false))
                    .addFields(field("decimal_field", BasicType.DECIMAL, 3, false))
                    .addFields(field("string_field", BasicType.STRING, 4, false))
                    .addFields(field("categorical_field", BasicType.STRING, 5, true))
                    .addFields(field("date_field", BasicType.DATE, 6, false))
                    .addFields(field("datetime_field", BasicType.DATETIME, 7, false)))
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
    static TagHeader flowId;

    @Test @Order(101)
    void importModel() throws Exception {

        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("tracdap-services/tracdap-svc-orch/src/test/resources")
                .setEntryPoint("data_round_trip.DataRoundTripModel")
                .setVersion(GitHelpers.getCurrentCommit())
                .build();

        var modelTag = Helpers.doModelImport(platform, TEST_TENANT, modelStub, List.of(), List.of());

        modelId = modelTag.getHeader();
    }

    @Test @Order(102)
    void saveInputData() {

        var writeRequest = DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(ROUND_TRIP_SCHEMA)
                .setFormat("text/csv")
                .setContent(ByteString.copyFromUtf8(ROUND_TRIP_CSV))
                .build();

        inputDataId = platform.dataClientBlocking().createSmallDataset(writeRequest);
    }

    @Test @Order(103)
    void createFlow() {

        var flowDef = FlowDefinition.newBuilder()
                .putNodes("round_trip_input", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("round_trip", FlowNode.newBuilder()
                        .setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("round_trip_input")
                        .addOutputs("round_trip_output")
                        .build())
                .putNodes("round_trip_output", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("round_trip_input"))
                        .setTarget(FlowSocket.newBuilder().setNode("round_trip").setSocket("round_trip_input")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("round_trip").setSocket("round_trip_output"))
                        .setTarget(FlowSocket.newBuilder().setNode("round_trip_output")))
                .build();

        var createRequest = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.FLOW)
                .setDefinition(ObjectDefinition.newBuilder().setObjectType(ObjectType.FLOW).setFlow(flowDef))
                .build();

        flowId = platform.metaClientBlocking().createObject(createRequest);
    }

    @Test @Order(201)
    void runModel_multiLineError() {

        var jobId = runModel("not_a_framework\nsecond line");
        var jobStatus = waitForCompletion(jobId);

        Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());
        Assertions.assertTrue(jobStatus.getStatusMessage().contains("Unsupported data framework: [not_a_framework\nsecond line]"), jobStatus.getStatusMessage());

        var result = Helpers.checkFailedJobResult(platform, TEST_TENANT, jobId);

        Assertions.assertEquals(jobStatus.getStatusMessage(), result.getStatusMessage());
        Assertions.assertEquals(result.getStatusMessage(), Helpers.jobErrorMessage(platform, TEST_TENANT, jobId));
        Assertions.assertEquals(0, result.getOutputsCount());
        Assertions.assertEquals(0, searchJobObjects(ObjectType.DATA, jobId).size());
    }

    @Test @Order(202)
    void runModel_longError() {

        var longValue = "x".repeat(5000);

        var jobId = runModel(longValue);
        var jobStatus = waitForCompletion(jobId);

        Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());

        var result = Helpers.checkFailedJobResult(platform, TEST_TENANT, jobId);
        var errorMessage = Helpers.jobErrorMessage(platform, TEST_TENANT, jobId);

        Assertions.assertTrue(result.getStatusMessage().contains(longValue), result.getStatusMessage());
        Assertions.assertTrue(errorMessage.getBytes(StandardCharsets.UTF_8).length <= 4000);
        Assertions.assertTrue(errorMessage.endsWith("… (truncated)"), errorMessage);
        Assertions.assertTrue(result.getStatusMessage().startsWith(errorMessage.substring(0, errorMessage.length() - "… (truncated)".length())));
    }

    @Test @Order(301)
    void runFlow_error() {

        var runFlow = RunFlowJob.newBuilder()
                .setFlow(MetadataUtil.selectorFor(flowId))
                .putParameters("data_framework", MetadataCodec.encodeValue("not_a_framework"))
                .putInputs("round_trip_input", MetadataUtil.selectorFor(inputDataId))
                .putModels("round_trip", MetadataUtil.selectorFor(modelId))
                .build();

        var jobId = startJob(JobDefinition.newBuilder()
                .setJobType(JobType.RUN_FLOW)
                .setRunFlow(runFlow)
                .build());

        var jobStatus = waitForCompletion(jobId);

        Assertions.assertEquals(JobStatusCode.FAILED, jobStatus.getStatusCode());
        Assertions.assertTrue(jobStatus.getStatusMessage().contains("Unsupported data framework: [not_a_framework]"), jobStatus.getStatusMessage());

        var result = Helpers.checkFailedJobResult(platform, TEST_TENANT, jobId);

        Assertions.assertEquals(0, result.getOutputsCount());
        Assertions.assertEquals(0, searchJobObjects(ObjectType.DATA, jobId).size());
    }

    private static TagHeader runModel(String dataFramework) {

        var runModel = RunModelJob.newBuilder()
                .setModel(MetadataUtil.selectorFor(modelId))
                .putParameters("data_framework", MetadataCodec.encodeValue(dataFramework))
                .putInputs("round_trip_input", MetadataUtil.selectorFor(inputDataId))
                .build();

        return startJob(JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setRunModel(runModel)
                .build());
    }

    private static TagHeader startJob(JobDefinition job) {

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

    private static FieldSchema.Builder field(String fieldName, BasicType fieldType, int fieldOrder, boolean categorical) {

        return FieldSchema.newBuilder()
                .setFieldName(fieldName)
                .setFieldType(fieldType)
                .setFieldOrder(fieldOrder)
                .setCategorical(categorical);
    }
}
