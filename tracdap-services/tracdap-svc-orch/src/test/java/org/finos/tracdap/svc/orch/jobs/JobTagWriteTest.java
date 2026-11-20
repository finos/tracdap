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
import org.finos.tracdap.common.service.TagWrite;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;


/**
 * Tag writes from other clients on a job while it runs: the orchestrator's own job tag writes
 * are resent over the newer tag, the job completes with its outputs and the other writes are kept.
 */
@Tag("integration")
@Tag("int-e2e")
public class JobTagWriteTest {

    private static final String TEST_TENANT = "ACME_CORP";
    private static final String E2E_CONFIG = "config/trac-e2e.yaml";
    private static final String E2E_TENANTS = "config/trac-e2e-tenants.yaml";

    private static final String JOB_MARKER_ATTR = "job_tag_write_test";

    private static final Set<JobStatusCode> COMPLETED_STATES = Set.of(
            JobStatusCode.SUCCEEDED, JobStatusCode.FAILED, JobStatusCode.CANCELLED);

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

    @AfterEach
    void clearHookCallback() {
        TestJobHooks.tagWriteCallback = null;
    }

    @Test
    void runModel_tagWritesWhileRunning() throws Exception {

        var modelId = importModel();
        var inputDataId = saveInputData();

        var marker = UUID.randomUUID().toString();
        var statusWriteRaced = new AtomicBoolean(false);
        var resultWriteRaced = new AtomicBoolean(false);

        // Another client writes the job tag just before the orchestrator's own write is saved, once for a status
        // update and once for the result batch, so each of the orchestrator's writes names a superseded tag

        TestJobHooks.tagWriteCallback = write -> {

            if (!isMarkedJob(write, marker))
                return;

            var raced = write.inBatch() ? resultWriteRaced : statusWriteRaced;

            if (raced.compareAndSet(false, true))
                platform.metaClientBlocking().updateTag(otherClientWrite(write, raced == resultWriteRaced));
        };

        var jobRequest = JobRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_MODEL)
                        .setRunModel(RunModelJob.newBuilder()
                                .setModel(MetadataUtil.selectorFor(modelId))
                                .putParameters("data_framework", MetadataCodec.encodeValue("pandas"))
                                .putInputs("round_trip_input", MetadataUtil.selectorFor(inputDataId))))
                .addJobAttrs(TagUpdate.newBuilder()
                        .setAttrName(JOB_MARKER_ATTR)
                        .setValue(MetadataCodec.encodeValue(marker)))
                .build();

        var jobId = Helpers.startJob(platform.orchClientBlocking(), jobRequest).getJobId();
        var jobStatus = waitForCompletion(jobId);

        Assertions.assertTrue(statusWriteRaced.get(), "No status update was raced");
        Assertions.assertTrue(resultWriteRaced.get(), "The result save was not raced");

        Assertions.assertEquals(JobStatusCode.SUCCEEDED, jobStatus.getStatusCode(), jobStatus.getStatusMessage());

        var jobTag = readObject(MetadataUtil.selectorForLatest(jobId));
        var jobAttrs = jobTag.getAttrsMap();

        // The orchestrator's updates are on the latest tag, with both writes from the other client
        Assertions.assertEquals("SUCCEEDED", MetadataCodec.decodeStringValue(jobAttrs.get(MetadataConstants.TRAC_JOB_STATUS_ATTR)));
        Assertions.assertEquals("during_status_update", MetadataCodec.decodeStringValue(jobAttrs.get("status_update_edit")));
        Assertions.assertEquals("during_result_save", MetadataCodec.decodeStringValue(jobAttrs.get("result_save_edit")));

        // The result and its output are saved
        var result = readObject(jobTag.getDefinition().getJob().getResultId()).getDefinition().getResult();
        Assertions.assertEquals(JobStatusCode.SUCCEEDED, result.getStatusCode());
        Assertions.assertTrue(result.containsOutputs("round_trip_output"));

        var output = readObject(result.getOutputsOrThrow("round_trip_output"));
        Assertions.assertEquals(ObjectType.DATA, output.getHeader().getObjectType());
        Assertions.assertEquals(MetadataUtil.objectKey(jobId), MetadataCodec.decodeStringValue(output.getAttrsOrThrow("trac_create_job")));
    }

    private static boolean isMarkedJob(TagWrite write, String marker) {

        if (write.objectType() != ObjectType.JOB)
            return false;

        var markerAttr = write.priorTag().getAttrsOrDefault(JOB_MARKER_ATTR, null);

        return markerAttr != null && MetadataCodec.decodeStringValue(markerAttr).equals(marker);
    }

    private static MetadataWriteRequest otherClientWrite(TagWrite write, boolean resultSave) {

        var attrName = resultSave ? "result_save_edit" : "status_update_edit";
        var attrValue = resultSave ? "during_result_save" : "during_status_update";

        return MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.JOB)
                .setPriorVersion(MetadataUtil.selectorFor(write.priorTag().getHeader()))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName(attrName)
                        .setValue(MetadataCodec.encodeValue(attrValue)))
                .build();
    }

    private static TagHeader importModel() throws Exception {

        var modelStub = ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("TRAC_LOCAL_REPO")
                .setPath("tracdap-services/tracdap-svc-orch/src/test/resources")
                .setEntryPoint("data_round_trip.DataRoundTripModel")
                .setVersion(GitHelpers.getCurrentCommit())
                .build();

        return Helpers.doModelImport(platform, TEST_TENANT, modelStub, List.of(), List.of()).getHeader();
    }

    private static TagHeader saveInputData() {

        return platform.dataClientBlocking().createSmallDataset(DataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSchema(ROUND_TRIP_SCHEMA)
                .setFormat("text/csv")
                .setContent(ByteString.copyFromUtf8(ROUND_TRIP_CSV))
                .build());
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

    private static org.finos.tracdap.metadata.Tag readObject(TagSelector selector) {

        return platform.metaClientBlocking().readObject(MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(selector)
                .build());
    }

    private static FieldSchema.Builder field(String fieldName, BasicType fieldType, int fieldOrder) {

        return FieldSchema.newBuilder()
                .setFieldName(fieldName)
                .setFieldType(fieldType)
                .setFieldOrder(fieldOrder);
    }
}
