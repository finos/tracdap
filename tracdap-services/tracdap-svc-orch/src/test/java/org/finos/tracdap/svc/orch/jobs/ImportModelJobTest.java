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

import org.finos.tracdap.common.exception.EJobResult;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.config.JobConfig;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.JobResultAttrs;
import org.finos.tracdap.metadata.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


public class ImportModelJobTest {

    private static final TagHeader MODEL_ID = TagHeader.newBuilder()
            .setObjectType(ObjectType.MODEL)
            .setObjectId(UUID.randomUUID().toString())
            .setObjectVersion(1)
            .setTagVersion(1)
            .build();

    private static TagUpdate attr(String attrName, String value) {

        return TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_OR_REPLACE_ATTR)
                .setAttrName(attrName)
                .setValue(MetadataCodec.encodeValue(value))
                .build();
    }

    private static JobConfig jobConfig(List<TagUpdate> suppliedAttrs) {

        var importModel = org.finos.tracdap.metadata.ImportModelJob.newBuilder()
                .addAllModelAttrs(suppliedAttrs);

        var job = JobDefinition.newBuilder()
                .setJobType(JobType.IMPORT_MODEL)
                .setImportModel(importModel);

        return JobConfig.newBuilder()
                .setJob(job)
                .build();
    }

    private static JobResult runtimeResult(List<TagUpdate> modelAttrs) {

        var modelKey = MetadataUtil.objectKey(MODEL_ID);

        return JobResult.newBuilder()
                .putObjects(modelKey, ObjectDefinition.newBuilder().setObjectType(ObjectType.MODEL).build())
                .putAttrs(modelKey, JobResultAttrs.newBuilder().addAllAttrs(modelAttrs).build())
                .build();
    }

    private static JobResult processResult(List<TagUpdate> modelAttrs, List<TagUpdate> suppliedAttrs) {

        var resultIds = Map.of(MetadataUtil.objectKey(MODEL_ID), MODEL_ID);

        return new ImportModelJob().processResult(jobConfig(suppliedAttrs), runtimeResult(modelAttrs), resultIds);
    }

    @Test
    void modelAttrs_ok() {

        var result = processResult(
                List.of(attr("model_family", "pnl_aggregation")),
                List.of(attr("model_owner", "risk_team")));

        var attrs = result.getAttrsOrThrow(MetadataUtil.objectKey(MODEL_ID)).getAttrsList();
        var attrNames = attrs.stream().map(TagUpdate::getAttrName).collect(Collectors.toList());

        Assertions.assertEquals(List.of("model_family", "model_owner"), attrNames);
    }

    @ParameterizedTest
    @ValueSource(strings = {"trac_model_entry_point", "_hidden_attr", "TRAC_CREATE_USER_ID", "tracdap_internal"})
    void modelAttrs_reserved(String attrName) {

        Assertions.assertThrows(EJobResult.class, () -> processResult(List.of(attr(attrName, "value")), List.of()));
    }
}
