/*
 * Copyright 2024 finTRAC Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.validation.version;

import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.test.BaseValidatorTest;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.meta.TestData;
import org.junit.jupiter.api.Test;


class ModelVersionValidationTest extends BaseValidatorTest {

    // Model type, language and coordinates must be the same between versions
    // Everything else (params, inputs, outputs) is allowed to change
    // This includes structural changes e.g. the type or name of fields in inputs / outputs

    @Test
    void versionChangesModelType() {

        var modelV1 = TestData.dummyModelDef();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .setModelType(ModelType.DATA_IMPORT_MODEL))
                .build();

        expectInvalidVersion(modelV2, modelV1);
    }

    @Test
    void versionChangesModelLanguage() {

        var modelV1 = TestData.dummyModelDef();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .setLanguage("scala"))
                .build();

        expectInvalidVersion(modelV2, modelV1);
    }

    @Test
    void versionChangesModelCoordinates() {

        var modelV1 = TestData.dummyModelDef();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .setPackageGroup("com.acme.different.division")
                .setPath("altered/layout/src"))
                .build();

        expectInvalidVersion(modelV2, modelV1);
    }

    @Test
    void versionAddsParameter() {

        var modelV1 = TestData.dummyModelDef();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .putParameters("a_new_parameter", ModelParameter.newBuilder()
                        .setParamType(TypeSystem.descriptor(BasicType.BOOLEAN))
                        .setLabel("A new parameter not in the prior version")
                        .build()))
                .build();

        expectValidVersion(modelV2, modelV1);
    }

    @Test
    void versionChangesParameter() {

        var modelV1 = TestData.dummyModelDef();

        var param1 = modelV1.getModel().getParametersOrThrow("param1");
        var param1V2 = param1.toBuilder()
                .setParamType(TypeSystem.descriptor(BasicType.DATETIME))
                .build();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .putParameters("param1", param1V2))
                .build();

        expectValidVersion(modelV2, modelV1);
    }

    @Test
    void versionAddsInput() {

        var modelV1 = TestData.dummyModelDef();

        var newInput = ModelInputSchema.newBuilder()
                .setSchema(SchemaDefinition.newBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setTable(TableSchema.newBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("an_example_field")
                        .setFieldType(BasicType.DECIMAL))))
                .build();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .putInputs("an_extra_input", newInput))
                .build();

        expectValidVersion(modelV2, modelV1);
    }

    @Test
    void versionAddsInputField() {

        var modelV1 = TestData.dummyModelDef();

        var input1 = modelV1.getModel().getInputsOrThrow("input1");
        var input1V2 = input1.toBuilder()
                .setSchema(input1.getSchema().toBuilder()
                .setTable(input1.getSchema().getTable().toBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("an_extra_field")
                        .setFieldType(BasicType.STRING))))
                .build();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .putInputs("input1", input1V2))
                .build();

        expectValidVersion(modelV2, modelV1);
    }

    @Test
    void versionChangesInputField() {

        var modelV1 = TestData.dummyModelDef();

        var input1 = modelV1.getModel().getInputsOrThrow("input1");
        var field1 = input1.getSchema().getTable().getFields(0);
        var field1V2 = field1.toBuilder().setFieldType(BasicType.STRING).build();
        var input1V2 = input1.toBuilder()
                .setSchema(input1.getSchema().toBuilder()
                .setTable(input1.getSchema().getTable().toBuilder()
                .setFields(0, field1V2)))
                .build();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .putInputs("input1", input1V2))
                .build();

        expectValidVersion(modelV2, modelV1);
    }

    @Test
    void versionAddsOutput() {

        var modelV1 = TestData.dummyModelDef();

        var newOutput = ModelOutputSchema.newBuilder()
                .setSchema(SchemaDefinition.newBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setTable(TableSchema.newBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("an_example_field")
                        .setFieldType(BasicType.DECIMAL))))
                .build();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .putOutputs("an_extra_output", newOutput))
                .build();

        expectValidVersion(modelV2, modelV1);
    }

    @Test
    void versionAddsOutputField() {

        var modelV1 = TestData.dummyModelDef();

        var output1 = modelV1.getModel().getOutputsOrThrow("output1");
        var output1V2 = output1.toBuilder()
                .setSchema(output1.getSchema().toBuilder()
                .setTable(output1.getSchema().getTable().toBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setFieldName("an_extra_field")
                        .setFieldType(BasicType.STRING))))
                .build();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .putOutputs("output1", output1V2))
                .build();

        expectValidVersion(modelV2, modelV1);
    }

    @Test
    void versionChangesOutputField() {

        var modelV1 = TestData.dummyModelDef();

        var output1 = modelV1.getModel().getOutputsOrThrow("output1");
        var field1 = output1.getSchema().getTable().getFields(0);
        var field1V2 = field1.toBuilder().setFieldType(BasicType.STRING).build();
        var output1V2 = output1.toBuilder()
                .setSchema(output1.getSchema().toBuilder()
                .setTable(output1.getSchema().getTable().toBuilder()
                .setFields(0, field1V2)))
                .build();

        var modelV2 = modelV1.toBuilder()
                .setModel(modelV1.getModel().toBuilder()
                .putOutputs("output1", output1V2))
                .build();

        expectValidVersion(modelV2, modelV1);
    }
}
