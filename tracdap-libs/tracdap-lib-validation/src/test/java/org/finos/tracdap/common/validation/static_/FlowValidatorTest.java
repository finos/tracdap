/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.test.BaseValidatorTest;
import org.finos.tracdap.metadata.*;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;


public class FlowValidatorTest extends BaseValidatorTest {

    @Test
    void basicFlow_ok1() {

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                    .addInputs("input_1").addInputs("input_2")
                    .addOutputs("output_1")
                    .putNodeProps("sample_prop", MetadataCodec.encodeValue(2.0))
                    .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .build();

        expectValid(flow);
    }

    @Test
    void basicFlow_ok2() {

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("in1").addInputs("in2")
                        .addOutputs("out1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("in1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("in2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("out1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .build();

        expectValid(flow);
    }

    @Test
    void basicFlow_ok3() {

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))

                // Flow schema
                .putInputs("input_1", ModelInputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .putInputs("input_2", ModelInputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .putOutputs("output_1", ModelOutputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .putParameters("param_1", ModelParameter.newBuilder()
                        .setParamType(TypeSystem.descriptor(BasicType.DATE))
                        .setLabel("An example parameter")
                        .setDefaultValue(MetadataCodec.encodeValue(LocalDate.now()))
                        .build())

                .build();

        expectValid(flow);
    }

    @Test
    void basicFlow_ok4() {

        // This flow uses one input twice (input 1 feeds both models)
        // Model 1 flows into model 2
        // Should all be allowed

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("model_2", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_2").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_2").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_2").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .build();

        expectValid(flow);
    }

    @Test
    void basicFlow_unmetOutput() {

        // Output output_2 is not fed by anything

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())
                .putNodes("output_2", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_unusedModel_1() {

        // The model model_2 is not connected on inputs or outputs

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // This model is not connected to anything in the flow
                .putNodes("model_2", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_unusedModel_2() {

        // Model model_2 has all its inputs satisfied, but none of the outputs are used

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // This model does not have any of its outputs used
                .putNodes("model_2", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))

                // Wire up inputs for model 2, but not outputs
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_2").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_2").setSocket("input_2")))

                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_danglingEdge_1() {

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))

                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_danglingEdge_2() {

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))

                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_danglingEdge_3() {

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_2")))

                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_missingEdge() {

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_backwardsEege() {

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setTarget(FlowSocket.newBuilder().setNode("input_2"))
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_twoEdgesOneSocket() {

        // Two edges provide input to model_1.input_1, this is not allowed

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_cyclicDependency() {

        // This flow has a cyclic dependency between model 1 and model 2

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2").addInputs("input_3")
                        .addOutputs("output_1")
                        .build())
                .putNodes("model_2", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_2").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_3")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_2").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_2").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_2").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))
                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_mismatchedSchema_1() {

        // This flow declares an explicit schema that does not match the declared input / output nodes

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("input_2", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_2"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))

                // Flow schema
                .putInputs("input_1", ModelInputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .putOutputs("output_1", ModelOutputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_mismatchedSchema_2() {

        // This flow declares an explicit schema that does not match the declared input / output nodes

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))

                // Flow schema
                .putInputs("input_1", ModelInputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .putInputs("input_2", ModelInputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .putOutputs("output_1", ModelOutputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .build();

        expectInvalid(flow);
    }

    @Test
    void basicFlow_mismatchedSchema_3() {

        // This flow declares an explicit schema that does not match the declared input / output nodes
        // For this test the differences are only in casing

        var flow = FlowDefinition.newBuilder()

                // Nodes
                .putNodes("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNodes("model_1", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE)
                        .addInputs("input_1").addInputs("input_2")
                        .addOutputs("output_1")
                        .build())
                .putNodes("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())

                // Edges
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_1")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("input_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("model_1").setSocket("input_2")))
                .addEdges(FlowEdge.newBuilder()
                        .setSource(FlowSocket.newBuilder().setNode("model_1").setSocket("output_1"))
                        .setTarget(FlowSocket.newBuilder().setNode("output_1")))

                // Flow schema
                .putInputs("INPUT_1", ModelInputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .putOutputs("OUTPUT_1", ModelOutputSchema.newBuilder()
                        .setSchema(SchemaDefinition.newBuilder()
                        .setSchemaType(SchemaType.TABLE)
                        .setPartType(PartType.PART_ROOT)
                        .setTable(TableSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_1")
                                .setFieldOrder(0)
                                .setFieldType(BasicType.STRING)
                                .setLabel("A string field"))
                        .addFields(FieldSchema.newBuilder()
                                .setFieldName("field_2")
                                .setFieldOrder(1)
                                .setFieldType(BasicType.FLOAT)
                                .setLabel("A float field"))))
                        .build())

                .build();

        expectInvalid(flow);
    }
}
