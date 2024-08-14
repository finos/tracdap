/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.graph;

import org.finos.tracdap.metadata.*;


public class NodeMetadata {

    private final FlowNode flowNode;

    private final ModelParameter modelParameter;
    private final ModelInputSchema modelInputSchema;
    private final ModelOutputSchema modelOutputSchema;

    private final ObjectDefinition runtimeObject;
    private final Value runtimeValue;

    public NodeMetadata(
            FlowNode flowNode,
            ModelParameter modelParameter, ModelInputSchema modelInputSchema, ModelOutputSchema modelOutputSchema,
            ObjectDefinition runtimeObject, Value runtimeValue) {

        this.flowNode = flowNode;
        this.modelParameter = modelParameter;
        this.modelInputSchema = modelInputSchema;
        this.modelOutputSchema = modelOutputSchema;
        this.runtimeObject = runtimeObject;
        this.runtimeValue = runtimeValue;
    }

    public NodeMetadata withFlowNode(FlowNode flowNode) {
        return new NodeMetadata(flowNode, modelParameter, modelInputSchema, modelOutputSchema, runtimeObject, runtimeValue);
    }

    public NodeMetadata withModelParameter(ModelParameter modelParameter) {
        return new NodeMetadata(flowNode, modelParameter, modelInputSchema, modelOutputSchema, runtimeObject, runtimeValue);
    }

    public NodeMetadata withModelInputSchema(ModelInputSchema modelInputSchema) {
        return new NodeMetadata(flowNode, modelParameter, modelInputSchema, modelOutputSchema, runtimeObject, runtimeValue);
    }

    public NodeMetadata withModelOutputSchema(ModelOutputSchema modelOutputSchema) {
        return new NodeMetadata(flowNode, modelParameter, modelInputSchema, modelOutputSchema, runtimeObject, runtimeValue);
    }

    public NodeMetadata withRuntimeObject(ObjectDefinition runtimeObject) {
        return new NodeMetadata(flowNode, modelParameter, modelInputSchema, modelOutputSchema, runtimeObject, runtimeValue);
    }

    public NodeMetadata withRuntimeValue(Value runtimeValue) {
        return new NodeMetadata(flowNode, modelParameter, modelInputSchema, modelOutputSchema, runtimeObject, runtimeValue);
    }

    public FlowNode flowNode() {
        return flowNode;
    }

    public ModelParameter modelParameter() {
        return modelParameter;
    }

    public ModelInputSchema modelInputSchema() {
        return modelInputSchema;
    }

    public ModelOutputSchema modelOutputSchema() {
        return modelOutputSchema;
    }

    public ObjectType runtimeObjectType() {
        if (runtimeObject == null)
            return ObjectType.OBJECT_TYPE_NOT_SET;
        return runtimeObject.getObjectType();
    }

    public ObjectDefinition runtimeObject() {
        return runtimeObject;
    }

    public Value runtimeValue() {
        return runtimeValue;
    }
}
