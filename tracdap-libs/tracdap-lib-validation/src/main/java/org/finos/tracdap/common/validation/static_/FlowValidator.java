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

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class FlowValidator {

    private static final Descriptors.Descriptor FLOW_DEFINITION;
    private static final Descriptors.FieldDescriptor FD_NODES;
    private static final Descriptors.FieldDescriptor FD_EDGES;

    private static final Descriptors.Descriptor FLOW_NODE;
    private static final Descriptors.FieldDescriptor FN_NODE_TYPE;
    private static final Descriptors.FieldDescriptor FN_NODE_SEARCH;
    private static final Descriptors.FieldDescriptor FN_MODEL_STUB;

    private static final Descriptors.Descriptor FLOW_EDGE;
    private static final Descriptors.FieldDescriptor FE_SOURCE;
    private static final Descriptors.FieldDescriptor FE_TARGET;

    private static final Descriptors.Descriptor FLOW_SOCKET;
    private static final Descriptors.FieldDescriptor FS_NODE;
    private static final Descriptors.FieldDescriptor FS_SOCKET;

    private static final Descriptors.Descriptor FLOW_MODEL_STUB;
    private static final Descriptors.FieldDescriptor FMS_PARAMETERS;
    private static final Descriptors.FieldDescriptor FMS_INPUTS;
    private static final Descriptors.FieldDescriptor FMS_OUTPUTS;

    static {

        FLOW_DEFINITION = FlowDefinition.getDescriptor();
        FD_NODES = field(FLOW_DEFINITION, FlowDefinition.NODES_FIELD_NUMBER);
        FD_EDGES = field(FLOW_DEFINITION, FlowDefinition.EDGES_FIELD_NUMBER);

        FLOW_NODE = FlowNode.getDescriptor();
        FN_NODE_TYPE = field(FLOW_NODE, FlowNode.NODETYPE_FIELD_NUMBER);
        FN_NODE_SEARCH = field(FLOW_NODE, FlowNode.NODESEARCH_FIELD_NUMBER);
        FN_MODEL_STUB = field(FLOW_NODE, FlowNode.MODELSTUB_FIELD_NUMBER);

        FLOW_EDGE = FlowEdge.getDescriptor();
        FE_SOURCE = field(FLOW_EDGE, FlowEdge.SOURCE_FIELD_NUMBER);
        FE_TARGET = field(FLOW_EDGE, FlowEdge.SOURCE_FIELD_NUMBER);

        FLOW_SOCKET = FlowSocket.getDescriptor();
        FS_NODE = field(FLOW_SOCKET, FlowSocket.NODE_FIELD_NUMBER);
        FS_SOCKET = field(FLOW_SOCKET, FlowSocket.SOCKET_FIELD_NUMBER);

        FLOW_MODEL_STUB = FlowModelStub.getDescriptor();
        FMS_PARAMETERS = field(FLOW_MODEL_STUB, FlowModelStub.PARAMETERS_FIELD_NUMBER);
        FMS_INPUTS = field(FLOW_MODEL_STUB, FlowModelStub.INPUTS_FIELD_NUMBER);
        FMS_OUTPUTS = field(FLOW_MODEL_STUB, FlowModelStub.OUTPUTS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext flow(FlowDefinition msg, ValidationContext ctx) {

        ctx = ctx.pushMap(FD_NODES)
                .apply(CommonValidators::mapNotEmpty)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapValues(FlowValidator::flowNode, FlowNode.class)
                .pop();

        ctx = ctx.pushRepeated(FD_EDGES)
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated(FlowValidator::flowEdge, FlowEdge.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext flowNode(FlowNode msg, ValidationContext ctx) {

        ctx = ctx.push(FN_NODE_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, FlowNodeType.class)
                .pop();

        ctx = ctx.push(FN_NODE_SEARCH)
                .apply(CommonValidators::optional)
                .apply(SearchValidator::searchExpression, SearchExpression.class)
                .pop();

        var isModelNode = msg.getNodeType() == FlowNodeType.MODEL_NODE;

        ctx = ctx.push(FN_MODEL_STUB)
                .apply(CommonValidators.ifAndOnlyIf(isModelNode))
                .apply(FlowValidator::flowModelStub, FlowModelStub.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext flowEdge(FlowEdge msg, ValidationContext ctx) {

        ctx = ctx.push(FE_SOURCE)
                .apply(CommonValidators::required)
                .apply(FlowValidator::flowSocket, FlowSocket.class)
                .pop();

        ctx = ctx.push(FE_TARGET)
                .apply(CommonValidators::required)
                .apply(FlowValidator::flowSocket, FlowSocket.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext flowSocket(FlowSocket msg, ValidationContext ctx) {

        ctx = ctx.push(FS_NODE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(FS_SOCKET)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::identifier)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext flowModelStub(FlowModelStub msg, ValidationContext ctx) {

        ctx = ctx.pushMap(FMS_PARAMETERS)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapValues(TypeSystemValidator::typeDescriptor, TypeDescriptor.class)
                .pop();

        ctx = ctx.pushRepeated(FMS_INPUTS)
                .applyRepeated(CommonValidators::identifier, String.class)
                .applyRepeated(CommonValidators::notTracReserved, String.class)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .pop();

        ctx = ctx.pushRepeated(FMS_OUTPUTS)
                .applyRepeated(CommonValidators::identifier, String.class)
                .applyRepeated(CommonValidators::notTracReserved, String.class)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .pop();

        return ctx;
    }
}
