#  Copyright 2020 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import trac.rt.config.config as config
import trac.rt.exceptions as _ex

from .graph import *


class GraphBuilder:

    @staticmethod
    def build_job(job_config: config.JobConfig) -> Graph:

        target_def = job_config.objects.get(job_config.target)

        if target_def is None:
            raise _ex.EConfigParse(f"No definition available for job target '{job_config.target}'")

        # Only calculation jobs are supported at present
        return GraphBuilder.build_calculation_job(job_config)

    @classmethod
    def build_calculation_job(cls, job_config: config.JobConfig) -> Graph:

        job_namespace = NodeNamespace(f"job={job_config.job_id}")
        null_graph = Graph({}, NodeId('', job_namespace))

        # Create a job context with no dependencies and no external data mappings
        ctx_push_graph = GraphBuilder.build_context_push(
            job_namespace, null_graph, input_mapping=dict())

        # Input graph will prepare data views job inputs
        input_graph = cls.build_job_inputs(job_config, job_namespace, ctx_push_graph)

        # Now create the root execution node, which will be either a single model or a flow
        # The root exec node can run directly in the job context, no need to do a context push
        # All input views are already mapped and there is only a single execution target

        job_target_obj = job_config.objects.get(job_config.target)
        job_target_graph = GraphBuilder.build_model_or_flow(
            job_config, job_namespace, input_graph, job_target_obj)

        # Output graph will extract and save data items from job-level output data views

        output_graph = cls.build_job_outputs(job_config, job_namespace, job_target_graph)

        # Build job-level metadata outputs

        output_metadata_nodes = frozenset(
            nid for nid, n in output_graph.nodes.items()
            if isinstance(n, JobOutputMetadataNode))

        job_metadata_id = NodeId("trac_job_metadata", job_namespace)
        job_metadata_node = JobResultMetadataNode(job_metadata_id, output_metadata_nodes)

        job_logs_node = None
        job_metrics_node = None

        job_models = [job_target_graph.root_id]
        # job_outputs = [view_id for view_id in output_views]
        # job_save_ops = [save_id for save_id in save_physical_outputs]

        # Build the top level job node

        job_node_id = NodeId("trac_job_completion", job_namespace)
        job_node = JobNode(job_node_id, job_metadata_id, explicit_deps=job_models)

        job_graph_nodes = {
            **output_graph.nodes,
            job_metadata_id: job_metadata_node,
            job_node_id: job_node}

        job_graph = Graph(job_graph_nodes, job_node_id)

        # CTX pop happens last, after the job node is complete

        job_ctx_pop = GraphBuilder.build_context_pop(
            job_namespace, job_graph, dict())

        return job_ctx_pop

    @classmethod
    def build_job_inputs(
            cls, job_config: config.JobConfig, namespace: NodeNamespace,
            graph: Graph) -> Graph:

        nodes = {**graph.nodes}

        for input_name, data_id in job_config.inputs.items():

            data_def = job_config.objects[data_id].data

            # TODO: Real lookup for selectors
            storage_def = job_config.objects[data_def.storageId.objectId].storage

            # TODO: Get this from somewhere
            root_part_opaque_key = 'part-root'
            data_item = data_def.parts[root_part_opaque_key].snap.deltas[0].dataItem

            # Physical load of data items from disk
            # Currently one item per input, since inputs are single part/delta
            data_load_id = NodeId(f"{data_item}:LOAD", namespace)
            data_load_node = LoadDataNode(data_load_id, data_item, data_def, storage_def, explicit_deps=[graph.root_id])

            # Input items mapped directly from their load operations
            data_item_id = NodeId(data_item, namespace)
            data_item_node = IdentityNode(data_item_id, data_load_id)

            # Inputs views assembled by mapping one root part to each view
            data_view_id = NodeId(input_name, namespace)
            data_view_node = DataViewNode(data_view_id, data_def.schema, data_item_id)

            nodes[data_load_id] = data_load_node
            nodes[data_item_id] = data_item_node
            nodes[data_view_id] = data_view_node

        return Graph(nodes, graph.root_id)

    @classmethod
    def build_job_outputs(
            cls, job_config: config.JobConfig, namespace: NodeNamespace,
            graph: Graph) -> Graph:

        nodes = {**graph.nodes}

        for output_name, data_id in job_config.outputs.items():

            data_def = job_config.objects[data_id].data

            # TODO: Real lookup for selectors
            storage_def = job_config.objects[data_def.storageId.objectId].storage

            # TODO: Get this from somewhere
            root_part_opaque_key = 'part-root'
            data_item = data_def.parts[root_part_opaque_key].snap.deltas[0].dataItem

            # Output data view must already exist in the namespace
            data_view_id = NodeId(output_name, namespace)

            # Map one data item from each view, since outputs are single part/delta
            data_item_id = NodeId(data_item, namespace)
            data_item_node = DataItemNode(data_item_id, data_view_id, data_item)

            # Create a physical save operation for the data item
            data_save_id = NodeId(f"{data_item}:SAVE", namespace)
            data_save_node = SaveDataNode(data_save_id, data_item_id, data_def, storage_def)  # TODO: deps for save node

            # Create an output metadata node
            # Output metadata is associate with the job-level output (i.e. the data view)
            # It references all the connected physical save operations, currently there is just one for part-root
            output_meta_id = NodeId(f"{output_name}:METADATA", namespace)
            output_meta_node = JobOutputMetadataNode(output_meta_id, data_view_id, {data_save_id: data_item})

            nodes[data_item_id] = data_item_node
            nodes[data_save_id] = data_save_node
            nodes[output_meta_id] = output_meta_node

        return Graph(nodes, graph.root_id)

    @staticmethod
    def build_model_or_flow_with_context(
            job_config: config.JobConfig,
            namespace: NodeNamespace, graph: Graph,
            model_or_flow: meta.ObjectDefinition,
            input_mapping: tp.Dict[str, NodeId],
            output_mapping: tp.Dict[str, NodeId]) -> Graph:

        # Generate a name for a new unique sub-context
        model_or_flow_name = "trac_model"  # TODO: unique name
        sub_namespace_name = f"{model_or_flow.objectType} = {model_or_flow_name}"
        sub_namespace = NodeNamespace(sub_namespace_name, namespace)

        # Execute in the sub-context by doing PUSH, EXEC, POP
        push_graph = GraphBuilder.build_context_push(sub_namespace, graph, input_mapping)
        exec_graph = GraphBuilder.build_model_or_flow(job_config, sub_namespace, push_graph, model_or_flow)
        pop_graph = GraphBuilder.build_context_pop(sub_namespace, exec_graph, output_mapping)

        return pop_graph

    @staticmethod
    def build_model_or_flow(
            job_config: config.JobConfig,
            namespace: NodeNamespace, graph: Graph,
            model_or_flow: meta.ObjectDefinition) -> Graph:

        if model_or_flow.objectType == meta.ObjectType.MODEL:
            return GraphBuilder.build_model(job_config, namespace, graph, model_or_flow.model)

        elif model_or_flow.objectType == meta.ObjectType.FLOW:
            return GraphBuilder.build_flow(job_config, namespace, graph, model_or_flow.flow)

        else:
            raise _ex.EConfigParse("Invalid job config given to the execution engine")

    @staticmethod
    def build_model(
            job_config: config.JobConfig,
            namespace: NodeNamespace, graph: Graph,
            model_def: meta.ModelDefinition) -> Graph:

        def node_id_for(node_name):
            return NodeId(node_name, namespace)

        # Input data should already be mapped to named inputs in the model context
        input_ids = frozenset(map(node_id_for, model_def.inputs.keys()))

        # Create the model node
        # Always add the prior graph root ID as a dependency
        # This is to ensure dependencies are still pulled in for models with no inputs!

        model_name = model_def.entryPoint.split(".")[-1]  # TODO: Check unique model name
        model_id = node_id_for(model_name)
        model_node = ModelNode(model_id, model_def, input_ids, explicit_deps=[graph.root_id])

        # Create nodes for each model output
        # The model node itself outputs a bundle (dictionary of named outputs)
        # These need to be mapped to individual nodes in the graph

        # These output mapping nodes are closely tied to the representation of the model itself
        # In the future, we may want models to emit individual outputs before the whole model is complete

        model_node_map = {model_id: model_node}

        for output_name in model_def.outputs:

            output_id = NodeId(output_name, namespace)
            output_node = KeyedItemNode(output_id, model_id, output_name)

            model_node_map[output_id] = output_node

        # Assemble a graph to include the model and its outputs
        return Graph({**graph.nodes, **model_node_map}, model_id)

    @staticmethod
    def build_flow(
            job_config: config.JobConfig,
            namespace: NodeNamespace, graph: Graph,
            flow_def: meta.FlowDefinition) -> Graph:

        raise NotImplementedError("Runtime execution graph for multi-model flows not implemented yet")

    @staticmethod
    def build_context_push(
            namespace: NodeNamespace, graph: Graph,
            input_mapping: tp.Dict[str, NodeId]) -> Graph:

        """
        Create a context push operation, all inputs are mapped by name
        """

        def node_id_for(input_name):
            return NodeId(input_name, namespace)

        push_mapping = {
            node_id_for(input_name): input_id
            for input_name, input_id
            in input_mapping.items()}

        push_id = NodeId("trac_ctx_push", namespace)
        push_node = ContextPushNode(push_id, namespace, push_mapping)

        # Create an explicit marker for each data node pushed into the new context
        marker_nodes = {
            node_id: IdentityNode(node_id, proxy_for=push_id)
            for node_id in push_mapping.keys()}

        return Graph({
            **graph.nodes, **marker_nodes,
            push_id: push_node}, push_id)

    @staticmethod
    def build_context_pop(
            namespace: NodeNamespace, graph: Graph,
            output_mapping: tp.Dict[str, NodeId]) -> Graph:

        """
        Create a context pop operation, all outputs are mapped by name
        """

        def node_id_for(input_name):
            return NodeId(input_name, namespace)

        pop_mapping = {
            node_id_for(output_name): output_id
            for output_name, output_id
            in output_mapping.items()}

        pop_id = NodeId("trac_ctx_pop", namespace)
        pop_node = ContextPopNode(pop_id, namespace, pop_mapping, explicit_deps=[graph.root_id])

        # Create an explicit marker for each data node popped into the outer context
        marker_nodes = {
            node_id: IdentityNode(node_id, proxy_for=pop_id)
            for node_id in pop_mapping.values()}

        return Graph({
            **graph.nodes, **marker_nodes,
            pop_id: pop_node}, pop_id)
