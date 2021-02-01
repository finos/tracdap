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

import trac.rt.metadata as meta
import trac.rt.config.config as config
from .graph import *


class GraphBuilder:

    @staticmethod
    def build_job(job_config: config.JobConfig) -> Graph:

        target_def = job_config.objects.get(job_config.target)

        if target_def is None:
            raise RuntimeError(f"No definition available for job target '{job_config.target}'")  # TODO: Error

        # Only calculation jobs are supported at present
        return GraphBuilder.build_calculation_job(job_config)

    @staticmethod
    def build_calculation_job(job_config: config.JobConfig) -> Graph:

        # Create a job context with no dependencies and no external data mappings
        # All input data will be loaded inside the job context

        null_graph = Graph({}, NodeId('', NodeNamespace('')))
        job_namespace = NodeNamespace(f"job={job_config.job_id}")

        job_ctx_push = GraphBuilder.build_context_push(
            job_namespace, null_graph, dict())

        # Create load operations to load data into the job context once it is created

        # TODO

        # Now create the root execution node, which will be either a single model or a flow
        # The root exec node can run directly in the job context, no need to do a context push
        # All inputs/outputs are already set up and there is only a single execution target

        exec_target = job_config.objects.get(job_config.target)
        exec_graph = GraphBuilder.build_model_or_flow(
            job_config, job_namespace, job_ctx_push, exec_target)

        # Create save operations, data will be saved directly from the job context

        # TODO

        # Create metadata and oversight nodes

        # TODO

        # Create a top level job node which will act as the graph root
        # Do not pop any data back into the global context
        # For the time being, data is loaded and saved independently for each job

        job_ctx_pop = GraphBuilder.build_context_pop(
            job_namespace, exec_graph, dict())

        job_node_id = NodeId("trac_job_marker", job_namespace)
        job_node = JobNode(job_node_id, job_ctx_pop.root_id)

        return Graph({**job_ctx_pop.nodes, job_node_id: job_node}, job_node_id)

    @staticmethod
    def build_data_load():
        pass

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
            raise RuntimeError("Invalid job config given to the execution engine")  # TODO: Error

    @staticmethod
    def build_model(
            job_config: config.JobConfig,
            namespace: NodeNamespace, graph: Graph,
            model_def: meta.ModelDefinition) -> Graph:

        def node_id_for(node_name):
            return NodeId(node_name, namespace)

        model_id = node_id_for('trac_model_exec')  # TODO: Model name

        # Input data should already be mapped to named inputs in the model context
        input_ids = frozenset(map(node_id_for, model_def.input.keys()))
        ctx_push_id = graph.root_id

        model_node = ModelNode(model_id, model_def, input_ids, explicit_deps=[ctx_push_id])

        return Graph({**graph.nodes, model_id: model_node}, model_id)

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
