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

        job_def = job_config.objects.get(job_config.target)

        if job_def is None:
            raise RuntimeError(f"No job definition available for job ID '{job_config.target}'")  # TODO: Error

        if job_def.objectType != meta.ObjectType.JOB:
            raise RuntimeError(f"Invalid definition for job ID '{job_config.target}'")  # TODO: Error

        # Only calculation jobs are supported at present
        return GraphBuilder.build_calculation_job(job_config, job_def.job, job_config.objects)

    @staticmethod
    def build_calculation_job(
            job_config: config.JobConfig, job_def: meta.JobDefinition,
            metadata: tp.Dict[meta.TagSelector, meta.ObjectDefinition]) -> Graph:

        # Create a job context with no dependencies and no external data mappings
        # All input data will be loaded inside the job context

        null_graph = Graph({}, NodeId(NodeContext(''), ''))
        job_ctx = NodeContext(f"job={job_config.job_id}")
        job_ctx_push = GraphBuilder.build_context_push(job_ctx, null_graph, dict())

        # Create load operations to load data into the job context once it is created

        # TODO

        # Now create the root execution node, which will be either a single model or a flow
        # The root exec node can run directly in the job context, no need to do a context push
        # All inputs/outputs are already set up and there is only a single execution target

        exec_target = job_config.objects.get(job_config.target)
        exec_graph = GraphBuilder.build_model_or_flow(
            job_config, job_ctx, job_ctx_push, exec_target)

        # Create save operations, data will be saved directly from the job context

        # TODO

        # Create metadata and oversight nodes

        # TODO

        # Create a top level job node which will act as the graph root
        # Do not pop any data back into the global context
        # For the time being, data is loaded and saved independently for each job

        job_ctx_pop = GraphBuilder.build_context_pop(job_ctx, exec_graph, dict())

        job_node_id = NodeId(job_ctx, "trac_job_marker")
        job_node = JobNode(job_node_id, job_def, job_ctx_pop.root_id)

        return Graph({**job_ctx_pop.nodes, job_node_id: job_node}, job_node_id)

    @staticmethod
    def build_data_load():
        pass

    @staticmethod
    def build_model_or_flow_with_context(
            job_config: config.JobConfig,
            context: NodeContext, graph: Graph,
            model_or_flow: meta.ObjectDefinition,
            input_mapping: tp.Dict[str, NodeId],
            output_mapping: tp.Dict[str, NodeId]) -> Graph:

        # Generate a name for a new unique sub-context
        model_or_flow_name = "trac_model"  # TODO: unique name
        sub_ctx_namespace = f"{model_or_flow.objectType} = {model_or_flow_name}"
        sub_ctx = NodeContext(sub_ctx_namespace, context)

        # Execute in the sub-context by doing PUSH, EXEC, POP
        push_graph = GraphBuilder.build_context_push(sub_ctx, graph, input_mapping)
        exec_graph = GraphBuilder.build_model_or_flow(job_config, sub_ctx, push_graph, model_or_flow)
        pop_graph = GraphBuilder.build_context_pop(sub_ctx, exec_graph, output_mapping)

        return pop_graph

    @staticmethod
    def build_model_or_flow(
            job_config: config.JobConfig,
            context: NodeContext, graph: Graph,
            model_or_flow: meta.ObjectDefinition) -> Graph:

        if model_or_flow.objectType == meta.ObjectType.MODEL:
            return GraphBuilder.build_model(job_config, context, graph, model_or_flow.model)

        elif model_or_flow.objectType == meta.ObjectType.FLOW:
            return GraphBuilder.build_flow(job_config, context, graph, model_or_flow.flow)

        else:
            raise RuntimeError("Invalid job config given to the execution engine")  # TODO: Error

    @staticmethod
    def build_model(
            job_config: config.JobConfig,
            context: NodeContext, graph: Graph,
            model_def: meta.ModelDefinition) -> Graph:

        def node_id_for(node_name):
            return NodeId(context, node_name)

        model_id = node_id_for('trac_model_exec')  # TODO: Model name

        # Input data should already be mapped to named inputs in the model context
        input_ids = frozenset(map(node_id_for, model_def.input.keys()))

        model_node = ModelNode(model_id, model_def, input_ids)

        return Graph({**graph.nodes, model_id: model_node}, model_id)

    @staticmethod
    def build_flow(
            job_config: config.JobConfig,
            context: NodeContext, graph: Graph,
            flow_def: meta.FlowDefinition) -> Graph:

        raise NotImplementedError("Runtime execution graph for multi-model flows not implemented yet")

    @staticmethod
    def build_context_push(
            context: NodeContext, graph: Graph,
            input_mapping: tp.Dict[str, NodeId]) -> Graph:

        """
        Create a context push operation, all inputs are mapped by name
        """

        def node_id_for(input_name):
            return NodeId(context, input_name)

        ctx_push_mapping = {
            node_id_for(input_name): input_id
            for input_name, input_id
            in input_mapping.items()}

        ctx_push_id = NodeId(context, "trac_ctx_push")
        ctx_push_node = ContextPushNode(ctx_push_id, ctx_push_mapping)

        # Create an explicit marker for each data node pushed into the new context
        marker_nodes = {
            node_id: IdentityNode(node_id, proxy_for=ctx_push_id)
            for node_id in ctx_push_mapping.keys()}

        return Graph({
            **graph.nodes, **marker_nodes,
            ctx_push_id: ctx_push_node}, ctx_push_id)

    @staticmethod
    def build_context_pop(
            context: NodeContext, graph: Graph,
            output_mapping: tp.Dict[str, NodeId]) -> Graph:

        """
        Create a context pop operation, all outputs are mapped by name
        """

        def node_id_for(input_name):
            return NodeId(context, input_name)

        ctx_pop_mapping = {
            node_id_for(output_name): output_id
            for output_name, output_id
            in output_mapping.items()}

        ctx_pop_id = NodeId(context, "trac_ctx_pop")
        ctx_pop_node = ContextPopNode(ctx_pop_id, ctx_pop_mapping)

        # Create an explicit marker for each data node popped into the outer context
        marker_nodes = {
            node_id: IdentityNode(node_id, proxy_for=ctx_pop_id)
            for node_id in ctx_pop_mapping.values()}

        return Graph({
            **graph.nodes, **marker_nodes,
            ctx_pop_id: ctx_pop_node}, ctx_pop_id)
