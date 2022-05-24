#  Copyright 2022 Accenture Global Solutions Limited
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

import copy

import tracdap.rt.config as config
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.data as _data
import tracdap.rt.impl.util as _util

from .graph import *


class GraphBuilder:

    @staticmethod
    def build_job(
            job_config: config.JobConfig,
            result_spec: JobResultSpec) -> Graph:

        if job_config.job.jobType == meta.JobType.IMPORT_MODEL:
            return GraphBuilder.build_import_model_job(job_config, result_spec)

        if job_config.job.jobType == meta.JobType.RUN_MODEL:
            return GraphBuilder.build_run_model_job(job_config, result_spec)

        raise _ex.EConfigParse(f"Job type [{job_config.job.jobType}] is not supported yet")

    @classmethod
    def build_import_model_job(cls, job_config: config.JobConfig, result_spec: JobResultSpec) -> Graph:

        job_namespace = NodeNamespace(_util.object_key(job_config.jobId))
        null_graph = Graph({}, NodeId('', job_namespace))

        # Create a job context with no dependencies and no external data mappings
        ctx_push_graph = GraphBuilder.build_context_push(
            job_namespace, null_graph, input_mapping=dict())

        # TODO: Import model job should pre-allocate an ID, then model ID comes from job_config.resultMapping
        new_model_id = _util.new_object_id(meta.ObjectType.MODEL)

        model_scope = _util.object_key(job_config.jobId)
        import_details = job_config.job.importModel

        import_id = NodeId.of("trac_import_model", job_namespace, meta.ModelDefinition)
        import_node = ImportModelNode(import_id, model_scope, import_details, explicit_deps=[ctx_push_graph.root_id])

        import_result_id = NodeId.of("trac_import_result", job_namespace, ObjectMap)
        import_result_node = ImportModelResultNode(import_result_id, import_id, new_model_id)

        build_result_id = NodeId.of("trac_build_result", job_namespace, cfg.JobResult)
        build_result_node = BuildJobResultNode(
            build_result_id, job_config.jobId,
            [import_result_id], explicit_deps=[import_id])

        save_result_id = NodeId("trac_save_result", job_namespace)
        save_result_node = SaveJobResultNode(save_result_id, build_result_id, result_spec)

        job_node_id = NodeId("trac_job", job_namespace)
        job_result_id = save_result_id if result_spec.save_result else build_result_id
        job_node = JobNode(job_node_id, import_id, job_result_id)

        job_graph_nodes = {
            **ctx_push_graph.nodes,
            import_id: import_node,
            import_result_id: import_result_node,
            build_result_id: build_result_node,
            job_node_id: job_node}

        if result_spec.save_result:
            job_graph_nodes[save_result_id] = save_result_node

        job_graph = Graph(job_graph_nodes, job_node_id)

        job_ctx_pop = GraphBuilder.build_context_pop(
            job_namespace, job_graph, dict())

        return job_ctx_pop

    @classmethod
    def build_run_model_job(
            cls, job_config: config.JobConfig,
            result_spec: JobResultSpec) -> Graph:

        return cls.build_calculation_job(
            job_config, result_spec,
            job_config.job.runModel.model,
            job_config.job.runModel.parameters,
            job_config.job.runModel.inputs,
            job_config.job.runModel.outputs)

    @classmethod
    def build_calculation_job(
            cls, job_config: config.JobConfig,
            result_spec: JobResultSpec,
            target: meta.TagSelector, parameters: tp.Dict[str, meta.Value],
            inputs: tp.Dict[str, meta.TagSelector], outputs: tp.Dict[str, meta.TagSelector]) -> Graph:

        job_namespace = NodeNamespace(_util.object_key(job_config.jobId))
        null_graph = Graph({}, NodeId('', job_namespace))

        # Create a job context with no dependencies and no external data mappings
        ctx_push_graph = GraphBuilder.build_context_push(
            job_namespace, null_graph, input_mapping=dict())

        params_graph = cls.build_job_parameters(parameters, job_namespace, ctx_push_graph)

        # Input graph will prepare data views job inputs
        input_graph = cls.build_job_inputs(job_config, inputs, job_namespace, params_graph)

        # Now create the root execution node, which will be either a single model or a flow
        # The root exec node can run directly in the job context, no need to do a context push
        # All input views are already mapped and there is only a single execution target

        target_obj = _util.get_job_resource(target, job_config)
        target_graph = GraphBuilder.build_model_or_flow(
            job_config, job_namespace, input_graph, target_obj)

        # Output graph will extract and save data items from job-level output data views

        output_graph = cls.build_job_outputs(job_config, outputs, job_namespace, target_graph)

        # Build job-level metadata outputs

        data_result_ids = list(
            nid for nid, n in output_graph.nodes.items()
            if isinstance(n, DataResultNode))

        build_result_id = NodeId.of("trac_build_result", job_namespace, cfg.JobResult)
        build_result_node = BuildJobResultNode(
            build_result_id, job_config.jobId,
            data_result_ids, explicit_deps=[target_graph.root_id])

        save_result_id = NodeId("trac_save_result", job_namespace)
        save_result_node = SaveJobResultNode(save_result_id, build_result_id, result_spec)

        # Build the top level job node

        job_node_id = NodeId("trac_job", job_namespace)
        job_result_id = save_result_id if result_spec.save_result else build_result_id
        job_node = JobNode(job_node_id, target_graph.root_id, job_result_id)

        job_graph_nodes = {
            **output_graph.nodes,
            build_result_id: build_result_node,
            job_node_id: job_node}

        if result_spec.save_result:
            job_graph_nodes[save_result_id] = save_result_node

        job_graph = Graph(job_graph_nodes, job_node_id)

        # CTX pop happens last, after the job node is complete

        job_ctx_pop = GraphBuilder.build_context_pop(
            job_namespace, job_graph, dict())

        return job_ctx_pop

    @classmethod
    def build_job_parameters(
            cls, parameters: tp.Dict[str, meta.Value],
            namespace: NodeNamespace, graph: Graph) -> Graph:

        nodes = {**graph.nodes}

        job_params_node_id = NodeId("trac_job_params", namespace)
        job_params_node = SetParametersNode(job_params_node_id, parameters, explicit_deps=[graph.root_id])

        nodes[job_params_node_id] = job_params_node

        for param_name in parameters:

            param_node_id = NodeId(param_name, namespace)
            param_node = KeyedItemNode(param_node_id, job_params_node_id, param_name)

            nodes[param_node_id] = param_node

        return Graph(nodes, graph.root_id)

    @classmethod
    def build_job_inputs(
            cls, job_config: config.JobConfig, inputs: tp.Dict[str, meta.TagSelector],
            namespace: NodeNamespace, graph: Graph) -> Graph:

        nodes = copy.copy(graph.nodes)

        for input_name, data_selector in inputs.items():

            # Build a data spec using metadata from the job config
            # For now we are always loading the root part, snap 0, delta 0
            data_def = _util.get_job_resource(data_selector, job_config).data
            storage_def = _util.get_job_resource(data_def.storageId, job_config).storage

            root_part_opaque_key = 'part-root'  # TODO: Central part names / constants
            data_item = data_def.parts[root_part_opaque_key].snap.deltas[0].dataItem
            data_spec = _data.DataItemSpec(data_item, data_def, storage_def, schema_def=None)

            # Data spec node is static, using the assembled data spec
            data_spec_id = NodeId.of(f"{input_name}:SPEC", namespace, _data.DataItemSpec)
            data_spec_node = StaticDataSpecNode(data_spec_id, data_spec, explicit_deps=[graph.root_id])

            # Physical load of data items from disk
            # Currently one item per input, since inputs are single part/delta
            data_load_id = NodeId.of(f"{input_name}:LOAD", namespace, _data.DataItem)
            data_load_node = LoadDataNode(data_load_id, data_spec_id, explicit_deps=[graph.root_id])

            # Input views assembled by mapping one root part to each view
            data_view_id = NodeId.of(input_name, namespace, _data.DataView)
            data_view_node = DataViewNode(data_view_id, data_def.schema, data_load_id)

            nodes[data_spec_id] = data_spec_node
            nodes[data_load_id] = data_load_node
            nodes[data_view_id] = data_view_node

        return Graph(nodes, graph.root_id)

    @classmethod
    def build_job_outputs(
            cls, job_config: config.JobConfig,
            outputs: tp.Dict[str, meta.TagSelector],
            namespace: NodeNamespace, graph: Graph) -> Graph:

        nodes = copy.copy(graph.nodes)

        for output_name, data_selector in outputs.items():

            # Output data view must already exist in the namespace
            data_view_id = NodeId.of(output_name, namespace, _data.DataView)
            data_spec_id = NodeId.of(f"{output_name}:SPEC", namespace, _data.DataItemSpec)

            data_obj = _util.get_job_resource(data_selector, job_config, optional=True)

            if data_obj is not None:

                data_def = data_obj.data
                storage_obj = _util.get_job_resource(data_def.storageId, job_config)
                storage_def = storage_obj.storage

                root_part_opaque_key = 'part-root'  # TODO: Central part names / constants
                data_item = data_def.parts[root_part_opaque_key].snap.deltas[0].dataItem
                data_spec = _data.DataItemSpec(data_item, data_def, storage_def, schema_def=None)

                data_spec_node = StaticDataSpecNode(data_spec_id, data_spec, explicit_deps=[data_view_id])

                output_data_key = output_name + ":DATA"
                output_storage_key = output_name + ":STORAGE"

            else:

                data_key = output_name + ":DATA"
                data_id = job_config.resultMapping[data_key]
                storage_key = output_name + ":STORAGE"
                storage_id = job_config.resultMapping[storage_key]

                data_spec_node = DynamicDataSpecNode(
                        data_spec_id, data_view_id,
                        data_id, storage_id,
                        prior_data_spec=None)

                output_data_key = _util.object_key(data_id)
                output_storage_key = _util.object_key(storage_id)

            # Map one data item from each view, since outputs are single part/delta
            data_item_id = NodeId(f"{output_name}:ITEM", namespace)
            data_item_node = DataItemNode(data_item_id, data_view_id)

            # Create a physical save operation for the data item
            data_save_id = NodeId.of(f"{output_name}:SAVE", namespace, None)
            data_save_node = SaveDataNode(data_save_id, data_spec_id, data_item_id)

            data_result_id = NodeId.of(f"{output_name}:RESULT", namespace, ObjectMap)
            data_result_node = DataResultNode(
                data_result_id, output_name, data_spec_id, data_save_id,
                output_data_key, output_storage_key)

            nodes[data_spec_id] = data_spec_node
            nodes[data_item_id] = data_item_node
            nodes[data_save_id] = data_save_node
            nodes[data_result_id] = data_result_node

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
        parameter_ids = frozenset(map(node_id_for, model_def.parameters.keys()))
        input_ids = frozenset(map(node_id_for, model_def.inputs.keys()))

        # Create the model node
        # Always add the prior graph root ID as a dependency
        # This is to ensure dependencies are still pulled in for models with no inputs!

        model_scope = _util.object_key(job_config.jobId)
        model_name = model_def.entryPoint.split(".")[-1]  # TODO: Check unique model name
        model_id = node_id_for(model_name)

        model_node = RunModelNode(
            model_id, model_scope, model_def,
            parameter_ids, input_ids,
            explicit_deps=[graph.root_id])

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
