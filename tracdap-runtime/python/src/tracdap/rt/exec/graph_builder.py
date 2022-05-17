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

from __future__ import annotations

import copy

import tracdap.rt.config as config
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.data as _data
import tracdap.rt.impl.util as _util

from .graph import *


class GraphBuilder:

    __JOB_BUILD_FUNC = tp.Callable[
        [config.JobConfig, JobResultSpec, NodeNamespace, NodeId],
        GraphSection]

    @classmethod
    def build_job(
            cls, job_config: config.JobConfig,
            result_spec: JobResultSpec) -> Graph:

        if job_config.job.jobType == meta.JobType.IMPORT_MODEL:
            return cls.build_standard_job(job_config, result_spec, cls.build_import_model_job)

        if job_config.job.jobType == meta.JobType.RUN_MODEL:
            return cls.build_standard_job(job_config, result_spec, cls.build_run_model_job)

        if job_config.job.jobType == meta.JobType.RUN_FLOW:
            return cls.build_standard_job(job_config, result_spec, cls.build_run_flow_job)

        raise _ex.EConfigParse(f"Job type [{job_config.job.jobType}] is not supported yet")

    @classmethod
    def build_standard_job(
            cls, job_config: config.JobConfig, result_spec: JobResultSpec,
            build_func: __JOB_BUILD_FUNC):

        # Set up the job context

        job_namespace = NodeNamespace(_util.object_key(job_config.jobId))

        job_push_id = NodeId("trac_job_push", job_namespace)
        job_push_node = ContextPushNode(job_push_id, job_namespace, mapping={})
        job_push_section = GraphSection({job_push_id: job_push_node}, inputs=set(), must_run=[job_push_id])

        # Build the execution graphs for the main job and results recording

        main_section = build_func(job_config, result_spec, job_namespace, job_push_id)

        # Clean up the job context

        job_pop_id = NodeId("trac_job_pop", job_namespace)
        job_pop_node = ContextPopNode(job_pop_id, job_namespace, mapping={}, explicit_deps=main_section.must_run)
        job_pop_section = GraphSection({job_pop_id: job_pop_node}, inputs=set(), must_run=[job_pop_id])

        job = cls._join_sections(job_push_section, main_section, job_pop_section)

        return Graph(job.nodes, job_pop_id)

    @classmethod
    def build_import_model_job(
            cls, job_config: config.JobConfig, result_spec: JobResultSpec,
            job_namespace: NodeNamespace, job_push_id: NodeId) \
            -> (GraphSection, GraphSection):

        # Main section: run the model import

        # TODO: Import model job should pre-allocate an ID, then model ID comes from job_config.resultMapping
        new_model_id = _util.new_object_id(meta.ObjectType.MODEL)
        new_model_key = _util.object_key(new_model_id)

        model_scope = _util.object_key(job_config.jobId)
        import_details = job_config.job.importModel

        import_id = NodeId.of("trac_import_model", job_namespace, meta.ObjectDefinition)
        import_node = ImportModelNode(import_id, model_scope, import_details, explicit_deps=[job_push_id])

        main_section = GraphSection(nodes={import_id: import_node}, must_run=[import_id])

        # Build job-level metadata outputs

        result_objects = {new_model_key: import_id}

        result_section = cls.build_job_results(
            job_config, job_namespace,
            result_spec, objects=result_objects,
            explicit_deps=[job_push_id, *main_section.must_run])

        return cls._join_sections(main_section, result_section)

    @classmethod
    def build_run_model_job(
            cls, job_config: config.JobConfig, result_spec: JobResultSpec,
            job_namespace: NodeNamespace, job_push_id: NodeId) \
            -> (GraphSection, GraphSection):

        return cls.build_calculation_job(
            job_config, result_spec, job_namespace, job_push_id,
            job_config.job.runModel.model,
            job_config.job.runModel.parameters,
            job_config.job.runModel.inputs,
            job_config.job.runModel.outputs)

    @classmethod
    def build_run_flow_job(
            cls, job_config: config.JobConfig, result_spec: JobResultSpec,
            job_namespace: NodeNamespace, job_push_id: NodeId) \
            -> (GraphSection, GraphSection):

        return cls.build_calculation_job(
            job_config, result_spec, job_namespace, job_push_id,
            job_config.job.runFlow.flow,
            job_config.job.runFlow.parameters,
            job_config.job.runFlow.inputs,
            job_config.job.runFlow.outputs)

    @classmethod
    def build_calculation_job(
            cls, job_config: config.JobConfig, result_spec: JobResultSpec,
            job_namespace: NodeNamespace, job_push_id: NodeId,
            target: meta.TagSelector, parameters: tp.Dict[str, meta.Value],
            inputs: tp.Dict[str, meta.TagSelector], outputs: tp.Dict[str, meta.TagSelector]) \
            -> GraphSection:

        # The main execution graph can run directly in the job context, no need to do a context push
        # since inputs and outputs in this context line up with the top level execution task

        params_section = cls.build_job_parameters(
            job_namespace, parameters,
            explicit_deps=[job_push_id])

        input_section = cls.build_job_inputs(
            job_config, job_namespace, inputs,
            explicit_deps=[job_push_id, *params_section.must_run])

        exec_obj = _util.get_job_resource(target, job_config)

        exec_section = cls.build_model_or_flow(
            job_config, job_namespace, exec_obj,
            explicit_deps=[job_push_id, *params_section.must_run, *input_section.must_run])

        output_section = cls.build_job_outputs(
            job_config, job_namespace, outputs,
            explicit_deps=[job_push_id, *exec_section.must_run])

        main_section = cls._join_sections(params_section, input_section, exec_section, output_section)

        # Build job-level metadata outputs

        data_result_ids = list(
            nid for nid, n in main_section.nodes.items()
            if isinstance(n, DataResultNode))

        result_section = cls.build_job_results(
            job_config, job_namespace,
            result_spec, bundles=data_result_ids,
            explicit_deps=[job_push_id, *main_section.must_run])

        return cls._join_sections(main_section, result_section)

    @classmethod
    def build_job_parameters(
            cls, job_namespace: NodeNamespace,
            parameters: tp.Dict[str, meta.Value],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        nodes = dict()

        for param_name, param_def in parameters.items():

            param_id = NodeId(param_name, job_namespace, meta.Value)
            param_node = StaticValueNode(param_id, param_def, explicit_deps=explicit_deps)

            nodes[param_id] = param_node

        return GraphSection(nodes, outputs=set(nodes.keys()), must_run=list(nodes.keys()))

    @classmethod
    def build_job_inputs(
            cls, job_config: config.JobConfig, job_namespace: NodeNamespace,
            inputs: tp.Dict[str, meta.TagSelector],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        nodes = dict()
        outputs = set()
        must_run = list()

        for input_name, data_selector in inputs.items():

            # Build a data spec using metadata from the job config
            # For now we are always loading the root part, snap 0, delta 0
            data_def = _util.get_job_resource(data_selector, job_config).data
            storage_def = _util.get_job_resource(data_def.storageId, job_config).storage

            root_part_opaque_key = 'part-root'  # TODO: Central part names / constants
            data_item = data_def.parts[root_part_opaque_key].snap.deltas[0].dataItem
            data_spec = _data.DataSpec(data_item, data_def, storage_def, schema_def=None)

            # Data spec node is static, using the assembled data spec
            data_spec_id = NodeId.of(f"{input_name}:SPEC", job_namespace, _data.DataSpec)
            data_spec_node = StaticValueNode(data_spec_id, data_spec, explicit_deps=explicit_deps)

            # Physical load of data items from disk
            # Currently one item per input, since inputs are single part/delta
            data_load_id = NodeId.of(f"{input_name}:LOAD", job_namespace, _data.DataItem)
            data_load_node = LoadDataNode(data_load_id, data_spec_id, explicit_deps=explicit_deps)

            # Input views assembled by mapping one root part to each view
            data_view_id = NodeId.of(input_name, job_namespace, _data.DataView)
            data_view_node = DataViewNode(data_view_id, data_def.schema, data_load_id)

            nodes[data_spec_id] = data_spec_node
            nodes[data_load_id] = data_load_node
            nodes[data_view_id] = data_view_node

            # Job-level data view is an output of the load operation
            outputs.add(data_view_id)
            must_run.append(data_spec_id)

        return GraphSection(nodes, outputs=outputs, must_run=must_run)

    @classmethod
    def build_job_outputs(
            cls, job_config: config.JobConfig, job_namespace: NodeNamespace,
            outputs: tp.Dict[str, meta.TagSelector],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        nodes = {}
        inputs = set()

        for output_name, data_selector in outputs.items():

            # Output data view must already exist in the namespace
            data_view_id = NodeId.of(output_name, job_namespace, _data.DataView)
            data_spec_id = NodeId.of(f"{output_name}:SPEC", job_namespace, _data.DataSpec)

            data_obj = _util.get_job_resource(data_selector, job_config, optional=True)

            if data_obj is not None:

                data_def = data_obj.data
                storage_obj = _util.get_job_resource(data_def.storageId, job_config)
                storage_def = storage_obj.storage

                root_part_opaque_key = 'part-root'  # TODO: Central part names / constants
                data_item = data_def.parts[root_part_opaque_key].snap.deltas[0].dataItem
                data_spec = _data.DataSpec(data_item, data_def, storage_def, schema_def=None)

                data_spec_node = StaticValueNode(data_spec_id, data_spec, explicit_deps=explicit_deps)

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
                        prior_data_spec=None,
                        explicit_deps=explicit_deps)

                output_data_key = _util.object_key(data_id)
                output_storage_key = _util.object_key(storage_id)

            # Map one data item from each view, since outputs are single part/delta
            data_item_id = NodeId(f"{output_name}:ITEM", job_namespace, _data.DataItem)
            data_item_node = DataItemNode(data_item_id, data_view_id)

            # Create a physical save operation for the data item
            data_save_id = NodeId.of(f"{output_name}:SAVE", job_namespace, None)
            data_save_node = SaveDataNode(data_save_id, data_spec_id, data_item_id)

            data_result_id = NodeId.of(f"{output_name}:RESULT", job_namespace, ObjectBundle)
            data_result_node = DataResultNode(
                data_result_id, output_name, data_spec_id, data_save_id,
                output_data_key, output_storage_key)

            nodes[data_spec_id] = data_spec_node
            nodes[data_item_id] = data_item_node
            nodes[data_save_id] = data_save_node
            nodes[data_result_id] = data_result_node

            # Job-level data view is an input to the save operation
            inputs.add(data_view_id)

        return GraphSection(nodes, inputs=inputs)

    @classmethod
    def build_job_results(
            cls, job_config: cfg.JobConfig, job_namespace: NodeNamespace, result_spec: JobResultSpec,
            objects: tp.Dict[str, NodeId[meta.ObjectDefinition]] = None, bundles: tp.List[NodeId[ObjectBundle]] = None,
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        build_result_id = NodeId.of("trac_build_result", job_namespace, cfg.JobResult)

        if objects is not None:

            results_inputs = set(objects.values())

            build_result_node = BuildJobResultNode(
                build_result_id, job_config.jobId,
                objects=objects, explicit_deps=explicit_deps)

        elif bundles is not None:

            results_inputs = set(bundles)

            build_result_node = BuildJobResultNode(
                build_result_id, job_config.jobId,
                bundles=bundles, explicit_deps=explicit_deps)

        else:
            raise _ex.EUnexpected()

        save_result_id = NodeId("trac_save_result", job_namespace)
        save_result_node = SaveJobResultNode(save_result_id, build_result_id, result_spec)

        if result_spec.save_result:
            result_nodes = {build_result_id: build_result_node, save_result_id: save_result_node}
            job_result_id = save_result_id
        else:
            result_nodes = {build_result_id: build_result_node}
            job_result_id = build_result_id

        return GraphSection(result_nodes, inputs=results_inputs, must_run=[job_result_id])

    @classmethod
    def build_model_or_flow_with_context(
            cls, job_config: config.JobConfig, namespace: NodeNamespace,
            model_or_flow: meta.ObjectDefinition,
            input_mapping: tp.Dict[str, NodeId], output_mapping: tp.Dict[str, NodeId],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        # Generate a name for a new unique sub-context

        model_or_flow_name = "trac_model"  # TODO: unique name
        sub_namespace_name = f"{model_or_flow.objectType} = {model_or_flow_name}"
        sub_namespace = NodeNamespace(sub_namespace_name, namespace)

        # Execute in the sub-context by doing PUSH, EXEC, POP

        push_section = cls.build_context_push(
            sub_namespace, input_mapping,
            explicit_deps)

        exec_section = cls.build_model_or_flow(
            job_config, sub_namespace, model_or_flow,
            explicit_deps=push_section.must_run)

        pop_section = cls.build_context_pop(
            sub_namespace, output_mapping,
            explicit_deps=exec_section.must_run)

        return cls._join_sections(push_section, exec_section, pop_section)

    @classmethod
    def build_model_or_flow(
            cls, job_config: config.JobConfig, namespace: NodeNamespace,
            model_or_flow: meta.ObjectDefinition,
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        if model_or_flow.objectType == meta.ObjectType.MODEL:
            return cls.build_model(namespace, model_or_flow.model, explicit_deps)

        elif model_or_flow.objectType == meta.ObjectType.FLOW:
            return cls.build_flow(job_config, namespace, model_or_flow.flow)

        else:
            raise _ex.EConfigParse("Invalid job config given to the execution engine")

    @classmethod
    def build_model(
            cls, namespace: NodeNamespace,
            model_def: meta.ModelDefinition,
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        def param_id(node_name):
            return NodeId(node_name, namespace, meta.Value)

        def data_id(node_name):
            return NodeId(node_name, namespace, _data.DataView)

        # Input data should already be mapped to named inputs in the model context
        parameter_ids = set(map(param_id, model_def.parameters))
        input_ids = set(map(data_id, model_def.inputs))
        output_ids = set(map(data_id, model_def.outputs))

        # Create the model node
        # Always add the prior graph root ID as a dependency
        # This is to ensure dependencies are still pulled in for models with no inputs!

        model_scope = str(namespace)
        model_name = model_def.entryPoint.split(".")[-1]  # TODO: Check unique model name
        model_id = NodeId(model_name, namespace, Bundle[_data.DataView])

        model_node = RunModelNode(
            model_id, model_scope, model_def,
            frozenset(parameter_ids), frozenset(input_ids),
            explicit_deps=explicit_deps)

        nodes = {model_id: model_node}

        # Create nodes for each model output
        # The model node itself outputs a bundle (dictionary of named outputs)
        # These need to be mapped to individual nodes in the graph

        # These output mapping nodes are closely tied to the representation of the model itself
        # In the future, we may want models to emit individual outputs before the whole model is complete

        for output_id in output_ids:
            nodes[output_id] = KeyedItemNode(output_id, model_id, output_id.name)

        # Assemble a graph to include the model and its outputs
        return GraphSection(nodes, inputs={*parameter_ids, *input_ids}, outputs=output_ids, must_run=[model_id])

    @classmethod
    def build_flow(
            cls, job_config: config.JobConfig, namespace: NodeNamespace,
            flow_def: meta.FlowDefinition,
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        def socket_key(socket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        # https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm

        remaining_nodes = copy.copy(flow_def.nodes)
        remaining_edges_by_target = {edge.target.node: [] for edge in flow_def.edges}
        remaining_edges_by_source = {edge.source.node: [] for edge in flow_def.edges}

        for edge in flow_def.edges:
            remaining_edges_by_target[edge.target.node].append(edge)
            remaining_edges_by_source[edge.source.node].append(edge)

        # Initial set of reachable flow nodes is just the input nodes
        reachable_nodes = dict(filter(
            lambda kv: kv[1].nodeType == meta.FlowNodeType.INPUT_NODE,
            remaining_nodes.items()))

        target_edges = {socket_key(edge.target): edge for edge in flow_def.edges}

        # Initial graph section for the flow is empty
        graph_section = GraphSection({})

        while any(reachable_nodes):

            node_name, node = reachable_nodes.popitem()

            if node.nodeType == meta.FlowNodeType.INPUT_NODE:
                graph_section.inputs.add(NodeId(node_name, namespace))

            if node.nodeType == meta.FlowNodeType.OUTPUT_NODE:
                graph_section.outputs.add(NodeId(node_name, namespace))

            if node.nodeType == meta.FlowNodeType.MODEL_NODE:
                graph_section.must_run.append(NodeId(node_name, namespace))

            execution_nodes = cls.build_flow_node(
                job_config, namespace, target_edges,
                node_name, node)

            graph_section.nodes.update(execution_nodes)

            source_edges = remaining_edges_by_source.pop(node_name)

            for edge in source_edges:

                target_node_name = edge.target.node
                target_edges_ = remaining_edges_by_target[target_node_name]
                target_edges_.remove(edge)

                if len(target_edges_) == 0:
                    target_node = remaining_nodes.pop(target_node_name)
                    reachable_nodes[target_node_name] = target_node

        if any(remaining_nodes):
            raise _ex.ETracInternal()  # todo: cyclic / unmet dependencies

        return graph_section

    @classmethod
    def build_flow_node(
            cls, job_config: config.JobConfig, namespace: NodeNamespace,
            target_edges: tp.Dict[meta.FlowSocket, meta.FlowEdge],
            node_name: str, node: meta.FlowNode) \
            -> NodeMap:

        flow_job = job_config.job.runFlow

        def socket_key(socket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        def socket_id(node_: str, socket_: str = None):
            socket_name = f"{node_}.{socket_}" if socket_ else node_
            return NodeId(socket_name, namespace)

        def target_mapping(node_: str, socket_: str = None):
            socket = meta.FlowSocket(node_, socket_)
            edge = target_edges.get(socket_key(socket))  # todo: inconsistent if missing
            return socket_id(edge.source.node, edge.source.socket), socket_id(edge.target.node, edge.target.socket)

        if node.nodeType == meta.FlowNodeType.INPUT_NODE:
            return {}

        if node.nodeType == meta.FlowNodeType.OUTPUT_NODE:
            source_id, target_id = target_mapping(node_name)
            return {target_id: IdentityNode(target_id, source_id)}

        if node.nodeType == meta.FlowNodeType.MODEL_NODE:

            input_mappings_ = [target_mapping(node_name, input_name) for input_name in node.modelStub.inputs]

            mapping_nodes = {
                target_id: IdentityNode(target_id, source_id)
                for source_id, target_id in input_mappings_}

            model_namespace = NodeNamespace(f"MODEL = {node_name}", namespace)
            model_selector = flow_job.models.get(node_name)
            model_obj = _util.get_job_resource(model_selector, job_config)

            push_mapping = dict(
                (NodeId(input_, model_namespace), socket_id(node_name, input_))
                for input_ in model_obj.model.inputs)

            pop_mapping = dict(
                (NodeId(output_, model_namespace), socket_id(node_name, output_))
                for output_ in model_obj.model.outputs)

            sub_graph = cls.build_model_or_flow_with_context(
                job_config, namespace,
                model_obj, push_mapping, pop_mapping)

            return {**sub_graph.nodes, **mapping_nodes}

        raise _ex.ETracInternal()  # TODO: Invalid node type

    @staticmethod
    def build_context_push(
            namespace: NodeNamespace, input_mapping: tp.Dict[str, NodeId],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        """
        Create a context push operation, all inputs are mapped by name
        """

        def node_id_for(input_name, result_type):
            return NodeId(input_name, namespace, result_type)

        push_mapping = {
            node_id_for(input_name, input_id.result_type): input_id
            for input_name, input_id
            in input_mapping.items()}

        push_id = NodeId("trac_ctx_push", namespace)
        push_node = ContextPushNode(push_id, namespace, push_mapping, explicit_deps)

        nodes = {push_id: push_node}

        # Create an explicit marker for each data node pushed into the new context

        for node_id in push_mapping.keys():
            nodes[node_id] = IdentityNode(node_id, proxy_for=push_id)

        return GraphSection(
            nodes,
            inputs={*push_mapping.values()},
            outputs={*push_mapping.keys()},
            must_run=[push_id])

    @staticmethod
    def build_context_pop(
            namespace: NodeNamespace, output_mapping: tp.Dict[str, NodeId],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        """
        Create a context pop operation, all outputs are mapped by name
        """

        def node_id_for(input_name, result_type):
            return NodeId(input_name, namespace, result_type)

        pop_mapping = {
            node_id_for(output_name, output_id.result_type): output_id
            for output_name, output_id
            in output_mapping.items()}

        pop_id = NodeId("trac_ctx_pop", namespace)
        pop_node = ContextPopNode(pop_id, namespace, pop_mapping, explicit_deps)

        nodes = {pop_id: pop_node}

        # Create an explicit marker for each data node popped into the outer context

        for node_id in pop_mapping.values():
            nodes[node_id] = IdentityNode(node_id, proxy_for=pop_id)

        return GraphSection(
            nodes,
            inputs={*pop_mapping.keys()},
            outputs={*pop_mapping.values()},
            must_run=[pop_id])

    @classmethod
    def _join_sections(cls, *sections: GraphSection, allow_partial_inputs: bool = False):

        n_sections = len(sections)
        first_section = sections[0]
        last_section = sections[-1]

        nodes = {**first_section.nodes}
        inputs = set(first_section.inputs)
        must_run = list()

        for i in range(1, n_sections):

            current_section = sections[i]

            requirements_not_met = set(filter(
                lambda n: n not in nodes,
                current_section.inputs))

            if any(requirements_not_met):
                if allow_partial_inputs:
                    inputs.update(requirements_not_met)
                else:
                    raise _ex.ETracInternal()  # todo inconsistent graph

            nodes.update(current_section.nodes)
            must_run.extend(current_section.must_run)

        return GraphSection(nodes, inputs, last_section.outputs, must_run)
