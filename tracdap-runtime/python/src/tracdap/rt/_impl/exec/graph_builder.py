#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import itertools as _itr
import typing as _tp

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.util as _util

from .graph import *


class GraphBuilder:

    __JOB_DETAILS = _tp.TypeVar(
        "__JOB_DETAILS",
        _meta.RunModelJob,
        _meta.RunFlowJob,
        _meta.ImportModelJob,
        _meta.ImportDataJob,
        _meta.ExportDataJob)

    __JOB_BUILD_FUNC = _tp.Callable[[_meta.JobDefinition, NodeId], GraphSection]

    @classmethod
    def dynamic(cls, context: GraphContext) -> "GraphBuilder":

        sys_config = _cfg.RuntimeConfig(storage=context.storage_config)
        job_config = _cfg.JobConfig(context.job_id)

        return GraphBuilder(sys_config, job_config)

    def __init__(self, sys_config: _cfg.RuntimeConfig, job_config: _cfg.JobConfig):

        self._sys_config = sys_config
        self._job_config = job_config

        self._job_key = _util.object_key(job_config.jobId)
        self._job_namespace = NodeNamespace(self._job_key)

        # Dictionary of object type to preallocated IDs
        self._preallocated_ids = dict(
            (k, list(v)) for k, v in _itr.groupby(
                sorted(job_config.preallocatedIds, key=lambda oid: oid.objectType.value),
                lambda oid: oid.objectType))

        self._errors = list()

    def unallocated_ids(self) -> _tp.Dict[_meta.ObjectType, _meta.TagHeader]:
        return self._preallocated_ids

    def _child_builder(self, job_id: _meta.TagHeader) -> "GraphBuilder":

        builder = GraphBuilder(self._sys_config, self._job_config)
        builder._job_key = _util.object_key(job_id)
        builder._job_namespace = NodeNamespace(builder._job_key)

        # Do not share preallocated IDs with the child graph
        builder._preallocated_ids = dict()

        return builder

    def build_job(self, job_def: _meta.JobDefinition, ) -> Graph:

        try:

            if job_def.jobType == _meta.JobType.IMPORT_MODEL:
                graph = self.build_standard_job(job_def, self.build_import_model_job)

            elif job_def.jobType == _meta.JobType.RUN_MODEL:
                graph = self.build_standard_job(job_def, self.build_run_model_job)

            elif job_def.jobType == _meta.JobType.RUN_FLOW:
                graph = self.build_standard_job(job_def, self.build_run_flow_job)

            elif job_def.jobType in [_meta.JobType.IMPORT_DATA, _meta.JobType.EXPORT_DATA]:
                graph = self.build_standard_job(job_def, self.build_import_export_data_job)

            elif job_def.jobType == _meta.JobType.JOB_GROUP:
                graph = self.build_standard_job(job_def, self.build_job_group)

            else:
                self._error(_ex.EJobValidation(f"Job type [{job_def.jobType.name}] is not supported yet"))
                raise self._error_summary()

            if any(self._errors):
                raise self._error_summary()
            else:
                return graph

        except Exception as e:

            # If there are recorded, errors, assume unhandled exceptions are a result of those
            # Only report the recorded errors, to reduce noise
            if any(self._errors):
                raise self._error_summary()

            # If no errors are recorded, an exception here would be a bug
            raise _ex.ETracInternal(f"Unexpected error preparing the job execution graph") from e

    def _error_summary(self) -> Exception:

        if len(self._errors) == 1:
            return self._errors[0]
        else:
            err_text = "\n".join(map(str, self._errors))
            return _ex.EJobValidation("Invalid job configuration\n" + err_text)

    def build_standard_job(self, job_def: _meta.JobDefinition, build_func: __JOB_BUILD_FUNC):

        # Set up the job context

        push_id = NodeId("trac_job_push", self._job_namespace, Bundle[_tp.Any])
        push_node = ContextPushNode(push_id, self._job_namespace)
        push_section = GraphSection({push_id: push_node}, must_run=[push_id])

        # Build the execution graphs for the main job and results recording

        main_section = build_func(job_def, push_id)
        main_result_id = NodeId.of("trac_job_result", self._job_namespace, _cfg.JobResult)

        # Clean up the job context

        global_result_id = NodeId.of(self._job_key, NodeNamespace.root(), _cfg.JobResult)

        pop_id = NodeId("trac_job_pop", self._job_namespace, Bundle[_tp.Any])
        pop_mapping = {main_result_id: global_result_id}

        pop_node = ContextPopNode(
            pop_id, self._job_namespace, pop_mapping,
            explicit_deps=[push_id, *main_section.must_run],
            bundle=NodeNamespace.root())

        global_result_node = BundleItemNode(global_result_id, pop_id, self._job_key)

        pop_section = GraphSection({
            pop_id: pop_node,
            global_result_id: global_result_node})

        job = self._join_sections(push_section, main_section, pop_section)

        return Graph(job.nodes, global_result_id)

    def build_import_model_job(self, job_def: _meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        # TRAC object ID for the new model
        model_id = self._allocate_id(_meta.ObjectType.MODEL)

        import_details = job_def.importModel
        import_scope = self._job_key

        # Graph node ID for the import operation
        import_id = NodeId.of("trac_import_model", self._job_namespace, GraphOutput)

        import_node = ImportModelNode(
            import_id, model_id,
            import_details, import_scope,
            explicit_deps=[job_push_id])

        main_section = GraphSection(nodes={import_id: import_node})

        # RESULT will have a single (unnamed) output
        result_section = self.build_job_result([import_id], explicit_deps=[job_push_id, *main_section.must_run])

        return self._join_sections(main_section, result_section)

    def build_import_export_data_job(self, job_def: _meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        # TODO: These are processed as regular calculation jobs for now
        # That might be ok, but is worth reviewing

        if job_def.jobType == _meta.JobType.IMPORT_DATA:
            job_details = job_def.importData
        else:
            job_details = job_def.exportData

        target_selector = job_details.model
        target_obj = _util.get_job_metadata(target_selector, self._job_config)
        target_def = target_obj.model

        return self.build_calculation_job(
            job_def, job_push_id,
            target_selector, target_def,
            job_details)

    def build_run_model_job(self, job_def: _meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        job_details = job_def.runModel

        target_selector = job_details.model
        target_obj = _util.get_job_metadata(target_selector, self._job_config)
        target_def = target_obj.model

        return self.build_calculation_job(
            job_def, job_push_id,
            target_selector, target_def,
            job_details)

    def build_run_flow_job(self, job_def: _meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        job_details = job_def.runFlow

        target_selector = job_details.flow
        target_obj = _util.get_job_metadata(target_selector, self._job_config)
        target_def = target_obj.flow

        return self.build_calculation_job(
            job_def, job_push_id,
            target_selector, target_def,
            job_details)

    def build_job_group(self, job_def: _meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        job_group = job_def.jobGroup

        if job_group.jobGroupType == _meta.JobGroupType.SEQUENTIAL_JOB_GROUP:
            return self.build_sequential_job_group(job_group, job_push_id)

        if job_group.jobGroupType == _meta.JobGroupType.PARALLEL_JOB_GROUP:
            return self.build_parallel_job_group(job_group, job_push_id)

        else:
            self._error(_ex.EJobValidation(f"Job group type [{job_group.jobGroupType.name}] is not supported yet"))
            return GraphSection(dict(), inputs={job_push_id})

    def build_sequential_job_group(self, job_group: _meta.JobGroup, job_push_id: NodeId) -> GraphSection:

        nodes = dict()
        prior_id = job_push_id

        for child_def in job_group.sequential.jobs:

            child_node = self.build_child_job(child_def, explicit_deps=[prior_id])
            nodes[child_node.id] = child_node

            prior_id = child_node.id

        # No real results from job groups yet (they cannot be executed from the platform)
        job_result =  _cfg.JobResult()
        result_id = NodeId.of("trac_job_result", self._job_namespace, _cfg.JobResult)
        result_node = StaticValueNode(result_id, job_result, explicit_deps=[prior_id])
        nodes[result_id] = result_node

        return GraphSection(nodes, inputs={job_push_id}, outputs={result_id})

    def build_parallel_job_group(self, job_group: _meta.JobGroup, job_push_id: NodeId) -> GraphSection:

        nodes = dict()
        parallel_ids = [job_push_id]

        for child_def in job_group.parallel.jobs:

            child_node = self.build_child_job(child_def, explicit_deps=[job_push_id])
            nodes[child_node.id] = child_node

            parallel_ids.append(child_node.id)

        # No real results from job groups yet (they cannot be executed from the platform)
        job_result =  _cfg.JobResult()
        result_id = NodeId.of("trac_job_result", self._job_namespace, _cfg.JobResult)
        result_node = StaticValueNode(result_id, job_result, explicit_deps=parallel_ids)
        nodes[result_id] = result_node

        return GraphSection(nodes, inputs={job_push_id}, outputs={result_id})

    def build_child_job(self, child_job_def: _meta.JobDefinition, explicit_deps) -> Node[_cfg.JobResult]:

        child_job_id = self._allocate_id(_meta.ObjectType.JOB)

        child_builder = self._child_builder(child_job_id)
        child_graph = child_builder.build_job(child_job_def)

        child_node_name = _util.object_key(child_job_id)
        child_node_id = NodeId.of(child_node_name, self._job_namespace, _cfg.JobResult)

        child_node = ChildJobNode(
            child_node_id, child_job_id, child_job_def,
            child_graph, explicit_deps)

        return child_node

    def build_calculation_job(
            self, job_def: _meta.JobDefinition, job_push_id: NodeId,
            target_selector: _meta.TagSelector,
            target_def: _tp.Union[_meta.ModelDefinition, _meta.FlowDefinition],
            job_details: __JOB_DETAILS) \
            -> GraphSection:

        # The main execution graph can run directly in the job context, no need to do a context push
        # since inputs and outputs in this context line up with the top level execution task

        # Required / provided items are the same for RUN_MODEL and RUN_FLOW jobs

        required_params = target_def.parameters
        required_inputs = target_def.inputs
        expected_outputs = target_def.outputs

        provided_params = job_details.parameters
        provided_inputs = job_details.inputs
        prior_outputs = job_details.priorOutputs

        params_section = self.build_job_parameters(
            required_params, provided_params,
            explicit_deps=[job_push_id])

        input_section = self.build_job_inputs(
            required_inputs, provided_inputs,
            explicit_deps=[job_push_id])

        prior_outputs_section = self.build_job_prior_outputs(
            expected_outputs, prior_outputs,
            explicit_deps=[job_push_id])

        exec_namespace = self._job_namespace
        exec_obj = _util.get_job_metadata(target_selector, self._job_config)

        exec_section = self.build_model_or_flow(
            exec_namespace, job_def, exec_obj,
            explicit_deps=[job_push_id])

        output_section = self.build_job_outputs(
            expected_outputs, prior_outputs,
            explicit_deps=[job_push_id])

        main_section = self._join_sections(
            params_section, input_section, prior_outputs_section,
            exec_section, output_section)

        # Build job-level metadata outputs

        output_ids = list(
            nid for nid, n in main_section.nodes.items()
            if nid.result_type == GraphOutput or isinstance(n, SaveDataNode))

        # Map the SAVE nodes to their corresponding named output keys
        output_keys = dict(
            (nid, nid.name.replace(":SAVE", ""))
            for nid, n in output_section.nodes.items()
            if isinstance(n, SaveDataNode))

        result_section = self.build_job_result(
            output_ids, output_keys,
            explicit_deps=[job_push_id, *main_section.must_run])

        return self._join_sections(main_section, result_section)

    def build_job_parameters(
            self,
            required_params: _tp.Dict[str, _meta.ModelParameter],
            supplied_params: _tp.Dict[str, _meta.Value],
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        nodes = dict()

        for param_name, param_schema in required_params.items():

            param_def = supplied_params.get(param_name)

            if param_def is None:
                if param_schema.defaultValue is not None:
                    param_def = param_schema.defaultValue
                else:
                    self._error(_ex.EJobValidation(f"Missing required parameter: [{param_name}]"))
                    continue

            param_id = NodeId(param_name, self._job_namespace, _meta.Value)
            param_node = StaticValueNode(param_id, param_def, explicit_deps=explicit_deps)

            nodes[param_id] = param_node

        return GraphSection(nodes, outputs=set(nodes.keys()), must_run=list(nodes.keys()))

    def build_job_inputs(
            self,
            required_inputs: _tp.Dict[str, _meta.ModelInputSchema],
            supplied_inputs: _tp.Dict[str, _meta.TagSelector],
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        nodes = dict()
        outputs = set()

        for input_name, input_schema in required_inputs.items():

            input_selector = supplied_inputs.get(input_name)

            if input_selector is None:

                if input_schema.optional:
                    data_view_id = NodeId.of(input_name, self._job_namespace, _data.DataView)
                    data_view = _data.DataView.create_empty(input_schema.objectType)
                    nodes[data_view_id] = StaticValueNode(data_view_id, data_view, explicit_deps=explicit_deps)
                    outputs.add(data_view_id)
                else:
                    self._error(_ex.EJobValidation(f"Missing required input: [{input_name}]"))

                continue

            if input_schema.objectType == _meta.ObjectType.DATA:
                self._build_data_input(input_name, input_selector, nodes, outputs, explicit_deps)
            elif input_schema.objectType == _meta.ObjectType.FILE:
                self._build_file_input(input_name, input_selector, nodes, outputs, explicit_deps)
            else:
                self._error(_ex.EJobValidation(f"Invalid input type [{input_schema.objectType}] for input [{input_name}]"))

        return GraphSection(nodes, outputs=outputs)

    def build_job_prior_outputs(
            self,
            expected_outputs: _tp.Dict[str, _meta.ModelOutputSchema],
            prior_outputs: _tp.Dict[str, _meta.TagSelector],
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        nodes = dict()
        outputs = set()

        for output_name, output_schema in expected_outputs.items():

            prior_selector = prior_outputs.get(output_name)

            # Prior outputs are always optional
            if prior_selector is None:
                continue

            if output_schema.objectType == _meta.ObjectType.DATA:
                prior_spec = self._build_data_spec(prior_selector)
            elif output_schema.objectType == _meta.ObjectType.FILE:
                prior_spec = self._build_file_spec(prior_selector)
            else:
                self._error(_ex.EJobValidation(f"Invalid output type [{output_schema.objectType}] for output [{output_name}]"))
                continue

            prior_output_id = NodeId.of(f"{output_name}:PRIOR", self._job_namespace, _data.DataSpec)
            nodes[prior_output_id] = StaticValueNode(prior_output_id, prior_spec, explicit_deps=explicit_deps)
            outputs.add(prior_output_id)

        return GraphSection(nodes, outputs=outputs)

    def build_job_outputs(
            self,
            required_outputs: _tp.Dict[str, _meta.ModelOutputSchema],
            prior_outputs: _tp.Dict[str, _meta.TagSelector],
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        nodes = {}
        section_inputs = set()

        for output_name, output_schema in required_outputs.items():

            # Output data view must already exist in the namespace, it is an input to the save operation
            data_view_id = NodeId.of(output_name, self._job_namespace, _data.DataView)
            section_inputs.add(data_view_id)

            # Check for prior outputs
            prior_selector = prior_outputs.get(output_name)

            if output_schema.objectType == _meta.ObjectType.DATA:
                self._build_data_output(output_name, output_schema, data_view_id, prior_selector, nodes, explicit_deps)
            elif output_schema.objectType == _meta.ObjectType.FILE:
                self._build_file_output(output_name, output_schema, data_view_id, prior_selector, nodes, explicit_deps)
            else:
                self._error(_ex.EJobValidation(f"Invalid output type [{output_schema.objectType}] for input [{output_name}]"))

        return GraphSection(nodes, inputs=section_inputs)

    def _build_data_input(self, input_name, input_selector, nodes, outputs, explicit_deps):

        data_spec = self._build_data_spec(input_selector)

        # Physical load of data items from disk
        # Currently one item per input, since inputs are single part/delta
        data_load_id = NodeId.of(f"{input_name}:LOAD", self._job_namespace, _data.DataItem)
        nodes[data_load_id] = LoadDataNode(data_load_id, spec=data_spec, explicit_deps=explicit_deps)

        # Input views assembled by mapping one root part to each view
        data_view_id = NodeId.of(input_name, self._job_namespace, _data.DataView)
        nodes[data_view_id] = DataViewNode(data_view_id, data_spec.schema, data_load_id)
        outputs.add(data_view_id)

    def _build_data_output(self, output_name, output_schema, data_view_id, prior_selector, nodes, explicit_deps):

        # Map one data item from each view, since outputs are single part/delta
        data_item_id = NodeId(f"{output_name}:ITEM", self._job_namespace, _data.DataItem)
        nodes[data_item_id] = DataItemNode(data_item_id, data_view_id)

        if prior_selector is None:
            # New output - Allocate new TRAC object IDs
            prior_spec = None
            data_id = self._allocate_id(_meta.ObjectType.DATA)
            storage_id = self._allocate_id(_meta.ObjectType.STORAGE)
        else:
            # New version - Get the prior version metadata and bump the object IDs
            prior_spec = self._build_data_spec(prior_selector)
            data_id = _util.new_object_version(prior_spec.primary_id)
            storage_id = _util.new_object_version(prior_spec.storage_id)

        # Graph node ID for the save operation
        data_save_id = NodeId.of(f"{output_name}:SAVE", self._job_namespace, _data.DataSpec)

        if output_schema.dynamic:

            # For dynamic outputs, an extra graph node is needed to assemble the schema information
            # This will call build_data_spec() at runtime, once the schema is known
            data_spec_id = NodeId.of(f"{output_name}:DYNAMIC_SCHEMA", self._job_namespace, _data.DataSpec)
            nodes[data_spec_id] = DataSpecNode(
                data_spec_id, data_view_id,
                data_id, storage_id, output_name,
                self._sys_config.storage,
                prior_data_spec=prior_spec,
                explicit_deps=explicit_deps)

            # Save operation uses the dynamically produced schema info
            nodes[data_save_id] = SaveDataNode(data_save_id, data_item_id, spec_id=data_spec_id)

        else:

            # If the output is not dynamic, a data spec can be built ahead of time
            data_spec = _data.build_data_spec(
                data_id, storage_id, output_name,
                output_schema.schema,
                self._sys_config.storage,
                prior_spec=prior_spec)

            # Save operation uses the statically produced schema info
            nodes[data_save_id] = SaveDataNode(data_save_id, data_item_id, spec=data_spec)

    def _build_data_spec(self, data_selector):

        # Build a data spec using metadata from the job config
        # For now we are always loading the root part, snap 0, delta 0
        data_def = _util.get_job_metadata(data_selector, self._job_config).data
        storage_def = _util.get_job_metadata(data_def.storageId, self._job_config).storage

        if data_def.schemaId:
            schema_def = _util.get_job_metadata(data_def.schemaId, self._job_config).schema
        else:
            schema_def = data_def.schema

        root_part_opaque_key = 'part-root'  # TODO: Central part names / constants
        data_item = data_def.parts[root_part_opaque_key].snap.deltas[0].dataItem

        data_id = _util.get_job_mapping(data_selector, self._job_config)
        storage_id = _util.get_job_mapping(data_def.storageId, self._job_config)

        return _data.DataSpec \
                .create_data_spec(data_item, data_def, storage_def, schema_def) \
                .with_ids(data_id, storage_id)

    def _build_file_input(self, input_name, input_selector, nodes, outputs, explicit_deps):

        file_spec = self._build_file_spec(input_selector)

        file_load_id = NodeId.of(f"{input_name}:LOAD", self._job_namespace, _data.DataItem)
        nodes[file_load_id] = LoadDataNode(file_load_id, spec=file_spec, explicit_deps=explicit_deps)

        # Input views assembled by mapping one root part to each view
        file_view_id = NodeId.of(input_name, self._job_namespace, _data.DataView)
        nodes[file_view_id] = DataViewNode(file_view_id, None, file_load_id)
        outputs.add(file_view_id)

    def _build_file_output(self, output_name, output_schema, file_view_id, prior_selector, nodes, explicit_deps):

        # Map file item from view
        file_item_id = NodeId(f"{output_name}:ITEM", self._job_namespace, _data.DataItem)
        nodes[file_item_id] = DataItemNode(file_item_id, file_view_id, explicit_deps=explicit_deps)

        if prior_selector is None:
            # New output - Allocate new TRAC object IDs
            prior_spec = None
            file_id = self._allocate_id(_meta.ObjectType.FILE)
            storage_id = self._allocate_id(_meta.ObjectType.STORAGE)
        else:
            # New version - Get the prior version metadata and bump the object IDs
            prior_spec = self._build_file_spec(prior_selector) if prior_selector else None
            file_id = _util.new_object_version(prior_spec.primary_id)
            storage_id = _util.new_object_version(prior_spec.storage_id)

        # File spec can always be built ahead of time (no equivalent of dynamic schemas)
        file_spec = _data.build_file_spec(
            file_id, storage_id,
            output_name, output_schema.fileType,
            self._sys_config.storage,
            prior_spec=prior_spec)

        # Graph node for the save operation
        file_save_id = NodeId.of(f"{output_name}:SAVE", self._job_namespace, _data.DataSpec)
        nodes[file_save_id] = SaveDataNode(file_save_id, file_item_id, spec=file_spec)

    def _build_file_spec(self, file_selector):

        file_def = _util.get_job_metadata(file_selector, self._job_config).file
        storage_def = _util.get_job_metadata(file_def.storageId, self._job_config).storage

        file_id = _util.get_job_mapping(file_selector, self._job_config)
        storage_id = _util.get_job_mapping(file_def.storageId, self._job_config)

        return _data.DataSpec \
            .create_file_spec(file_def.dataItem, file_def, storage_def) \
            .with_ids(file_id, storage_id)

    def build_model_or_flow_with_context(
            self, namespace: NodeNamespace, model_or_flow_name: str,
            job_def: _meta.JobDefinition, model_or_flow: _meta.ObjectDefinition,
            input_mapping: _tp.Dict[str, NodeId], output_mapping: _tp.Dict[str, NodeId],
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        # Generate a name for a new unique sub-context

        sub_namespace_name = f"{model_or_flow.objectType.name} = {model_or_flow_name}"
        sub_namespace = NodeNamespace(sub_namespace_name, namespace)

        # Execute in the sub-context by doing PUSH, EXEC, POP
        # Note that POP node must be in the sub namespace too

        push_section = self.build_context_push(
            sub_namespace, input_mapping,
            explicit_deps)

        exec_section = self.build_model_or_flow(
            sub_namespace, job_def, model_or_flow,
            explicit_deps=push_section.must_run)

        pop_section = self.build_context_pop(
            sub_namespace, output_mapping,
            explicit_deps=exec_section.must_run)

        return self._join_sections(push_section, exec_section, pop_section)

    def build_model_or_flow(
            self, namespace: NodeNamespace,
            job_def: _meta.JobDefinition,
            model_or_flow: _meta.ObjectDefinition,
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        if model_or_flow.objectType == _meta.ObjectType.MODEL:
            return self.build_model(namespace, job_def, model_or_flow.model, explicit_deps)

        elif model_or_flow.objectType == _meta.ObjectType.FLOW:
            return self.build_flow(namespace, job_def, model_or_flow.flow)

        else:
            message = f"Invalid job config, expected model or flow, got [{model_or_flow.objectType}]"
            self._error(_ex.EJobValidation(message))

            # Allow building to continue for better error reporting
            return GraphSection(dict())

    def build_model(
            self, namespace: NodeNamespace,
            job_def: _meta.JobDefinition,
            model_def: _meta.ModelDefinition,
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        self.check_model_type(job_def, model_def)

        def param_id(node_name):
            return NodeId(node_name, namespace, _meta.Value)

        def data_id(node_name):
            return NodeId(node_name, namespace, _data.DataView)

        # Input data should already be mapped to named inputs in the model context
        parameter_ids = set(map(param_id, model_def.parameters))
        input_ids = set(map(data_id, model_def.inputs))
        output_ids = set(map(data_id, model_def.outputs))

        # Set up storage access for import / export data jobs
        if job_def.jobType == _meta.JobType.IMPORT_DATA:
            storage_access = job_def.importData.storageAccess
        elif job_def.jobType == _meta.JobType.EXPORT_DATA:
            storage_access = job_def.exportData.storageAccess
        else:
            storage_access = None

        # Create the model node
        # Always add the prior graph root ID as a dependency
        # This is to ensure dependencies are still pulled in for models with no inputs!

        job_namespace = namespace
        while job_namespace.parent != NodeNamespace.root():
            job_namespace = job_namespace.parent

        model_scope = str(job_namespace)
        model_name = model_def.entryPoint.split(".")[-1]  # TODO: Check unique model name
        model_id = NodeId(model_name, namespace, Bundle[_data.DataView])

        # Used to set up a dynamic builder at runtime if dynamic graph updates are needed
        context = GraphContext(
            self._job_config.jobId,
            self._job_namespace, namespace,
            self._sys_config.storage)

        model_node = RunModelNode(
            model_id, model_def, model_scope,
            frozenset(parameter_ids), frozenset(input_ids),
            explicit_deps=explicit_deps, bundle=model_id.namespace,
            storage_access=storage_access, graph_context=context)

        nodes = {model_id: model_node}

        # Create nodes for each model output
        # The model node itself outputs a bundle (dictionary of named outputs)
        # These need to be mapped to individual nodes in the graph

        # These output mapping nodes are closely tied to the representation of the model itself
        # In the future, we may want models to emit individual outputs before the whole model is complete

        for output_id in output_ids:
            nodes[output_id] = BundleItemNode(output_id, model_id, output_id.name)

        # Assemble a graph to include the model and its outputs
        return GraphSection(nodes, inputs={*parameter_ids, *input_ids}, outputs=output_ids, must_run=[model_id])

    def build_flow(
            self, namespace: NodeNamespace,
            job_def: _meta.JobDefinition,
            flow_def: _meta.FlowDefinition,
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        def socket_key(socket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        # https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm

        # Group edges by source and target node
        remaining_edges_by_target = {edge.target.node: [] for edge in flow_def.edges}
        remaining_edges_by_source = {edge.source.node: [] for edge in flow_def.edges}

        for edge in flow_def.edges:
            remaining_edges_by_target[edge.target.node].append(edge)
            remaining_edges_by_source[edge.source.node].append(edge)

        # Group edges by target socket (only one edge per target in a consistent flow)
        target_edges = {socket_key(edge.target): edge for edge in flow_def.edges}

        # Initially parameters and inputs are reachable, everything else is not
        def is_input(n): return n[1].nodeType in [_meta.FlowNodeType.PARAMETER_NODE, _meta.FlowNodeType.INPUT_NODE]
        reachable_nodes = dict(filter(is_input, flow_def.nodes.items()))
        remaining_nodes = dict(filter(lambda n: not is_input(n), flow_def.nodes.items()))

        # Initial graph section for the flow is empty
        graph_section = GraphSection({}, must_run=explicit_deps)

        while any(reachable_nodes):

            node_name, node = reachable_nodes.popitem()

            sub_section = self.build_flow_node(
                namespace, job_def, target_edges,
                node_name, node, explicit_deps)

            graph_section = self._join_sections(graph_section, sub_section, allow_partial_inputs=True)

            if node.nodeType != _meta.FlowNodeType.OUTPUT_NODE:

                source_edges = remaining_edges_by_source.pop(node_name)

                for edge in source_edges:

                    target_node_name = edge.target.node
                    target_edges_ = remaining_edges_by_target[target_node_name]
                    target_edges_.remove(edge)

                    if len(target_edges_) == 0:
                        target_node = remaining_nodes.pop(target_node_name)
                        reachable_nodes[target_node_name] = target_node

        if any(remaining_nodes):
            missing_targets = [edge.target for node in remaining_edges_by_target.values() for edge in node]
            missing_target_names = [f"{t.node}.{t.socket}" if t.socket else t.node for t in missing_targets]
            missing_nodes = list(map(lambda n: NodeId(n, namespace), missing_target_names))
            self._invalid_graph_error(missing_nodes)

        return graph_section

    def build_flow_node(
            self, namespace: NodeNamespace,
            job_def: _meta.JobDefinition,
            target_edges: _tp.Dict[_meta.FlowSocket, _meta.FlowEdge],
            node_name: str, node: _meta.FlowNode,
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        def socket_key(socket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        def socket_id(node_: str, socket_: str = None, result_type=None):
            socket_name = f"{node_}.{socket_}" if socket_ else node_
            return NodeId(socket_name, namespace, result_type)

        def edge_mapping(node_: str, socket_: str = None, result_type=None):
            socket = socket_key(_meta.FlowSocket(node_, socket_))
            edge = target_edges.get(socket)
            # Report missing edges as a job consistency error (this might happen sometimes in dev mode)
            if edge is None:
                self._error(_ex.EJobValidation(f"Inconsistent flow: Socket [{socket}] is not connected"))
            return socket_id(edge.source.node, edge.source.socket, result_type)

        if node.nodeType == _meta.FlowNodeType.PARAMETER_NODE:
            return GraphSection({}, inputs={NodeId(node_name, namespace, result_type=_meta.Value)})

        if node.nodeType == _meta.FlowNodeType.INPUT_NODE:
            return GraphSection({}, inputs={NodeId(node_name, namespace, result_type=_data.DataView)})

        if node.nodeType == _meta.FlowNodeType.OUTPUT_NODE:
            target_id = NodeId(node_name, namespace, result_type=_data.DataView)
            source_id = edge_mapping(node_name, None, _data.DataView)
            return GraphSection({target_id: IdentityNode(target_id, source_id)}, outputs={target_id})

        if node.nodeType == _meta.FlowNodeType.MODEL_NODE:

            param_mapping = {socket: edge_mapping(node_name, socket, _meta.Value) for socket in node.parameters}
            input_mapping = {socket: edge_mapping(node_name, socket, _data.DataView) for socket in node.inputs}
            output_mapping = {socket: socket_id(node_name, socket, _data.DataView) for socket in node.outputs}

            push_mapping = {**input_mapping, **param_mapping}
            pop_mapping = output_mapping

            model_selector = job_def.runFlow.models.get(node_name)
            model_obj = _util.get_job_metadata(model_selector, self._job_config)

            # Missing models in the job config is a job consistency error
            if model_obj is None or model_obj.objectType != _meta.ObjectType.MODEL:
                self._error(_ex.EJobValidation(f"No model was provided for flow node [{node_name}]"))

            # Explicit check for model compatibility - report an error now, do not try build_model()
            self.check_model_compatibility(model_selector, model_obj.model, node_name, node)
            self.check_model_type(job_def, model_obj.model)

            return self.build_model_or_flow_with_context(
                namespace, node_name,
                job_def, model_obj,
                push_mapping, pop_mapping,
                explicit_deps)

        self._error(_ex.EJobValidation(f"Flow node [{node_name}] has invalid node type [{node.nodeType}]"))

        # Allow building to continue for better error reporting
        return GraphSection(dict())

    def check_model_compatibility(
            self, model_selector: _meta.TagSelector,
            model_def: _meta.ModelDefinition, node_name: str, flow_node: _meta.FlowNode):

        model_params = list(sorted(model_def.parameters.keys()))
        model_inputs = list(sorted(model_def.inputs.keys()))
        model_outputs = list(sorted(model_def.outputs.keys()))

        node_params = list(sorted(flow_node.parameters))
        node_inputs = list(sorted(flow_node.inputs))
        node_outputs = list(sorted(flow_node.outputs))

        if model_params != node_params or model_inputs != node_inputs or model_outputs != node_outputs:
            model_key = _util.object_key(model_selector)
            self._error(_ex.EJobValidation(f"Incompatible model for flow node [{node_name}] (Model: [{model_key}])"))

    def check_model_type(self, job_def: _meta.JobDefinition, model_def: _meta.ModelDefinition):

        if job_def.jobType == _meta.JobType.IMPORT_DATA:
            allowed_model_types = [_meta.ModelType.DATA_IMPORT_MODEL]
        elif job_def.jobType == _meta.JobType.EXPORT_DATA:
            allowed_model_types = [_meta.ModelType.DATA_EXPORT_MODEL]
        else:
            allowed_model_types = [_meta.ModelType.STANDARD_MODEL]

        if model_def.modelType not in allowed_model_types:
            job_type = job_def.jobType.name
            model_type = model_def.modelType.name
            self._error(_ex.EJobValidation(f"Job type [{job_type}] cannot use model type [{model_type}]"))

    @staticmethod
    def build_context_push(
            namespace: NodeNamespace, input_mapping: _tp.Dict[str, NodeId],
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        """
        Create a context push operation, all inputs are mapped by name
        """

        push_mapping = {
            NodeId(input_name, namespace, outer_id.result_type): outer_id
            for input_name, outer_id
            in input_mapping.items()}

        push_id = NodeId("trac_ctx_push", namespace, Bundle[_tp.Any])
        push_node = ContextPushNode(push_id, namespace, push_mapping, explicit_deps, bundle=push_id.namespace)

        nodes = {push_id: push_node}

        # Create an explicit marker for each data node pushed into the new context
        for inner_id, outer_id in push_mapping.items():
            nodes[inner_id] = BundleItemNode(inner_id, push_id, inner_id.name, explicit_deps=[push_id])

        return GraphSection(
            nodes,
            inputs={*push_mapping.values()},
            outputs={*push_mapping.keys()},
            must_run=[push_id])

    @staticmethod
    def build_context_pop(
            namespace: NodeNamespace, output_mapping: _tp.Dict[str, NodeId],
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        """
        Create a context pop operation, all outputs are mapped by name
        """

        pop_mapping = {
            NodeId(output_name, namespace, outer_id.result_type): outer_id
            for output_name, outer_id
            in output_mapping.items()}

        push_id = NodeId("trac_ctx_push", namespace, Bundle[_tp.Any])
        explicit_deps = [push_id, *explicit_deps] if explicit_deps else [push_id]

        pop_id = NodeId("trac_ctx_pop", namespace, Bundle[_tp.Any])
        pop_node = ContextPopNode(
            pop_id, namespace, pop_mapping,
            explicit_deps=explicit_deps,
            bundle=pop_id.namespace.parent)

        nodes = {pop_id: pop_node}

        # Create an explicit marker for each data node popped into the outer context
        for inner_id, outer_id in pop_mapping.items():
            nodes[outer_id] = BundleItemNode(outer_id, pop_id, outer_id.name, explicit_deps=[pop_id])

        return GraphSection(
            nodes,
            inputs={*pop_mapping.keys()},
            outputs={*pop_mapping.values()},
            must_run=[pop_id])

    def build_job_result(
            self, output_ids: _tp.List[NodeId[JOB_OUTPUT_TYPE]],
            output_keys: _tp.Optional[_tp.Dict[NodeId, str]] = None,
            explicit_deps: _tp.Optional[_tp.List[NodeId]] = None) \
            -> GraphSection:

        if output_keys:
            named_outputs = dict((output_keys[oid], oid) for oid in filter(lambda oid: oid in output_keys, output_ids))
            unnamed_outputs = list(filter(lambda oid: oid not in output_keys, output_ids))
        else:
            named_outputs = dict()
            unnamed_outputs = output_ids

        result_node_id = NodeId.of("trac_job_result", self._job_namespace, _cfg.JobResult)
        result_node = JobResultNode(
            result_node_id,
            self._job_config.jobId,
            self._job_config.resultId,
            named_outputs, unnamed_outputs,
            explicit_deps=explicit_deps)

        result_nodes = {result_node_id: result_node}

        return GraphSection(result_nodes, inputs=set(output_ids), must_run=[result_node_id])

    def build_dynamic_outputs(self, source_id: NodeId, output_names: _tp.List[str]) -> GraphUpdate:

        nodes = dict()
        dependencies = dict()

        # All dynamic outputs are DATA with dynamic schemas for now
        dynamic_schema = _meta.ModelOutputSchema(
            objectType=_meta.ObjectType.DATA,
            schema=None, dynamic=True)

        for output_name in output_names:

            # Node to extract dynamic outputs from the source node (a model or flow output bundle)
            output_id = NodeId.of(output_name, source_id.namespace, _data.DataView)
            output_node = BundleItemNode(output_id, source_id, output_name)
            nodes[output_id] = output_node

            # All dynamic outputs are DATA for now
            self._build_data_output(output_name, dynamic_schema, output_id, prior_selector=None, nodes=nodes,
                                    explicit_deps=[source_id])

        named_outputs = dict(
            (nid.name, nid) for nid, n in nodes.items()
            if nid.result_type == GraphOutput or isinstance(n, SaveDataNode))

        dynamic_outputs_id = NodeId.of("trac_dynamic_outputs", source_id.namespace, DynamicOutputsNode)
        dynamic_outputs_node = DynamicOutputsNode(
            dynamic_outputs_id, named_outputs,
            explicit_deps=[source_id])

        job_result_id = NodeId.of("trac_job_result", self._job_namespace, _cfg.JobResult)

        nodes[dynamic_outputs_id] = dynamic_outputs_node
        dependencies[job_result_id] = [Dependency(dynamic_outputs_id, DependencyType.HARD)]

        return GraphUpdate(nodes, dependencies)

    def _allocate_id(self, object_type: _meta.ObjectType):

        preallocated_ids = self._preallocated_ids.get(object_type)

        if preallocated_ids:
            # Preallocated IDs have objectVersion = 0, use a new version to get objectVersion = 1
            return _util.new_object_version(preallocated_ids.pop())
        else:
            return _util.new_object_id(object_type)

    def _join_sections(self, *sections: GraphSection, allow_partial_inputs: bool = False):

        n_sections = len(sections)
        first_section = sections[0]
        last_section = sections[-1]

        nodes = {**first_section.nodes}
        inputs = set(first_section.inputs)
        must_run = list(first_section.must_run) if first_section.must_run else []

        for i in range(1, n_sections):

            current_section = sections[i]

            requirements_not_met = set(filter(
                lambda n: n not in nodes,
                current_section.inputs))

            if any(requirements_not_met):
                if allow_partial_inputs:
                    inputs.update(requirements_not_met)
                else:
                    self._invalid_graph_error(requirements_not_met)

            nodes.update(current_section.nodes)

            must_run = list(filter(lambda n: n not in current_section.inputs, must_run))
            must_run.extend(current_section.must_run)

        return GraphSection(nodes, inputs, last_section.outputs, must_run)

    def _invalid_graph_error(self, missing_dependencies: _tp.Iterable[NodeId]):

        missing_ids = ", ".join(map(self._missing_item_display_name, missing_dependencies))
        message = f"The execution graph has unsatisfied dependencies: [{missing_ids}]"

        self._error(_ex.EJobValidation(message))

    @classmethod
    def _missing_item_display_name(cls, node_id: NodeId):

        components = node_id.namespace.components()

        # The execution graph is built for an individual job, so the top-level namespace is always the job key
        # Do not list the job key component with every missing node, that would be overly verbose
        # If we ever start using global resources (to share between jobs), this logic may need to change

        if len(components) <= 1:
            return node_id.name
        else:
            return f"{node_id.name} / {', '.join(components[:-1])}"

    def _error(self, error: Exception):

        self._errors.append(error)
