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

import datetime as _dt

import tracdap.rt.config as config
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.util as _util

from .graph import *


class GraphBuilder:

    __JOB_DETAILS = tp.TypeVar(
        "__JOB_DETAILS",
        meta.RunModelJob,
        meta.RunFlowJob,
        meta.ImportModelJob,
        meta.ImportDataJob,
        meta.ExportDataJob)

    __JOB_BUILD_FUNC = tp.Callable[[meta.JobDefinition, NodeId], GraphSection]

    def __init__(self, sys_config: config.RuntimeConfig, job_config: config.JobConfig):

        self._sys_config = sys_config
        self._job_config = job_config

        self._job_key = _util.object_key(job_config.jobId)
        self._job_namespace = NodeNamespace(self._job_key)

        self._errors = []

    def _child_builder(self, job_id: meta.TagHeader) -> "GraphBuilder":

        builder = GraphBuilder(self._sys_config, self._job_config)
        builder._job_key = _util.object_key(job_id)
        builder._job_namespace = NodeNamespace(builder._job_key)

        return builder

    def build_job(self, job_def: meta.JobDefinition,) -> Graph:

        try:

            if job_def.jobType == meta.JobType.IMPORT_MODEL:
                return self.build_standard_job(job_def, self.build_import_model_job)

            if job_def.jobType == meta.JobType.RUN_MODEL:
                return self.build_standard_job(job_def, self.build_run_model_job)

            if job_def.jobType == meta.JobType.RUN_FLOW:
                return self.build_standard_job(job_def, self.build_run_flow_job)

            if job_def.jobType in [meta.JobType.IMPORT_DATA, meta.JobType.EXPORT_DATA]:
                return self.build_standard_job(job_def, self.build_import_export_data_job)

            if job_def.jobType == meta.JobType.JOB_GROUP:
                return self.build_standard_job(job_def, self.build_job_group)

            self._error(_ex.EJobValidation(f"Job type [{job_def.jobType.name}] is not supported yet"))

        except Exception as e:

            # If there are recorded, errors, assume unhandled exceptions are a result of those
            # Only report the recorded errors, to reduce noise
            if any(self._errors):
                pass

            # If no errors are recorded, an exception here would be a bug
            raise _ex.ETracInternal(f"Unexpected error preparing the job execution graph") from e

        finally:

            if any(self._errors):

                if len(self._errors) == 1:
                    raise self._errors[0]
                else:
                    err_text = "\n".join(map(str, self._errors))
                    raise _ex.EJobValidation("Invalid job configuration\n" + err_text)

    def build_standard_job(self, job_def: meta.JobDefinition, build_func: __JOB_BUILD_FUNC):

        # Set up the job context

        push_id = NodeId("trac_job_push", self._job_namespace, Bundle[tp.Any])
        push_node = ContextPushNode(push_id, self._job_namespace)
        push_section = GraphSection({push_id: push_node}, must_run=[push_id])

        # Build the execution graphs for the main job and results recording

        main_section = build_func(job_def, push_id)
        main_result_id = NodeId.of("trac_job_result", self._job_namespace, config.JobResult)

        # Clean up the job context

        global_result_id = NodeId.of(self._job_key, NodeNamespace.root(), config.JobResult)

        pop_id = NodeId("trac_job_pop", self._job_namespace, Bundle[tp.Any])
        pop_mapping = {main_result_id: global_result_id}

        pop_node = ContextPopNode(
            pop_id, self._job_namespace, pop_mapping,
            explicit_deps=main_section.must_run,
            bundle=NodeNamespace.root())

        global_result_node = BundleItemNode(global_result_id, pop_id, self._job_key)

        pop_section = GraphSection({
            pop_id: pop_node,
            global_result_id: global_result_node})

        job = self._join_sections(push_section, main_section, pop_section)

        return Graph(job.nodes, global_result_id)

    def build_import_model_job(self, job_def: meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        # Main section: run the model import

        # TODO: Import model job should pre-allocate an ID, then model ID comes from job_config.resultMapping
        new_model_id = _util.new_object_id(meta.ObjectType.MODEL)
        new_model_key = _util.object_key(new_model_id)

        model_scope = self._job_key
        import_details = job_def.importModel

        import_id = NodeId.of("trac_import_model", self._job_namespace, meta.ObjectDefinition)
        import_node = ImportModelNode(import_id, model_scope, import_details, explicit_deps=[job_push_id])

        main_section = GraphSection(nodes={import_id: import_node})

        # Build job-level metadata outputs

        result_section = self.build_job_results(
            objects={new_model_key: import_id},
            explicit_deps=[job_push_id, *main_section.must_run])

        return self._join_sections(main_section, result_section)

    def build_import_export_data_job(self, job_def: meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        # TODO: These are processed as regular calculation jobs for now
        # That might be ok, but is worth reviewing

        if job_def.jobType == meta.JobType.IMPORT_DATA:
            job_details = job_def.importData
        else:
            job_details = job_def.exportData

        target_selector = job_details.model
        target_obj = _util.get_job_resource(target_selector, self._job_config)
        target_def = target_obj.model

        return self.build_calculation_job(
            job_def, job_push_id,
            target_selector, target_def,
            job_details)

    def build_run_model_job(self, job_def: meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        job_details = job_def.runModel

        target_selector = job_details.model
        target_obj = _util.get_job_resource(target_selector, self._job_config)
        target_def = target_obj.model

        return self.build_calculation_job(
            job_def, job_push_id,
            target_selector, target_def,
            job_details)

    def build_run_flow_job(self, job_def: meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        job_details = job_def.runFlow

        target_selector = job_details.flow
        target_obj = _util.get_job_resource(target_selector, self._job_config)
        target_def = target_obj.flow

        return self.build_calculation_job(
            job_def, job_push_id,
            target_selector, target_def,
            job_details)

    def build_job_group(self, job_def: meta.JobDefinition, job_push_id: NodeId) -> GraphSection:

        job_group = job_def.jobGroup

        if job_group.jobGroupType == meta.JobGroupType.SEQUENTIAL_JOB_GROUP:
            return self.build_sequential_job_group(job_group, job_push_id)

        if job_group.jobGroupType == meta.JobGroupType.PARALLEL_JOB_GROUP:
            return self.build_parallel_job_group(job_group, job_push_id)

        else:
            self._error(_ex.EJobValidation(f"Job group type [{job_group.jobGroupType.name}] is not supported yet"))
            return GraphSection(dict(), inputs={job_push_id})

    def build_sequential_job_group(self, job_group: meta.JobGroup, job_push_id: NodeId) -> GraphSection:

        nodes = dict()
        prior_id = job_push_id

        for child_def in job_group.sequential.jobs:

            child_node = self.build_child_job(child_def, explicit_deps=[prior_id])
            nodes[child_node.id] = child_node

            prior_id = child_node.id

        # No real results from job groups yet (they cannot be executed from the platform)
        job_result =  cfg.JobResult()
        result_id = NodeId.of("trac_job_result", self._job_namespace, cfg.JobResult)
        result_node = StaticValueNode(result_id, job_result, explicit_deps=[prior_id])
        nodes[result_id] = result_node

        return GraphSection(nodes, inputs={job_push_id}, outputs={result_id})

    def build_parallel_job_group(self, job_group: meta.JobGroup, job_push_id: NodeId) -> GraphSection:

        nodes = dict()
        parallel_ids = [job_push_id]

        for child_def in job_group.parallel.jobs:

            child_node = self.build_child_job(child_def, explicit_deps=[job_push_id])
            nodes[child_node.id] = child_node

            parallel_ids.append(child_node.id)

        # No real results from job groups yet (they cannot be executed from the platform)
        job_result =  cfg.JobResult()
        result_id = NodeId.of("trac_job_result", self._job_namespace, cfg.JobResult)
        result_node = StaticValueNode(result_id, job_result, explicit_deps=parallel_ids)
        nodes[result_id] = result_node

        return GraphSection(nodes, inputs={job_push_id}, outputs={result_id})

    def build_child_job(self, child_job_def: meta.JobDefinition, explicit_deps) -> Node[config.JobResult]:

        child_job_id = _util.new_object_id(meta.ObjectType.JOB)

        child_builder = self._child_builder(child_job_id)
        child_graph = child_builder.build_job(child_job_def)

        child_node_name = _util.object_key(child_job_id)
        child_node_id = NodeId.of(child_node_name, self._job_namespace, cfg.JobResult)

        child_node = ChildJobNode(
            child_node_id, child_job_id, child_job_def,
            child_graph, explicit_deps)

        return child_node

    def build_calculation_job(
            self, job_def: meta.JobDefinition, job_push_id: NodeId,
            target_selector: meta.TagSelector,
            target_def: tp.Union[meta.ModelDefinition, meta.FlowDefinition],
            job_details: __JOB_DETAILS) \
            -> GraphSection:

        # The main execution graph can run directly in the job context, no need to do a context push
        # since inputs and outputs in this context line up with the top level execution task

        # Required / provided items are the same for RUN_MODEL and RUN_FLOW jobs

        required_params = target_def.parameters
        required_inputs = target_def.inputs
        required_outputs = target_def.outputs

        provided_params = job_details.parameters
        provided_inputs = job_details.inputs
        provided_outputs = job_details.outputs

        params_section = self.build_job_parameters(
            required_params, provided_params,
            explicit_deps=[job_push_id])

        input_section = self.build_job_inputs(
            required_inputs, provided_inputs,
            explicit_deps=[job_push_id])

        exec_namespace = self._job_namespace
        exec_obj = _util.get_job_resource(target_selector, self._job_config)

        exec_section = self.build_model_or_flow(
            exec_namespace, job_def, exec_obj,
            explicit_deps=[job_push_id])

        output_section = self.build_job_outputs(
            required_outputs, provided_outputs,
            explicit_deps=[job_push_id])

        main_section = self._join_sections(params_section, input_section, exec_section, output_section)

        # Build job-level metadata outputs

        data_result_ids = list(
            nid for nid, n in main_section.nodes.items()
            if isinstance(n, DataResultNode))

        result_section = self.build_job_results(
            bundles=data_result_ids,
            explicit_deps=[job_push_id, *main_section.must_run])

        return self._join_sections(main_section, result_section)

    def build_job_parameters(
            self,
            required_params: tp.Dict[str, meta.ModelParameter],
            supplied_params: tp.Dict[str, meta.Value],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
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

            param_id = NodeId(param_name, self._job_namespace, meta.Value)
            param_node = StaticValueNode(param_id, param_def, explicit_deps=explicit_deps)

            nodes[param_id] = param_node

        return GraphSection(nodes, outputs=set(nodes.keys()), must_run=list(nodes.keys()))

    def build_job_inputs(
            self,
            required_inputs: tp.Dict[str, meta.ModelInputSchema],
            supplied_inputs: tp.Dict[str, meta.TagSelector],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        nodes = dict()
        outputs = set()

        for input_name, input_def in required_inputs.items():

            # Backwards compatibility with pre 0.8 versions
            input_type = meta.ObjectType.DATA \
                if input_def.objectType == meta.ObjectType.OBJECT_TYPE_NOT_SET \
                else input_def.objectType

            input_selector = supplied_inputs.get(input_name)

            if input_selector is None:

                if input_def.optional:
                    data_view_id = NodeId.of(input_name, self._job_namespace, _data.DataView)
                    data_view = _data.DataView.create_empty(input_type)
                    nodes[data_view_id] = StaticValueNode(data_view_id, data_view, explicit_deps=explicit_deps)
                    outputs.add(data_view_id)
                else:
                    self._error(_ex.EJobValidation(f"Missing required input: [{input_name}]"))

            elif input_type == meta.ObjectType.DATA:
                self._build_data_input(input_name, input_selector, nodes, outputs, explicit_deps)

            elif input_type == meta.ObjectType.FILE:
                self._build_file_input(input_name, input_selector, nodes, outputs, explicit_deps)

            else:
                self._error(_ex.EJobValidation(f"Invalid input type [{input_type.name}] for input [{input_name}]"))

        return GraphSection(nodes, outputs=outputs)

    def _build_data_input(self, input_name, input_selector, nodes, outputs, explicit_deps):

        # Build a data spec using metadata from the job config
        # For now we are always loading the root part, snap 0, delta 0
        data_def = _util.get_job_resource(input_selector, self._job_config).data
        storage_def = _util.get_job_resource(data_def.storageId, self._job_config).storage

        if data_def.schemaId:
            schema_def = _util.get_job_resource(data_def.schemaId, self._job_config).schema
        else:
            schema_def = data_def.schema

        root_part_opaque_key = 'part-root'  # TODO: Central part names / constants
        data_item = data_def.parts[root_part_opaque_key].snap.deltas[0].dataItem
        data_spec = _data.DataSpec.create_data_spec(data_item, data_def, storage_def, schema_def)

        # Physical load of data items from disk
        # Currently one item per input, since inputs are single part/delta
        data_load_id = NodeId.of(f"{input_name}:LOAD", self._job_namespace, _data.DataItem)
        nodes[data_load_id] = LoadDataNode(data_load_id, spec=data_spec, explicit_deps=explicit_deps)

        # Input views assembled by mapping one root part to each view
        data_view_id = NodeId.of(input_name, self._job_namespace, _data.DataView)
        nodes[data_view_id] = DataViewNode(data_view_id, schema_def, data_load_id)
        outputs.add(data_view_id)

    def _build_file_input(self, input_name, input_selector, nodes, outputs, explicit_deps):

        file_def = _util.get_job_resource(input_selector, self._job_config).file
        storage_def = _util.get_job_resource(file_def.storageId, self._job_config).storage

        file_spec = _data.DataSpec.create_file_spec(file_def.dataItem, file_def, storage_def)
        file_load_id = NodeId.of(f"{input_name}:LOAD", self._job_namespace, _data.DataItem)
        nodes[file_load_id] = LoadDataNode(file_load_id, spec=file_spec, explicit_deps=explicit_deps)

        # Input views assembled by mapping one root part to each view
        file_view_id = NodeId.of(input_name, self._job_namespace, _data.DataView)
        nodes[file_view_id] = DataViewNode(file_view_id, None, file_load_id)
        outputs.add(file_view_id)

    def build_job_outputs(
            self,
            required_outputs: tp.Dict[str, meta.ModelOutputSchema],
            supplied_outputs: tp.Dict[str, meta.TagSelector],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        nodes = {}
        inputs = set()

        for output_name, output_def in required_outputs.items():

            # Output data view must already exist in the namespace, it is an input to the save operation
            data_view_id = NodeId.of(output_name, self._job_namespace, _data.DataView)
            inputs.add(data_view_id)

            # Backwards compatibility with pre 0.8 versions
            output_type = meta.ObjectType.DATA \
                if output_def.objectType == meta.ObjectType.OBJECT_TYPE_NOT_SET \
                else output_def.objectType

            output_selector = supplied_outputs.get(output_name)

            if output_selector is None:
                if output_def.optional:
                    optional_info = "(configuration is required for all optional outputs, in case they are produced)"
                    self._error(_ex.EJobValidation(f"Missing optional output: [{output_name}] {optional_info}"))
                    continue
                else:
                    self._error(_ex.EJobValidation(f"Missing required output: [{output_name}]"))
                    continue

            elif output_type == meta.ObjectType.DATA:
                self._build_data_output(output_name, output_selector, data_view_id, nodes, explicit_deps)

            elif output_type == meta.ObjectType.FILE:
                self._build_file_output(output_name, output_def, output_selector, data_view_id, nodes, explicit_deps)

            else:
                self._error(_ex.EJobValidation(f"Invalid output type [{output_type.name}] for input [{output_name}]"))

        return GraphSection(nodes, inputs=inputs)

    def _build_data_output(self, output_name, output_selector, data_view_id, nodes, explicit_deps):

        # Map one data item from each view, since outputs are single part/delta
        data_item_id = NodeId(f"{output_name}:ITEM", self._job_namespace, _data.DataItem)
        nodes[data_item_id] = DataItemNode(data_item_id, data_view_id)

        data_obj = _util.get_job_resource(output_selector, self._job_config, optional=True)

        if data_obj is not None:

            # If data def for the output has been built in advance, use a static data spec

            data_def = data_obj.data
            storage_def = _util.get_job_resource(data_def.storageId, self._job_config).storage

            if data_def.schemaId:
                schema_def = _util.get_job_resource(data_def.schemaId, self._job_config).schema
            else:
                schema_def = data_def.schema

            root_part_opaque_key = 'part-root'  # TODO: Central part names / constants
            data_item = data_def.parts[root_part_opaque_key].snap.deltas[0].dataItem
            data_spec = _data.DataSpec.create_data_spec(data_item, data_def, storage_def, schema_def)

            # Create a physical save operation for the data item
            data_save_id = NodeId.of(f"{output_name}:SAVE", self._job_namespace, _data.DataSpec)
            nodes[data_save_id] = SaveDataNode(data_save_id, data_item_id, spec=data_spec)

            output_key = output_name
            storage_key = output_name + ":STORAGE"

        else:

            # If output data def for an output was not supplied in the job, create a dynamic data spec
            # Dynamic data def will always use an embedded schema (this is no ID for an external schema)

            mapped_output_key = output_name
            mapped_storage_key = output_name + ":STORAGE"

            data_id = self._job_config.resultMapping[mapped_output_key]
            storage_id = self._job_config.resultMapping[mapped_storage_key]

            data_spec_id = NodeId.of(f"{output_name}:SPEC", self._job_namespace, _data.DataSpec)
            nodes[data_spec_id] = DynamicDataSpecNode(
                data_spec_id, data_view_id,
                data_id, storage_id,
                prior_data_spec=None,
                explicit_deps=explicit_deps)

            # Create a physical save operation for the data item
            data_save_id = NodeId.of(f"{output_name}:SAVE", self._job_namespace, _data.DataSpec)
            nodes[data_save_id] = SaveDataNode(data_save_id, data_item_id, spec_id=data_spec_id)

            output_key = _util.object_key(data_id)
            storage_key = _util.object_key(storage_id)

        data_result_id = NodeId.of(f"{output_name}:RESULT", self._job_namespace, ObjectBundle)
        nodes[data_result_id] = DataResultNode(
            data_result_id, output_name, data_save_id,
            data_key=output_key,
            storage_key=storage_key)

    def _build_file_output(self, output_name, output_def, output_selector, file_view_id, nodes, explicit_deps):

        mapped_output_key = output_name
        mapped_storage_key = output_name + ":STORAGE"

        file_obj = _util.get_job_resource(output_selector, self._job_config, optional=True)

        if file_obj is not None:

            # Definitions already exist (generated by dev mode translator)

            file_def = _util.get_job_resource(output_selector, self._job_config).file
            storage_def = _util.get_job_resource(file_def.storageId, self._job_config).storage

            resolved_output_key = mapped_output_key
            resolved_storage_key = mapped_storage_key

        else:

            # Create new definitions (default behavior for jobs sent from the platform)

            output_id = self._job_config.resultMapping[mapped_output_key]
            storage_id = self._job_config.resultMapping[mapped_storage_key]

            file_type = output_def.fileType
            timestamp = _dt.datetime.fromisoformat(output_id.objectTimestamp.isoDatetime)
            data_item = f"file/{output_id.objectId}/version-{output_id.objectVersion}"
            storage_key = self._sys_config.storage.defaultBucket
            storage_path = f"file/FILE-{output_id.objectId}/version-{output_id.objectVersion}/{output_name}.{file_type.extension}"

            file_def = self.build_file_def(output_name, file_type, storage_id, data_item)
            storage_def = self.build_storage_def(data_item, storage_key, storage_path, file_type.mimeType, timestamp)

            resolved_output_key = _util.object_key(output_id)
            resolved_storage_key = _util.object_key(storage_id)

        # Required object defs are available, now build the graph nodes

        file_item_id = NodeId(f"{output_name}:ITEM", self._job_namespace, _data.DataItem)
        nodes[file_item_id] = DataItemNode(file_item_id, file_view_id, explicit_deps=explicit_deps)

        file_spec = _data.DataSpec.create_file_spec(file_def.dataItem, file_def, storage_def)
        file_save_id = NodeId.of(f"{output_name}:SAVE", self._job_namespace, _data.DataSpec)
        nodes[file_save_id] = SaveDataNode(file_save_id, file_item_id, spec=file_spec)

        data_result_id = NodeId.of(f"{output_name}:RESULT", self._job_namespace, ObjectBundle)
        nodes[data_result_id] = DataResultNode(
            data_result_id, output_name, file_save_id,
            file_key=resolved_output_key,
            storage_key=resolved_storage_key)

    @classmethod
    def build_output_file_and_storage(cls, output_key, file_type: meta.FileType, sys_config: cfg.RuntimeConfig, job_config: cfg.JobConfig):

        # TODO: Review and de-dupe building of output metadata
        # Responsibility for assigning outputs could perhaps move from orchestrator to runtime

        output_storage_key = f"{output_key}:STORAGE"

        output_id = job_config.resultMapping[output_key]
        output_storage_id = job_config.resultMapping[output_storage_key]

        timestamp = _dt.datetime.fromisoformat(output_id.objectTimestamp.isoDatetime)
        data_item = f"file/{output_id.objectId}/version-{output_id.objectVersion}"
        storage_key = sys_config.storage.defaultBucket
        storage_path = f"file/FILE-{output_id.objectId}/version-{output_id.objectVersion}/{output_key}.{file_type.extension}"

        file_def = cls.build_file_def(output_key, file_type, output_storage_id, data_item)
        storage_def = cls.build_storage_def(data_item, storage_key, storage_path, file_type.mimeType, timestamp)

        return file_def, storage_def

    @classmethod
    def build_runtime_outputs(cls, output_names: tp.List[str], job_namespace: NodeNamespace):

        # This method is called dynamically during job execution
        # So it cannot use stateful information like self._job_config or self._job_namespace

        # TODO: Factor out common logic with regular job outputs (including static / dynamic)

        nodes = {}
        inputs = set()
        outputs = list()

        for output_name in output_names:

            # Output data view must already exist in the namespace
            data_view_id = NodeId.of(output_name, job_namespace, _data.DataView)
            data_spec_id = NodeId.of(f"{output_name}:SPEC", job_namespace, _data.DataSpec)

            mapped_output_key = output_name
            mapped_storage_key = output_name + ":STORAGE"

            data_id = _util.new_object_id(meta.ObjectType.DATA)
            storage_id = _util.new_object_id(meta.ObjectType.STORAGE)

            data_spec_node = DynamicDataSpecNode(
                data_spec_id, data_view_id,
                data_id, storage_id,
                prior_data_spec=None)

            output_key = _util.object_key(data_id)
            storage_key = _util.object_key(storage_id)

            # Map one data item from each view, since outputs are single part/delta
            data_item_id = NodeId(f"{output_name}:ITEM", job_namespace, _data.DataItem)
            data_item_node = DataItemNode(data_item_id, data_view_id)

            # Create a physical save operation for the data item
            data_save_id = NodeId.of(f"{output_name}:SAVE", job_namespace, _data.DataSpec)
            data_save_node = SaveDataNode(data_save_id, data_item_id, spec_id=data_spec_id)

            data_result_id = NodeId.of(f"{output_name}:RESULT", job_namespace, ObjectBundle)
            data_result_node = DataResultNode(
                data_result_id, output_name, data_save_id,
                output_key, storage_key)

            nodes[data_spec_id] = data_spec_node
            nodes[data_item_id] = data_item_node
            nodes[data_save_id] = data_save_node
            nodes[data_result_id] = data_result_node

            # Job-level data view is an input to the save operation
            inputs.add(data_view_id)
            outputs.append(data_result_id)

        runtime_outputs = JobOutputs(bundles=outputs)
        runtime_outputs_id = NodeId.of("trac_runtime_outputs", job_namespace, JobOutputs)
        runtime_outputs_node = RuntimeOutputsNode(runtime_outputs_id, runtime_outputs)

        nodes[runtime_outputs_id] = runtime_outputs_node

        return GraphSection(nodes, inputs=inputs, outputs={runtime_outputs_id})

    @classmethod
    def build_file_def(cls, file_name, file_type, storage_id, data_item):

        file_def = meta.FileDefinition()
        file_def.name = f"{file_name}.{file_type.extension}"
        file_def.extension = file_type.extension
        file_def.mimeType = file_type.mimeType
        file_def.storageId = _util.selector_for_latest(storage_id)
        file_def.dataItem = data_item
        file_def.size = 0

        return file_def

    @classmethod
    def build_storage_def(
            cls, data_item: str,
            storage_key, storage_path, storage_format,
            timestamp: _dt.datetime):

        first_incarnation = 0

        storage_copy = meta.StorageCopy(
            storage_key, storage_path, storage_format,
            copyStatus=meta.CopyStatus.COPY_AVAILABLE,
            copyTimestamp=meta.DatetimeValue(timestamp.isoformat()))

        storage_incarnation = meta.StorageIncarnation(
            [storage_copy],
            incarnationIndex=first_incarnation,
            incarnationTimestamp=meta.DatetimeValue(timestamp.isoformat()),
            incarnationStatus=meta.IncarnationStatus.INCARNATION_AVAILABLE)

        storage_item = meta.StorageItem([storage_incarnation])

        storage_def = meta.StorageDefinition()
        storage_def.dataItems[data_item] = storage_item

        return storage_def

    def build_job_results(
            self,
            objects: tp.Dict[str, NodeId[meta.ObjectDefinition]] = None,
            bundles: tp.List[NodeId[ObjectBundle]] = None,
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        result_id = self._job_config.resultMapping.get("trac_job_result")
        result_node_id = NodeId.of("trac_job_result", self._job_namespace, cfg.JobResult)

        if objects is not None:

            results_inputs = set(objects.values())

            build_result_node = BuildJobResultNode(
                result_node_id, result_id, self._job_config.jobId,
                outputs=JobOutputs(objects=objects),
                explicit_deps=explicit_deps)

        elif bundles is not None:

            results_inputs = set(bundles)

            build_result_node = BuildJobResultNode(
                result_node_id, result_id, self._job_config.jobId,
                outputs=JobOutputs(bundles=bundles),
                explicit_deps=explicit_deps)

        else:
            raise _ex.EUnexpected()

        result_nodes = {result_node_id: build_result_node}

        return GraphSection(result_nodes, inputs=results_inputs, must_run=[result_node_id])

    def build_model_or_flow_with_context(
            self, namespace: NodeNamespace, model_or_flow_name: str,
            job_def: meta.JobDefinition, model_or_flow: meta.ObjectDefinition,
            input_mapping: tp.Dict[str, NodeId], output_mapping: tp.Dict[str, NodeId],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
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
            job_def: meta.JobDefinition,
            model_or_flow: meta.ObjectDefinition,
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        if model_or_flow.objectType == meta.ObjectType.MODEL:
            return self.build_model(namespace, job_def, model_or_flow.model, explicit_deps)

        elif model_or_flow.objectType == meta.ObjectType.FLOW:
            return self.build_flow(namespace, job_def, model_or_flow.flow)

        else:
            message = f"Invalid job config, expected model or flow, got [{model_or_flow.objectType}]"
            self._error(_ex.EJobValidation(message))

    def build_model(
            self, namespace: NodeNamespace,
            job_def: meta.JobDefinition,
            model_def: meta.ModelDefinition,
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        self.check_model_type(job_def, model_def)

        def param_id(node_name):
            return NodeId(node_name, namespace, meta.Value)

        def data_id(node_name):
            return NodeId(node_name, namespace, _data.DataView)

        # Input data should already be mapped to named inputs in the model context
        parameter_ids = set(map(param_id, model_def.parameters))
        input_ids = set(map(data_id, model_def.inputs))
        output_ids = set(map(data_id, model_def.outputs))

        # Set up storage access for import / export data jobs
        if job_def.jobType == meta.JobType.IMPORT_DATA:
            storage_access = job_def.importData.storageAccess
        elif job_def.jobType == meta.JobType.EXPORT_DATA:
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

        model_node = RunModelNode(
            model_id, model_scope, model_def,
            frozenset(parameter_ids), frozenset(input_ids),
            explicit_deps=explicit_deps, bundle=model_id.namespace,
            storage_access=storage_access)

        model_result_id = NodeId(f"{model_name}:RESULT", namespace)
        model_result_node = RunModelResultNode(model_result_id, model_id)

        nodes = {model_id: model_node, model_result_id: model_result_node}

        # Create nodes for each model output
        # The model node itself outputs a bundle (dictionary of named outputs)
        # These need to be mapped to individual nodes in the graph

        # These output mapping nodes are closely tied to the representation of the model itself
        # In the future, we may want models to emit individual outputs before the whole model is complete

        for output_id in output_ids:
            nodes[output_id] = BundleItemNode(output_id, model_id, output_id.name)

        # Assemble a graph to include the model and its outputs
        return GraphSection(nodes, inputs={*parameter_ids, *input_ids}, outputs=output_ids, must_run=[model_result_id])

    def build_flow(
            self, namespace: NodeNamespace,
            job_def: meta.JobDefinition,
            flow_def: meta.FlowDefinition,
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
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
        def is_input(n): return n[1].nodeType in [meta.FlowNodeType.PARAMETER_NODE, meta.FlowNodeType.INPUT_NODE]
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

            if node.nodeType != meta.FlowNodeType.OUTPUT_NODE:

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
            job_def: meta.JobDefinition,
            target_edges: tp.Dict[meta.FlowSocket, meta.FlowEdge],
            node_name: str, node: meta.FlowNode,
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        def socket_key(socket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        def socket_id(node_: str, socket_: str = None, result_type=None):
            socket_name = f"{node_}.{socket_}" if socket_ else node_
            return NodeId(socket_name, namespace, result_type)

        def edge_mapping(node_: str, socket_: str = None, result_type=None):
            socket = socket_key(meta.FlowSocket(node_, socket_))
            edge = target_edges.get(socket)
            # Report missing edges as a job consistency error (this might happen sometimes in dev mode)
            if edge is None:
                self._error(_ex.EJobValidation(f"Inconsistent flow: Socket [{socket}] is not connected"))
            return socket_id(edge.source.node, edge.source.socket, result_type)

        if node.nodeType == meta.FlowNodeType.PARAMETER_NODE:
            return GraphSection({}, inputs={NodeId(node_name, namespace, result_type=meta.Value)})

        if node.nodeType == meta.FlowNodeType.INPUT_NODE:
            return GraphSection({}, inputs={NodeId(node_name, namespace, result_type=_data.DataView)})

        if node.nodeType == meta.FlowNodeType.OUTPUT_NODE:
            target_id = NodeId(node_name, namespace, result_type=_data.DataView)
            source_id = edge_mapping(node_name, None, _data.DataView)
            return GraphSection({target_id: IdentityNode(target_id, source_id)}, outputs={target_id})

        if node.nodeType == meta.FlowNodeType.MODEL_NODE:

            param_mapping = {socket: edge_mapping(node_name, socket, meta.Value) for socket in node.parameters}
            input_mapping = {socket: edge_mapping(node_name, socket, _data.DataView) for socket in node.inputs}
            output_mapping = {socket: socket_id(node_name, socket, _data.DataView) for socket in node.outputs}

            push_mapping = {**input_mapping, **param_mapping}
            pop_mapping = output_mapping

            model_selector = job_def.runFlow.models.get(node_name)
            model_obj = _util.get_job_resource(model_selector, self._job_config)

            # Missing models in the job config is a job consistency error
            if model_obj is None or model_obj.objectType != meta.ObjectType.MODEL:
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

    def check_model_compatibility(
            self, model_selector: meta.TagSelector,
            model_def: meta.ModelDefinition, node_name: str, flow_node: meta.FlowNode):

        model_params = list(sorted(model_def.parameters.keys()))
        model_inputs = list(sorted(model_def.inputs.keys()))
        model_outputs = list(sorted(model_def.outputs.keys()))

        node_params = list(sorted(flow_node.parameters))
        node_inputs = list(sorted(flow_node.inputs))
        node_outputs = list(sorted(flow_node.outputs))

        if model_params != node_params or model_inputs != node_inputs or model_outputs != node_outputs:
            model_key = _util.object_key(model_selector)
            self._error(_ex.EJobValidation(f"Incompatible model for flow node [{node_name}] (Model: [{model_key}])"))

    def check_model_type(self, job_def: meta.JobDefinition, model_def: meta.ModelDefinition):

        if job_def.jobType == meta.JobType.IMPORT_DATA:
            allowed_model_types = [meta.ModelType.DATA_IMPORT_MODEL]
        elif job_def.jobType == meta.JobType.EXPORT_DATA:
            allowed_model_types = [meta.ModelType.DATA_EXPORT_MODEL]
        else:
            allowed_model_types = [meta.ModelType.STANDARD_MODEL]

        if model_def.modelType not in allowed_model_types:
            job_type = job_def.jobType.name
            model_type = model_def.modelType.name
            self._error(_ex.EJobValidation(f"Job type [{job_type}] cannot use model type [{model_type}]"))

    @staticmethod
    def build_context_push(
            namespace: NodeNamespace, input_mapping: tp.Dict[str, NodeId],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        """
        Create a context push operation, all inputs are mapped by name
        """

        push_mapping = {
            NodeId(input_name, namespace, outer_id.result_type): outer_id
            for input_name, outer_id
            in input_mapping.items()}

        push_id = NodeId("trac_ctx_push", namespace, Bundle[tp.Any])
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
            namespace: NodeNamespace, output_mapping: tp.Dict[str, NodeId],
            explicit_deps: tp.Optional[tp.List[NodeId]] = None) \
            -> GraphSection:

        """
        Create a context pop operation, all outputs are mapped by name
        """

        pop_mapping = {
            NodeId(output_name, namespace, outer_id.result_type): outer_id
            for output_name, outer_id
            in output_mapping.items()}

        pop_id = NodeId("trac_ctx_pop", namespace, Bundle[tp.Any])
        pop_node = ContextPopNode(pop_id, namespace, pop_mapping, explicit_deps, bundle=pop_id.namespace.parent)

        nodes = {pop_id: pop_node}

        # Create an explicit marker for each data node popped into the outer context
        for inner_id, outer_id in pop_mapping.items():
            nodes[outer_id] = BundleItemNode(outer_id, pop_id, outer_id.name, explicit_deps=[pop_id])

        return GraphSection(
            nodes,
            inputs={*pop_mapping.keys()},
            outputs={*pop_mapping.values()},
            must_run=[pop_id])

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

    def _invalid_graph_error(self, missing_dependencies: tp.Iterable[NodeId]):

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
