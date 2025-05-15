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

import abc
import copy
import io
import pathlib
import typing as tp

import tracdap.rt.api as _api
import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.exec.context as _ctx
import tracdap.rt._impl.exec.graph_builder as _graph
import tracdap.rt._impl.core.type_system as _types
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.storage as _storage
import tracdap.rt._impl.core.struct as _struct
import tracdap.rt._impl.core.models as _models
import tracdap.rt._impl.core.util as _util

from tracdap.rt._impl.exec.graph import *
from tracdap.rt._impl.exec.graph import _T


class NodeContext:

    """
    A NodeContext is a map of node results available to an individual node during execution.

    Looking up a missing item or an item with the wrong result type will result in a meaningful error.
    The context is immutable and should not be modified by node functions.

    The engine will supply a NodeContext for each invocation of a NodeFunction.
    The NodeContext interface abstracts away the engine structures used to represent execution state
    and should help avoid any temptation for node functions to modify the context directly.

    .. seealso::
        :py:class:`NodeFunction <NodeFunction>`
    """

    @abc.abstractmethod
    def lookup(self, node_id: NodeId[_T]) -> _T:
        pass

    @abc.abstractmethod
    def iter_items(self) -> tp.Iterator[tp.Tuple[NodeId, tp.Any]]:
        pass


class NodeCallback:

    @abc.abstractmethod
    def send_graph_update(self, update: _graph.GraphUpdate):
        pass


# Helper functions to access the node context (in case the NodeContext interface needs to change)

def _ctx_lookup(node_id: NodeId[_T], ctx: NodeContext) -> _T:

    return ctx.lookup(node_id)


def _ctx_iter_items(ctx: NodeContext) -> tp.Iterator[tp.Tuple[NodeId, tp.Any]]:

    return ctx.iter_items()


class NodeFunction(tp.Generic[_T]):

    """
    A NodeFunction is a unit of executable code to evaluate a single node in the execution graph

    Node functions are stateless and are executed with a NodeContext of available results from other nodes
    Each node function returns a single result, which must match the result type of its node ID

    Node functions are intended to be short wrapper functions around core capabilities of the runtime,
    that allows those capabilities to be called in a uniform way by the engine. They should contain
    minimal logic, mostly related to mapping context items. Long node functions include lower-level logic
    are generally a sign there is a missing/incomplete component in the impl package.

    .. seealso::
        :py:class:`Node <_graph.Node>`
        :py:class:`NodeContext <NodeContext>`
    """

    def __init__(self):
        self.node_callback: tp.Optional[NodeCallback] = None

    def __call__(self, ctx: NodeContext, callback: NodeCallback = None) -> _T:
        try:
            self.node_callback = callback
            return self._execute(ctx)
        finally:
            self.node_callback = None

    @abc.abstractmethod
    def _execute(self, ctx: NodeContext) -> _T:
        pass


# ----------------------------------------------------------------------------------------------------------------------
# NODE FUNCTION IMPLEMENTATIONS
# ----------------------------------------------------------------------------------------------------------------------


# STATIC VALUES
# -------------

class NoopFunc(NodeFunction[None]):

    def __init__(self, node: NoopNode):
        super().__init__()
        self.node = node

    def _execute(self, _: NodeContext) -> None:
        return None


class StaticValueFunc(NodeFunction[_T]):

    def __init__(self, node: StaticValueNode[_T]):
        super().__init__()
        self.node = node

    def _execute(self, ctx: NodeContext) -> _T:
        return self.node.value


# MAPPING OPERATIONS
# ------------------

class IdentityFunc(NodeFunction[_T]):

    def __init__(self, node: IdentityNode[_T]):
        super().__init__()
        self.node = node

    def _execute(self, ctx: NodeContext) -> _T:
        return _ctx_lookup(self.node.src_id, ctx)


class KeyedItemFunc(NodeFunction[_T]):

    def __init__(self, node: KeyedItemNode[_T]):
        super().__init__()
        self.node = node

    def _execute(self, ctx: NodeContext) -> _T:
        src_node_result = _ctx_lookup(self.node.src_id, ctx)
        src_item = src_node_result.get(self.node.src_item)
        return src_item


class _ContextPushPopFunc(NodeFunction[Bundle[tp.Any]], abc.ABC):

    # This approach to context push / pop assumes all the nodes to be mapped are already available
    # A better approach would be to map individual items as they become available

    _PUSH = True
    _POP = False

    def __init__(self, node: tp.Union[ContextPushNode, ContextPopNode], direction: bool):
        super().__init__()
        self.node = node
        self.direction = direction

    def _execute(self, ctx: NodeContext) -> Bundle[tp.Any]:

        if len(self.node.mapping) == 0:
            return dict()

        if self.direction == self._PUSH:
            bundle_mapping = {inner_id.name: outer_id for inner_id, outer_id in self.node.mapping.items()}
        else:
            bundle_mapping = {outer_id.name: inner_id for inner_id, outer_id in self.node.mapping.items()}

        bundle: Bundle[tp.Any] = dict()

        for item_name, source_id in bundle_mapping.items():

            bundle_item = _ctx_lookup(source_id, ctx)
            bundle[item_name] = bundle_item

        return bundle


class ContextPushFunc(_ContextPushPopFunc):

    def __init__(self, node: ContextPushNode):
        super(ContextPushFunc, self).__init__(node, self._PUSH)


class ContextPopFunc(_ContextPushPopFunc):

    def __init__(self, node: ContextPopNode):
        super(ContextPopFunc, self).__init__(node, self._POP)


# DATA HANDLING
# -------------

class DataSpecFunc(NodeFunction[_data.DataSpec]):

    def __init__(self, node: DataSpecNode):
        super().__init__()
        self.node = node

    def _execute(self, ctx: NodeContext) -> _data.DataSpec:

        # Get the schema from runtime data
        data_view = _ctx_lookup(self.node.data_view_id, ctx)
        trac_schema = data_view.trac_schema

        # Common logic for building a data spec is part of the data module
        return _data.build_data_spec(
            self.node.data_obj_id, self.node.storage_obj_id,
            self.node.context_key, trac_schema,
            self.node.storage_config,
            self.node.prior_data_spec)


class DataViewFunc(NodeFunction[_data.DataView]):

    def __init__(self, node: DataViewNode):
        super().__init__()
        self.node = node

    def _execute(self, ctx: NodeContext) -> _data.DataView:

        root_item = _ctx_lookup(self.node.root_item, ctx)
        root_part_key = _data.DataPartKey.for_root()

        # Map empty item -> emtpy view (for optional inputs not supplied)
        if root_item.is_empty():
            return _data.DataView.create_empty(root_item.object_type)

        # Handle file data views
        if root_item.object_type == _meta.ObjectType.FILE:
            return _data.DataView.for_file_item(root_item)

        # TODO: Generalize processing across DataView / DataItem types

        if root_item.schema_type == _meta.SchemaType.STRUCT:
            view = _data.DataView.for_trac_schema(self.node.schema)
            view.parts[root_part_key] = [root_item]
            return view

        # Everything else is a regular data view
        if self.node.schema is not None and len(self.node.schema.table.fields) > 0:
            trac_schema = self.node.schema
            data_view = _data.DataView.for_trac_schema(trac_schema)
        else:
            arrow_schema = root_item.schema
            data_view = _data.DataView.for_arrow_schema(arrow_schema)

        data_view = _data.DataMapping.add_item_to_view(data_view, root_part_key, root_item)

        return data_view


class DataItemFunc(NodeFunction[_data.DataItem]):

    def __init__(self, node: DataItemNode):
        super().__init__()
        self.node = node

    def _execute(self, ctx: NodeContext) -> _data.DataItem:

        data_view = _ctx_lookup(self.node.data_view_id, ctx)

        # Map empty view -> emtpy item (for optional outputs not supplied)
        if data_view.is_empty():
            return _data.DataItem.create_empty(data_view.object_type)

        # Handle file data views
        if data_view.object_type == _meta.ObjectType.FILE:
            return data_view.file_item

        # TODO: Support selecting data item described by self.node

        # Selecting data item for part-root, delta=0
        part_key = _data.DataPartKey.for_root()
        part = data_view.parts[part_key]
        delta = part[0]  # selects delta=0

        return delta


class _LoadSaveDataFunc(abc.ABC):

    def __init__(self, storage: _storage.StorageManager):
        self.storage = storage

    @classmethod
    def _choose_data_spec(cls, spec_id, spec, ctx: NodeContext):

        if spec_id is not None:
            return _ctx_lookup(spec_id, ctx)
        elif spec is not None:
            return spec
        else:
            raise _ex.EUnexpected()

    def _choose_copy(self, data_item: str, storage_def: _meta.StorageDefinition) -> _meta.StorageCopy:

        # Metadata should be checked for consistency before a job is accepted
        # An error here indicates a validation gap

        storage_info = storage_def.dataItems.get(data_item)

        if storage_info is None:
            raise _ex.EValidationGap()

        incarnation = next(filter(
            lambda i: i.incarnationStatus == _meta.IncarnationStatus.INCARNATION_AVAILABLE,
            reversed(storage_info.incarnations)), None)

        if incarnation is None:
            raise _ex.EValidationGap()

        copy_ = next(filter(
            lambda c: c.copyStatus == _meta.CopyStatus.COPY_AVAILABLE
                      and self.storage.has_data_storage(c.storageKey),
            incarnation.copies), None)

        if copy_ is None:
            raise _ex.EValidationGap()

        return copy_


class LoadDataFunc( _LoadSaveDataFunc, NodeFunction[_data.DataItem],):

    def __init__(self, node: LoadDataNode, storage: _storage.StorageManager):
        super().__init__(storage)
        self.node = node

    def _execute(self, ctx: NodeContext) -> _data.DataItem:

        data_spec = self._choose_data_spec(self.node.spec_id, self.node.spec, ctx)
        data_copy = self._choose_copy(data_spec.data_item, data_spec.storage)

        if data_spec.object_type == _api.ObjectType.FILE:
            return self._load_file(data_copy)

        elif data_spec.schema_type == _api.SchemaType.TABLE:
            return self._load_table(data_spec, data_copy)

        elif data_spec.schema_type == _api.SchemaType.STRUCT:
            return self._load_struct(data_copy)

        # TODO: Handle dynamic inputs, they should work for any schema type
        elif data_spec.schema_type == _api.SchemaType.SCHEMA_TYPE_NOT_SET:
            return self._load_table(data_spec, data_copy)

        else:
            raise _ex.EUnexpected()

    def _load_file(self, data_copy):

        storage = self.storage.get_file_storage(data_copy.storageKey)
        content = storage.read_bytes(data_copy.storagePath)

        return _data.DataItem.for_file_content(content)

    def _load_table(self, data_spec, data_copy):

        trac_schema = data_spec.schema if data_spec.schema else data_spec.definition.schema
        arrow_schema = _data.DataMapping.trac_to_arrow_schema(trac_schema) if trac_schema else None

        storage_options = dict(
            (opt_key, _types.MetadataCodec.decode_value(opt_value))
            for opt_key, opt_value in data_spec.storage.storageOptions.items())

        storage = self.storage.get_data_storage(data_copy.storageKey)

        table = storage.read_table(
            data_copy.storagePath, data_copy.storageFormat, arrow_schema,
            storage_options=storage_options)

        return _data.DataItem.for_table(table, table.schema, trac_schema)

    def _load_struct(self, data_copy):

        storage = self.storage.get_file_storage(data_copy.storageKey)

        with storage.read_byte_stream(data_copy.storagePath) as stream:
            with io.TextIOWrapper(stream, "utf-8") as text_stream:
                struct = _struct.StructProcessor.load_struct(text_stream, data_copy.storageFormat)

        return _data.DataItem.for_struct(struct)


class SaveDataFunc(_LoadSaveDataFunc, NodeFunction[_data.DataSpec]):

    def __init__(self, node: SaveDataNode, storage: _storage.StorageManager):
        super().__init__(storage)
        self.node = node

    def _execute(self, ctx: NodeContext) -> _data.DataSpec:

        # Item to be saved should exist in the current context
        data_item = _ctx_lookup(self.node.data_item_id, ctx)

        # Metadata already exists as data_spec but may not contain schema, row count, file size etc.
        data_spec = self._choose_data_spec(self.node.spec_id, self.node.spec, ctx)
        data_copy = self._choose_copy(data_spec.data_item, data_spec.storage)

        # Do not save empty outputs (optional outputs that were not produced)
        if data_item.is_empty():
            return _data.DataSpec.create_empty_spec(data_item.object_type, data_item.schema_type)

        if data_item.object_type == _api.ObjectType.FILE:
            return self._save_file(data_item, data_spec, data_copy)

        elif data_item.schema_type == _api.SchemaType.TABLE:
            return self._save_table(data_item, data_spec, data_copy)

        elif data_item.schema_type == _api.SchemaType.STRUCT:
            return self._save_struct(data_item, data_spec, data_copy)

        else:
            raise _ex.EUnexpected()

    def _save_file(self, data_item, data_spec, data_copy):

        if data_item.content is None:
            raise _ex.EUnexpected()

        storage = self.storage.get_file_storage(data_copy.storageKey)
        storage.write_bytes(data_copy.storagePath, data_item.content)

        data_spec = copy.deepcopy(data_spec)
        data_spec.definition.size = len(data_item.content)

        return data_spec

    def _save_table(self, data_item, data_spec, data_copy):

        # Current implementation will always put an Arrow table in the data item
        # Empty tables are allowed, so explicitly check if table is None
        # Testing "if not data_item.table" will fail for empty tables

        if data_item.content is None:
            raise _ex.EUnexpected()

        # Decode options (metadata values) from the storage definition
        options = dict()
        for opt_key, opt_value in data_spec.storage.storageOptions.items():
            options[opt_key] = _types.MetadataCodec.decode_value(opt_value)

        storage = self.storage.get_data_storage(data_copy.storageKey)
        storage.write_table(
            data_copy.storagePath, data_copy.storageFormat,
            data_item.content,
            storage_options=options, overwrite=False)

        data_spec = copy.deepcopy(data_spec)
        # TODO: Save row count in metadata

        if data_spec.definition.schema is None and data_spec.definition.schemaId is None:
            data_spec.definition.schema = _data.DataMapping.arrow_to_trac_schema(data_item.table.schema)

        return data_spec

    def _save_struct(self, data_item, data_spec, data_copy):

        if data_item.content is None:
            raise _ex.EUnexpected()

        struct_data = data_item.content
        storage_format = data_copy.storageFormat

        storage = self.storage.get_file_storage(data_copy.storageKey)

        # Using the text wrapper closes the stream early, which is inefficient in the data layer
        # Supporting text IO directly from the storage API would allow working with text streams more naturally
        with storage.write_byte_stream(data_copy.storagePath) as stream:
            with io.TextIOWrapper(stream, "utf-8") as text_stream:
                _struct.StructProcessor.save_struct(struct_data, text_stream, storage_format)

        data_spec = copy.deepcopy(data_spec)

        if data_spec.definition.schema is None and data_spec.definition.schemaId is None:
            data_spec.definition.schema = data_item.trac_schema

        return data_spec


# MODEL EXECUTION
# ---------------

class ImportModelFunc(NodeFunction[GraphOutput]):

    def __init__(self, node: ImportModelNode, models: _models.ModelLoader):
        super().__init__()
        self.node = node
        self._models = models

    def _execute(self, ctx: NodeContext) -> GraphOutput:

        model_id = self.node.model_id

        model_stub = self._build_model_stub(self.node.import_details)
        model_class = self._models.load_model_class(self.node.import_scope, model_stub)
        model_def = self._models.scan_model(model_stub, model_class)
        model_obj = _meta.ObjectDefinition(_meta.ObjectType.MODEL, model=model_def)

        model_attrs = [
            _meta.TagUpdate(_meta.TagOperation.CREATE_OR_REPLACE_ATTR, attr_name, attr_value)
            for attr_name, attr_value in model_def.staticAttributes.items()]

        return GraphOutput(model_id, model_obj, model_attrs)

    @staticmethod
    def _build_model_stub(import_details: _meta.ImportModelJob):

        return _meta.ModelDefinition(
            language=import_details.language,
            repository=import_details.repository,
            packageGroup=import_details.packageGroup,
            package=import_details.package,
            version=import_details.version,
            entryPoint=import_details.entryPoint,
            path=import_details.path)


class RunModelFunc(NodeFunction[Bundle[_data.DataView]]):

    def __init__(
            self, node: RunModelNode,
            model_class: _api.TracModel.__class__,
            checkout_directory: pathlib.Path,
            storage_manager: _storage.StorageManager,
            log_provider: _logging.LogProvider):

        super().__init__()
        self.node = node
        self.model_class = model_class
        self.checkout_directory = checkout_directory
        self.storage_manager = storage_manager
        self.log_provider = log_provider

    def _execute(self, ctx: NodeContext) -> Bundle[_data.DataView]:

        model_def = self.node.model_def

        # Create a context containing only declared items in the current namespace, addressed by name
        # The engine guarantees all required nodes are present and have type matching their node ID
        # Still, if any nodes are missing or have the wrong type TracContextImpl will raise ERuntimeValidation

        local_ctx = {}
        dynamic_outputs = []

        for node_id, node_result in _ctx_iter_items(ctx):
            if node_id.namespace == self.node.id.namespace:
                if node_id.name in model_def.parameters or node_id.name in model_def.inputs:
                    local_ctx[node_id.name] = node_result

        # Set up access to external storage if required

        storage_map = {}

        if self.node.storage_access:
            write_access = True if self.node.model_def.modelType == _meta.ModelType.DATA_EXPORT_MODEL else False
            for storage_key in self.node.storage_access:
                if self.storage_manager.has_file_storage(storage_key, external=True):
                    storage_impl = self.storage_manager.get_file_storage(storage_key, external=True)
                    storage = _ctx.TracFileStorageImpl(storage_key, storage_impl, write_access, self.checkout_directory, self.log_provider)
                    storage_map[storage_key] = storage
                elif self.storage_manager.has_data_storage(storage_key, external=True):
                    storage_impl = self.storage_manager.get_data_storage(storage_key, external=True)
                    # This is a work-around until the storage extension API can be updated / unified
                    if not isinstance(storage_impl, _storage.IDataStorageBase):
                        raise _ex.EStorageConfig(f"External storage for [{storage_key}] is using the legacy storage framework]")
                    converter = _data.DataConverter.noop()
                    storage = _ctx.TracDataStorageImpl(storage_key, storage_impl, converter, write_access, self.checkout_directory, self.log_provider)
                    storage_map[storage_key] = storage
                else:
                    raise _ex.EStorageConfig(f"External storage is not available: [{storage_key}]")


        # Run the model against the mapped local context

        if model_def.modelType in [_meta.ModelType.DATA_IMPORT_MODEL, _meta.ModelType.DATA_EXPORT_MODEL]:
            trac_ctx = _ctx.TracDataContextImpl(
                self.node.model_def, self.model_class,
                local_ctx, dynamic_outputs, storage_map,
                self.checkout_directory, self.log_provider)
        else:
            trac_ctx = _ctx.TracContextImpl(
                self.node.model_def, self.model_class,
                local_ctx, dynamic_outputs,
                self.checkout_directory, self.log_provider)

        try:
            model = object.__new__(self.model_class)
            model.__init__()
            model.run_model(trac_ctx)
        except _ex.ETrac:
            raise
        except Exception as e:
            details = _util.error_details_from_model_exception(e, self.checkout_directory)
            msg = f"There was an unhandled error in the model: {str(e)}{details}"
            raise _ex.EModelExec(msg) from e

        # Buidl a result bundle for the defined model outputs
        results: Bundle[_data.DataView] = dict()

        for output_name, output_schema in model_def.outputs.items():
            output: _data.DataView = local_ctx.get(output_name)
            if (output is None or output.is_empty()) and not output_schema.optional:
                raise _ex.ERuntimeValidation(f"Missing required output [{output_name}] from model [{self.model_class.__name__}]")
            results[output_name] = output or _data.DataView.create_empty()

        # Add dynamic outputs to the model result bundle
        for output_name in dynamic_outputs:
            output: _data.DataView = local_ctx.get(output_name)
            if output is None or output.is_empty():
                raise _ex.ERuntimeValidation(f"No data provided for [{output_name}] from model [{self.model_class.__name__}]")
            results[output_name] = output

        # Send a graph update to include the dynamic outputs in the job result
        if any(dynamic_outputs):
            builder = _graph.GraphBuilder.dynamic(self.node.graph_context)
            update = builder.build_dynamic_outputs(self.node.id, dynamic_outputs)
            self.node_callback.send_graph_update(update)

        return results


# RESULTS PROCESSING
# ------------------

class JobResultFunc(NodeFunction[_cfg.JobResult]):

    def __init__(self, node: JobResultNode):
        super().__init__()
        self.node = node

    def _execute(self, ctx: NodeContext) -> _cfg.JobResult:

        result_def = _meta.ResultDefinition()
        result_def.jobId = _util.selector_for(self.node.job_id)

        job_result = _cfg.JobResult()
        job_result.jobId = self.node.job_id
        job_result.resultId = self.node.result_id
        job_result.result = result_def

        self._process_named_outputs(self.node.named_outputs, ctx, job_result)
        self._process_unnamed_outputs(self.node.unnamed_outputs, ctx, job_result)

        # TODO: Handle individual failed results

        result_def.statusCode = _meta.JobStatusCode.SUCCEEDED

        return job_result

    def _process_named_outputs(self, named_outputs, ctx: NodeContext, job_result: _cfg.JobResult):

        for output_name, output_id in named_outputs.items():

            output = _ctx_lookup(output_id, ctx)

            if output_id.result_type == GraphOutput:
                self._process_graph_output(output_name, output, job_result)

            elif output_id.result_type == _data.DataSpec:
                self._process_data_spec(output_name, output, job_result)

            else:
                raise _ex.EUnexpected()

    def _process_unnamed_outputs(self, unnamed_outputs, ctx: NodeContext, job_result: _cfg.JobResult):

        for output_id in unnamed_outputs:

            output = _ctx_lookup(output_id, ctx)

            if output_id.result_type == GraphOutput:
                self._process_graph_output(None, output, job_result)

            elif output_id.result_type == _data.DataSpec:
                self._process_data_spec(None, output, job_result)

            else:
                raise _ex.EUnexpected()

    @staticmethod
    def _process_graph_output(output_name: tp.Optional[str], output: GraphOutput, job_result: _cfg.JobResult):

        output_key = _util.object_key(output.objectId)

        job_result.objectIds.append(output.objectId)
        job_result.objects[output_key] = output.definition

        if output.attrs is not None:
            job_result.attrs[output_key] = _cfg.JobResultAttrs(output.attrs)

        if output_name is not None:
            job_result.result.outputs[output_name] = _util.selector_for(output.objectId)

    @staticmethod
    def _process_data_spec(output_name: tp.Optional[str], data_spec: _data.DataSpec, job_result: _cfg.JobResult):

        # Do not record results for optional outputs that were not produced
        if data_spec.is_empty():
            return

        output_id = data_spec.primary_id
        output_key = _util.object_key(output_id)
        output_def = data_spec.definition

        if data_spec.object_type == _meta.ObjectType.DATA:
            output_obj = _meta.ObjectDefinition(data_spec.object_type, data=output_def)
        elif data_spec.object_type == _meta.ObjectType.FILE:
            output_obj = _meta.ObjectDefinition(data_spec.object_type, file=output_def)
        else:
            raise _ex.EUnexpected()

        storage_id = data_spec.storage_id
        storage_key = _util.object_key(storage_id)
        storage_def = data_spec.storage
        storage_obj = _meta.ObjectDefinition(objectType=_meta.ObjectType.STORAGE, storage=storage_def)

        job_result.objectIds.append(output_id)
        job_result.objectIds.append(storage_id)
        job_result.objects[output_key] = output_obj
        job_result.objects[storage_key] = storage_obj

        # Currently, jobs do not ever produce external schemas

        if output_name is not None:
            job_result.result.outputs[output_name] = _util.selector_for(output_id)


class DynamicOutputsFunc(NodeFunction[DynamicOutputsNode]):

    def __init__(self, node: DynamicOutputsNode):
        super().__init__()
        self.node = node

    def _execute(self, ctx: NodeContext) -> DynamicOutputsNode:
        return self.node


# MISC NODE TYPES
# ---------------


class ChildJobFunc(NodeFunction[None]):

    def __init__(self, node: ChildJobNode):
        super().__init__()
        self.node = node

    def _execute(self, ctx: NodeContext):
        # This node should never execute, the engine intercepts child job nodes and provides special handling
        raise _ex.ETracInternal("Child job was not processed correctly (this is a bug)")



# ----------------------------------------------------------------------------------------------------------------------
# FUNCTION RESOLUTION
# ----------------------------------------------------------------------------------------------------------------------


class FunctionResolver:

    """
    The function resolver maps graph nodes (data-only representations) to executable functions

    Most functions can be resolved with just the graph node (these are the "basic nodes")
    Some functions need other resources, such as access to data or models
    These resources are available in the context of a job,
    so a FunctionResolve instances is constructed per job and initialised with the job resources

    .. seealso::
        :py:class:`Node <tracdap.rt.exec.graph.Node>`,
        :py:class:`NodeFunction <NodeFunction>`
    """

    # TODO: Validate consistency for resource keys
    # Storage key should be validated for load data, save data and run model with storage access
    # Repository key should be validated for import model (and explicitly for run model)

    # Currently jobs with missing resources will fail at runtime, with a suitable error
    # The resolver is called during graph building
    # Putting the check here will raise a consistency error before the job starts processing

    __ResolveFunc = tp.Callable[['FunctionResolver', Node[_T]], NodeFunction[_T]]

    def __init__(self, models: _models.ModelLoader, storage: _storage.StorageManager, log_provider: _logging.LogProvider):
        self._models = models
        self._storage = storage
        self._log_provider = log_provider

    def resolve_node(self, node: Node[_T]) -> NodeFunction[_T]:

        basic_node_class = self.__basic_node_mapping.get(node.__class__)

        if basic_node_class:
            return basic_node_class(node)

        resolve_func = self.__node_mapping[node.__class__]

        if resolve_func is None:
            raise _ex.EUnexpected()

        return resolve_func(self, node)

    def resolve_load_data(self, node: LoadDataNode):
        return LoadDataFunc(node, self._storage)

    def resolve_save_data(self, node: SaveDataNode):
        return SaveDataFunc(node, self._storage)

    def resolve_import_model_node(self, node: ImportModelNode):
        return ImportModelFunc(node, self._models)

    def resolve_run_model_node(self, node: RunModelNode) -> NodeFunction:

        # TODO: Verify model_class against model_def

        model_class = self._models.load_model_class(node.model_scope, node.model_def)
        checkout_directory = self._models.model_load_checkout_directory(node.model_scope, node.model_def)
        storage_manager = self._storage if node.storage_access else None

        return RunModelFunc(node, model_class, checkout_directory, storage_manager, self._log_provider)

    __basic_node_mapping: tp.Dict[Node.__class__, NodeFunction.__class__] = {

        NoopNode: NoopFunc,
        StaticValueNode: StaticValueFunc,
        IdentityNode: IdentityFunc,
        KeyedItemNode: KeyedItemFunc,
        ContextPushNode: ContextPushFunc,
        ContextPopNode: ContextPopFunc,
        DataSpecNode: DataSpecFunc,
        DataViewNode: DataViewFunc,
        DataItemNode: DataItemFunc,
        JobResultNode: JobResultFunc,
        DynamicOutputsNode: DynamicOutputsFunc,
        ChildJobNode: ChildJobFunc,
        BundleItemNode: NoopFunc,
    }

    __node_mapping: tp.Dict[Node.__class__, __ResolveFunc] = {

        LoadDataNode: resolve_load_data,
        SaveDataNode: resolve_save_data,
        RunModelNode: resolve_run_model_node,
        ImportModelNode: resolve_import_model_node
    }
