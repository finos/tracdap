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

import datetime
import abc
import random
import dataclasses as dc  # noqa

import tracdap.rt.api as _api
import tracdap.rt.config as _config
import tracdap.rt.exceptions as _ex
import tracdap.rt._exec.context as _ctx
import tracdap.rt._impl.config_parser as _cfg_p  # noqa
import tracdap.rt._impl.type_system as _types  # noqa
import tracdap.rt._impl.data as _data  # noqa
import tracdap.rt._impl.storage as _storage  # noqa
import tracdap.rt._impl.models as _models  # noqa
import tracdap.rt._impl.util as _util  # noqa

from tracdap.rt._exec.graph import *
from tracdap.rt._exec.graph import _T


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

    def __call__(self, ctx: NodeContext) -> _T:
        return self._execute(ctx)

    @abc.abstractmethod
    def _execute(self, ctx: NodeContext) -> _T:
        pass


# ----------------------------------------------------------------------------------------------------------------------
# NODE FUNCTION IMPLEMENTATIONS
# ----------------------------------------------------------------------------------------------------------------------


class NoopFunc(NodeFunction[None]):

    def __init__(self, node: NoopNode):
        self.node = node

    def _execute(self, _: NodeContext) -> None:
        return None


class StaticValueFunc(NodeFunction[_T]):

    def __init__(self, node: StaticValueNode[_T]):
        self.node = node

    def _execute(self, ctx: NodeContext) -> _T:
        return self.node.value


class IdentityFunc(NodeFunction[_T]):

    def __init__(self, node: IdentityNode[_T]):
        self.node = node

    def _execute(self, ctx: NodeContext) -> _T:
        return _ctx_lookup(self.node.src_id, ctx)


class _ContextPushPopFunc(NodeFunction[Bundle[tp.Any]], abc.ABC):

    # This approach to context push / pop assumes all the nodes to be mapped are already available
    # A better approach would be to map individual items as they become available

    _PUSH = True
    _POP = False

    def __init__(self, node: tp.Union[ContextPushNode, ContextPopNode], direction: bool):
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


class KeyedItemFunc(NodeFunction[_T]):

    def __init__(self, node: KeyedItemNode[_T]):
        self.node = node

    def _execute(self, ctx: NodeContext) -> _T:
        src_node_result = _ctx_lookup(self.node.src_id, ctx)
        src_item = src_node_result.get(self.node.src_item)
        return src_item


class BuildJobResultFunc(NodeFunction[_config.JobResult]):

    def __init__(self, node: BuildJobResultNode):
        self.node = node

    def _execute(self, ctx: NodeContext) -> _config.JobResult:

        job_result = _config.JobResult()
        job_result.jobId = self.node.job_id
        job_result.statusCode = meta.JobStatusCode.SUCCEEDED

        # TODO: Handle individual failed results

        for obj_id, node_id in self.node.objects.items():
            obj_def = _ctx_lookup(node_id, ctx)
            job_result.results[obj_id] = obj_def

        for bundle_id in self.node.bundles:
            bundle = _ctx_lookup(bundle_id, ctx)
            job_result.results.update(bundle.items())

        return job_result


class SaveJobResultFunc(NodeFunction[None]):

    def __init__(self, node: SaveJobResultNode):
        self.node = node

    def _execute(self, ctx: NodeContext) -> None:

        job_result = _ctx_lookup(self.node.job_result_id, ctx)

        if not self.node.result_spec.save_result:
            return None

        job_result_format = self.node.result_spec.result_format
        job_result_str = _cfg_p.ConfigQuoter.quote(job_result, job_result_format)
        job_result_bytes = bytes(job_result_str, "utf-8")

        job_key = _util.object_key(job_result.jobId)
        job_result_file = f"job_result_{job_key}.{self.node.result_spec.result_format}"
        job_result_path = pathlib \
            .Path(self.node.result_spec.result_dir) \
            .joinpath(job_result_file)

        _util.logger_for_object(self).info(f"Saving job result to [{job_result_path}]")

        with open(job_result_path, "xb") as result_stream:
            result_stream.write(job_result_bytes)

        return None


class DataViewFunc(NodeFunction[_data.DataView]):

    def __init__(self, node: DataViewNode):
        self.node = node

    def _execute(self, ctx: NodeContext) -> _data.DataView:

        root_item = _ctx_lookup(self.node.root_item, ctx)
        root_part_key = _data.DataPartKey.for_root()

        data_view = _data.DataView.for_trac_schema(self.node.schema)
        data_view = _data.DataMapping.add_item_to_view(data_view, root_part_key, root_item)

        return data_view


class DataItemFunc(NodeFunction[_data.DataItem]):

    def __init__(self, node: DataItemNode):
        self.node = node

    def _execute(self, ctx: NodeContext) -> _data.DataItem:

        data_view = _ctx_lookup(self.node.data_view_id, ctx)

        # TODO: Support selecting data item described by self.node

        # Selecting data item for part-root, delta=0
        part_key = _data.DataPartKey.for_root()
        part = data_view.parts[part_key]
        delta = part[0]  # selects delta=0

        return delta


class DataResultFunc(NodeFunction[ObjectBundle]):

    def __init__(self, node: DataResultNode):
        self.node = node

    def _execute(self, ctx: NodeContext) -> ObjectBundle:

        data_spec = _ctx_lookup(self.node.data_spec_id, ctx)

        # TODO: Check result of save operation
        # save_result = _ctx_lookup(self.node.data_save_id, ctx)

        data_result = meta.ObjectDefinition(objectType=meta.ObjectType.DATA, data=data_spec.data_def)
        storage_result = meta.ObjectDefinition(objectType=meta.ObjectType.STORAGE, storage=data_spec.storage_def)

        bundle = {
            self.node.data_key: data_result,
            self.node.storage_key: storage_result}

        return bundle


class DynamicDataSpecFunc(NodeFunction[_data.DataSpec]):

    DATA_ITEM_TEMPLATE = "data/{}/{}/{}/snap-{:d}/delta-{:d}"
    STORAGE_PATH_TEMPLATE = "data/{}/{}/{}/snap-{:d}/delta-{:d}-x{:0>6x}"

    RANDOM = random.Random()
    RANDOM.seed()

    def __init__(self, node: DynamicDataSpecNode, storage: _storage.StorageManager):
        self.node = node
        self.storage = storage

    def _execute(self, ctx: NodeContext) -> _data.DataSpec:

        # When data def for an output was not supplied in the job, this function creates a dynamic data spec

        if self.node.prior_data_spec is not None:
            raise _ex.ETracInternal("Data updates not supported yet")

        data_view = _ctx_lookup(self.node.data_view_id, ctx)

        data_id = self.node.data_obj_id
        storage_id = self.node.storage_obj_id

        # TODO: pass the object timestamp in from somewhere

        # Note that datetime.utcnow() creates a datetime with no zone
        # datetime.now(utc) creates a datetime with an explicit UTC zone
        # The latter is more precise, also missing zones are rejected by validation
        # (lenient validation might infer the zone, this should be limited to front-facing APIs)

        object_timestamp = datetime.datetime.now(datetime.timezone.utc)

        part_key = meta.PartKey("part-root", meta.PartType.PART_ROOT)
        snap_index = 0
        delta_index = 0

        data_type = data_view.trac_schema.schemaType.name.lower()

        data_item = self.DATA_ITEM_TEMPLATE.format(
            data_type, data_id.objectId,
            part_key.opaqueKey, snap_index, delta_index)

        delta = meta.DataDefinition.Delta(delta_index, data_item)
        snap = meta.DataDefinition.Snap(snap_index, [delta])
        part = meta.DataDefinition.Part(part_key, snap)

        data_def = meta.DataDefinition()
        data_def.storageId = _util.selector_for_latest(storage_id)
        data_def.schema = data_view.trac_schema
        data_def.parts[part_key.opaqueKey] = part

        storage_key = self.storage.default_storage_key()
        storage_format = self.storage.default_storage_format()
        storage_suffix_bytes = random.randint(0, 1 << 24)

        storage_path = self.DATA_ITEM_TEMPLATE.format(
            data_type, data_id.objectId,
            part_key.opaqueKey, snap_index, delta_index,
            storage_suffix_bytes)

        storage_copy = meta.StorageCopy(
            storage_key, storage_path, storage_format,
            copyStatus=meta.CopyStatus.COPY_AVAILABLE,
            copyTimestamp=meta.DatetimeValue(object_timestamp.isoformat()))

        storage_incarnation = meta.StorageIncarnation(
            [storage_copy],
            incarnationIndex=0,
            incarnationTimestamp=meta.DatetimeValue(object_timestamp.isoformat()),
            incarnationStatus=meta.IncarnationStatus.INCARNATION_AVAILABLE)

        storage_item = meta.StorageItem([storage_incarnation])

        storage_def = meta.StorageDefinition()
        storage_def.dataItems[data_item] = storage_item

        # Dynamic data def will always use an embedded schema (this is no ID for an external schema)

        return _data.DataSpec(
            data_item,
            data_def,
            storage_def,
            schema_def=None)


class _LoadSaveDataFunc(abc.ABC):

    def __init__(self, storage: _storage.StorageManager):
        self.storage = storage

    def _choose_copy(self, data_item: str, storage_def: meta.StorageDefinition) -> meta.StorageCopy:

        # Metadata should be checked for consistency before a job is accepted
        # An error here indicates a validation gap

        storage_info = storage_def.dataItems.get(data_item)

        if storage_info is None:
            raise _ex.EValidationGap()

        incarnation = next(filter(
            lambda i: i.incarnationStatus == meta.IncarnationStatus.INCARNATION_AVAILABLE,
            reversed(storage_info.incarnations)), None)

        if incarnation is None:
            raise _ex.EValidationGap()

        copy_ = next(filter(
            lambda c: c.copyStatus == meta.CopyStatus.COPY_AVAILABLE
            and self.storage.has_data_storage(c.storageKey),
            incarnation.copies), None)

        if copy_ is None:
            raise _ex.EValidationGap()

        return copy_


class LoadDataFunc(NodeFunction[_data.DataItem], _LoadSaveDataFunc):

    def __init__(self, node: LoadDataNode, storage: _storage.StorageManager):
        super().__init__(storage)
        self.node = node

    def _execute(self, ctx: NodeContext) -> _data.DataItem:

        data_spec = _ctx_lookup(self.node.spec_id, ctx)
        data_copy = self._choose_copy(data_spec.data_item, data_spec.storage_def)
        data_storage = self.storage.get_data_storage(data_copy.storageKey)

        trac_schema = data_spec.schema_def if data_spec.schema_def else data_spec.data_def.schema
        arrow_schema = _data.DataMapping.trac_to_arrow_schema(trac_schema) if trac_schema else None

        # Decode options (metadata values) from the storage definition
        options = dict()
        for opt_key, opt_value in data_spec.storage_def.storageOptions.items():
            options[opt_key] = _types.MetadataCodec.decode_value(opt_value)

        table = data_storage.read_table(
            data_copy.storagePath,
            data_copy.storageFormat,
            arrow_schema,
            storage_options=options)

        return _data.DataItem(table.schema, table)


class SaveDataFunc(NodeFunction[None], _LoadSaveDataFunc):

    def __init__(self, node: SaveDataNode, storage: _storage.StorageManager):
        super().__init__(storage)
        self.node = node

    def _execute(self, ctx: NodeContext):

        # This function assumes that metadata has already been generated as the data_spec
        # i.e. it is already known which incarnation / copy of the data will be created

        data_spec = _ctx_lookup(self.node.spec_id, ctx)
        data_copy = self._choose_copy(data_spec.data_item, data_spec.storage_def)
        data_storage = self.storage.get_data_storage(data_copy.storageKey)

        # Item to be saved should exist in the current context
        data_item = _ctx_lookup(self.node.data_item_id, ctx)

        # Current implementation will always put an Arrow table in the data item
        # Empty tables are allowed, so explicitly check if table is None
        # Testing "if not data_item.table" will fail for empty tables

        if data_item.table is None:
            raise _ex.EUnexpected()

        # Decode options (metadata values) from the storage definition
        options = dict()
        for opt_key, opt_value in data_spec.storage_def.storageOptions.items():
            options[opt_key] = _types.MetadataCodec.decode_value(opt_value)

        data_storage.write_table(
            data_copy.storagePath, data_copy.storageFormat,
            data_item.table,
            storage_options=options, overwrite=False)


def _model_def_for_import(import_details: meta.ImportModelJob):

    return meta.ModelDefinition(
        language=import_details.language,
        repository=import_details.repository,
        packageGroup=import_details.packageGroup,
        package=import_details.package,
        version=import_details.version,
        entryPoint=import_details.entryPoint,
        path=import_details.path)


class ImportModelFunc(NodeFunction[meta.ObjectDefinition]):

    def __init__(self, node: ImportModelNode, models: _models.ModelLoader):
        self.node = node
        self._models = models

        self._log = _util.logger_for_object(self)

    def _execute(self, ctx: NodeContext) -> meta.ObjectDefinition:

        model_stub = _model_def_for_import(self.node.import_details)

        model_class = self._models.load_model_class(self.node.model_scope, model_stub)
        model_def = self._models.scan_model(model_stub, model_class)

        return meta.ObjectDefinition(meta.ObjectType.MODEL, model=model_def)


class RunModelFunc(NodeFunction[Bundle[_data.DataView]]):

    def __init__(self, node: RunModelNode, model_class: _api.TracModel.__class__):
        super().__init__()
        self.node = node
        self.model_class = model_class

    def _execute(self, ctx: NodeContext) -> Bundle[_data.DataView]:

        model_def = self.node.model_def

        # Create a context containing only declared items in the current namespace, addressed by name
        # The engine guarantees all required nodes are present and have type matching their node ID
        # Still, if any nodes are missing or have the wrong type TracContextImpl will raise ERuntimeValidation

        local_ctx = {}
        static_schemas = {}

        for node_id, node_result in _ctx_iter_items(ctx):

            if node_id.namespace != self.node.id.namespace:
                continue

            if node_id.name in model_def.parameters:
                param_name = node_id.name
                local_ctx[param_name] = node_result

            if node_id.name in model_def.inputs:
                input_name = node_id.name
                local_ctx[input_name] = node_result
                # At the moment, all model inputs have static schemas
                static_schemas[input_name] = model_def.inputs[input_name].schema

        # Add empty data views to the local context to hold model outputs
        # Assuming outputs are all defined with static schemas

        for output_name in model_def.outputs:
            output_schema = self.node.model_def.outputs[output_name].schema
            empty_data_view = _data.DataView.for_trac_schema(output_schema)
            local_ctx[output_name] = empty_data_view
            # At the moment, all model outputs have static schemas
            static_schemas[output_name] = output_schema

        # Run the model against the mapped local context

        trac_ctx = _ctx.TracContextImpl(self.node.model_def, self.model_class, local_ctx, static_schemas)

        try:
            model = self.model_class()
            model.run_model(trac_ctx)
        except _ex.ETrac:
            raise
        except Exception as e:
            msg = f"There was an unhandled error in the model: {str(e)}"
            raise _ex.EModelExec(msg) from e

        # The node result is just the model outputs taken from the local context
        model_outputs: Bundle[_data.DataView] = {
            name: obj for name, obj in local_ctx.items()
            if name in self.node.model_def.outputs}

        return model_outputs


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

    __ResolveFunc = tp.Callable[['FunctionResolver', Node[_T]], NodeFunction[_T]]

    def __init__(self, models: _models.ModelLoader, storage: _storage.StorageManager):
        self._models = models
        self._storage = storage

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

    def resolve_dynamic_data_spec(self, node: DynamicDataSpecNode):
        return DynamicDataSpecFunc(node, self._storage)

    def resolve_import_model_node(self, node: ImportModelNode):
        return ImportModelFunc(node, self._models)

    def resolve_run_model_node(self, node: RunModelNode) -> NodeFunction:

        model_class = self._models.load_model_class(node.model_scope, node.model_def)

        # TODO: Verify model_class against model_def

        return RunModelFunc(node, model_class)

    __basic_node_mapping: tp.Dict[Node.__class__, NodeFunction.__class__] = {

        ContextPushNode: ContextPushFunc,
        ContextPopNode: ContextPopFunc,
        IdentityNode: IdentityFunc,
        KeyedItemNode: KeyedItemFunc,
        DataViewNode: DataViewFunc,
        DataItemNode: DataItemFunc,
        BuildJobResultNode: BuildJobResultFunc,
        SaveJobResultNode: SaveJobResultFunc,
        DataResultNode: DataResultFunc,
        StaticValueNode: StaticValueFunc,
        BundleItemNode: NoopFunc,
        NoopNode: NoopFunc
    }

    __node_mapping: tp.Dict[Node.__class__, __ResolveFunc] = {

        LoadDataNode: resolve_load_data,
        SaveDataNode: resolve_save_data,
        DynamicDataSpecNode: resolve_dynamic_data_spec,
        RunModelNode: resolve_run_model_node,
        ImportModelNode: resolve_import_model_node
    }
