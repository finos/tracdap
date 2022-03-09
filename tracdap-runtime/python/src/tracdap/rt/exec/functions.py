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

from __future__ import annotations

import copy
import datetime
import enum
import abc
import json
import random

import yaml
import uuid

import tracdap.rt.api as _api
import tracdap.rt.config as _config
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.models as _models
import tracdap.rt.impl.storage as _storage
import tracdap.rt.impl.data as _data
import tracdap.rt.impl.util as _util
import tracdap.rt.impl.type_system as _types

import tracdap.rt.exec.context as _ctx
from tracdap.rt.exec.graph import *


NodeContext = tp.Dict[NodeId, object]  # Available prior node results when a node function is called
NodeResult = tp.Any  # Result of a node function (will be recorded against the node ID)

_T = tp.TypeVar('_T')


def _ctx_lookup(node_id: NodeId[_T], ctx: NodeContext) -> _T:
    # TODO: Error handling and type checking
    return ctx.get(node_id).result  # noqa


class NodeFunction(tp.Generic[_T]):

    @abc.abstractmethod
    def __call__(self, ctx: NodeContext) -> _T:
        pass


class NoopFunc(NodeFunction[None]):

    def __call__(self, _: NodeContext) -> None:
        return None


class IdentityFunc(NodeFunction):

    def __init__(self, node: IdentityNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> NodeResult:
        return _ctx_lookup(self.node.src_id, ctx)


class _ContextPushPopFunc(NodeFunction, abc.ABC):

    # This approach to context push / pop assumes all the nodes to be mapped are already available
    # A better approach would be to map individual items as they become available

    _PUSH = True
    _POP = False

    def __init__(self, node: tp.Union[ContextPushNode, ContextPopNode], direction: bool):
        self.node = node
        self.direction = direction

    def __call__(self, ctx: NodeContext) -> NodeResult:

        target_ctx = dict()

        for inner_id, outer_id in self.node.mapping.items():

            # Should never happen, push / pop nodes should always be in their own inner context
            if inner_id.namespace != self.node.namespace:
                raise _ex.EUnexpected()

            source_id = outer_id if self.direction == self._PUSH else inner_id
            target_id = inner_id if self.direction == self._PUSH else outer_id

            source_item = ctx.get(source_id)

            # Should never happen, source items are dependencies in the graph
            if source_item is None:
                raise _ex.EUnexpected()

            target_ctx[target_id] = source_item

        return target_ctx


class ContextPushFunc(_ContextPushPopFunc):

    def __init__(self, node: ContextPushNode):
        super(ContextPushFunc, self).__init__(node, self._PUSH)


class ContextPopFunc(_ContextPushPopFunc):

    def __init__(self, node: ContextPopNode):
        super(ContextPopFunc, self).__init__(node, self._POP)


class KeyedItemFunc(NodeFunction):

    def __init__(self, node: KeyedItemNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> NodeResult:
        src_node_result = _ctx_lookup(self.node.src_id, ctx)
        src_item = src_node_result.get(self.node.src_item)
        return src_item


class BuildJobResultFunc(NodeFunction[_config.JobResult]):

    def __init__(self, node: BuildJobResultNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> _config.JobResult:

        job_result = _config.JobResult()
        job_result.jobId = self.node.job_id
        job_result.statusCode = meta.JobStatusCode.SUCCEEDED

        for result_id in self.node.result_ids:

            # TODO: Handle individual failed results

            result_set = _ctx_lookup(result_id, ctx)
            job_result.results.update(result_set.items())

        return job_result


class SaveJobResultFunc(NodeFunction[None]):

    def __init__(self, node: SaveJobResultNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> None:

        job_result = _ctx_lookup(self.node.job_result_id, ctx)

        if not self.node.result_spec.save_result:
            return None

        # TODO: Full implementation
        class Dumper(json.JSONEncoder):
            def default(self, o: tp.Any) -> str:

                if isinstance(o, enum.Enum):
                    return o.name
                if isinstance(o, uuid.UUID):
                    return str(o)
                elif type(o).__module__.startswith("tracdap."):
                    return {**o.__dict__}  # noqa
                else:
                    return super().default(o)

        if self.node.result_spec.result_format == "json":
            job_result_bytes = bytes(json.dumps(job_result, cls=Dumper, indent=4), "utf-8")
        elif self.node.result_spec.result_format == "yaml":
            job_result_bytes = bytes(yaml.dump(job_result), "utf-8")
        else:
            raise _ex.EUnexpected(f"Unsupported result format [{self.node.result_spec.result_format}]")

        job_key = _util.object_key(job_result.jobId)
        job_result_file = f"job_result_{job_key}.{self.node.result_spec.result_format}"
        job_result_path = pathlib \
            .Path(self.node.result_spec.result_dir) \
            .joinpath(job_result_file)

        _util.logger_for_object(self).info(f"Saving job result to [{job_result_path}]")

        with open(job_result_path, "xb") as result_stream:
            result_stream.write(job_result_bytes)

        return None


class SetParametersFunc(NodeFunction[tp.Dict[str, tp.Any]]):

    def __init__(self, node: SetParametersNode):
        super().__init__()
        self.node = node

    def __call__(self, ctx: NodeContext) -> tp.Dict[str, tp.Any]:

        log = _util.logger_for_object(self)

        native_params = dict()

        for p_name, p_value in self.node.parameters.items():
            native_value = _types.decode_value(p_value)
            native_params[p_name] = native_value
            log.info(f"Set parameter [{p_name}] = {native_value} ({p_value.type.basicType.name})")

        return native_params


class DataViewFunc(NodeFunction[_data.DataView]):

    def __init__(self, node: DataViewNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> _data.DataView:

        root_item = _ctx_lookup(self.node.root_item, ctx)
        root_part_key = _data.DataPartKey.for_root()

        return _data.DataView(self.node.schema, {root_part_key: [root_item]})


class DataItemFunc(NodeFunction[_data.DataItem]):

    def __init__(self, node: DataItemNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> _data.DataItem:

        data_view = _ctx_lookup(self.node.data_view_id, ctx)

        # TODO: Support selecting data item described by self.node

        # Selecting data item for part-root, delta=0
        part_key = _data.DataPartKey.for_root()
        part = data_view.parts[part_key]
        delta = part[0]  # selects delta=0

        return delta


class DataResultFunc(NodeFunction[ObjectMap]):

    def __init__(self, node: DataResultNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> ObjectMap:

        data_spec = _ctx_lookup(self.node.data_spec_id, ctx)

        # TODO: Check result of save operation
        # save_result = _ctx_lookup(self.node.data_save_id, ctx)

        data_result = meta.ObjectDefinition(objectType=meta.ObjectType.DATA, data=data_spec.data_def)
        storage_result = meta.ObjectDefinition(objectType=meta.ObjectType.STORAGE, storage=data_spec.storage_def)

        return {self.node.data_key: data_result, self.node.storage_key: storage_result}


class StaticDataSpecFunc(NodeFunction[_data.DataItemSpec]):

    def __init__(self, node: StaticDataSpecNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> _data.DataItemSpec:
        return self.node.data_spec


class DynamicDataSpecFunc(NodeFunction[_data.DataItemSpec]):

    DATA_ITEM_TEMPLATE = "data/{}/{}/{}/snap-{:d}/delta-{:d}-x{:0>6x}"

    RANDOM = random.Random()
    RANDOM.seed()

    def __init__(self, node: DynamicDataSpecNode, storage: _storage.StorageManager):
        self.node = node
        self.storage = storage

    def __call__(self, ctx: NodeContext) -> _data.DataItemSpec:

        if self.node.prior_data_spec is not None:
            raise _ex.ETracInternal("Data updates not supported yet")

        data_view = _ctx_lookup(self.node.data_view_id, ctx)

        data_id = self.node.data_obj_id
        storage_id = self.node.storage_obj_id
        # TODO: pass this in from somewhere
        object_timestamp = datetime.datetime.utcnow()

        part_key = meta.PartKey("part-root", meta.PartType.PART_ROOT)
        snap_index = 0
        delta_index = 0

        data_type = data_view.schema.schemaType.name.lower()
        suffix_bytes = random.randint(0, 1 << 24)

        data_item = self.DATA_ITEM_TEMPLATE.format(
            data_type, data_id.objectId,
            part_key.opaqueKey, snap_index, delta_index,
            suffix_bytes)

        delta = meta.DataDefinition.Delta(delta_index, data_item)
        snap = meta.DataDefinition.Snap(snap_index, [delta])
        part = meta.DataDefinition.Part(part_key, snap)

        data_def = meta.DataDefinition()
        data_def.storageId = _util.selector_for_latest(storage_id)
        data_def.schema = data_view.schema
        data_def.parts[part_key.opaqueKey] = part

        storage_key = self.storage.default_storage_key()
        storage_format = self.storage.default_storage_format()
        storage_path = data_item

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

        return _data.DataItemSpec(
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

    def __call__(self, ctx: NodeContext) -> _data.DataItem:

        data_spec = _ctx_lookup(self.node.spec_id, ctx)
        data_copy = self._choose_copy(data_spec.data_item, data_spec.storage_def)
        data_storage = self.storage.get_data_storage(data_copy.storageKey)

        df = data_storage.read_pandas_table(
            data_spec.data_def.schema.table,
            data_copy.storagePath,
            data_copy.storageFormat,
            storage_options={})

        return _data.DataItem(pandas=df)


class SaveDataFunc(NodeFunction, _LoadSaveDataFunc):

    def __init__(self, node: SaveDataNode, storage: _storage.StorageManager):
        super().__init__(storage)
        self.node = node

    def __call__(self, ctx: NodeContext) -> NodeResult:

        # This function assumes that metadata has already been generated as the data_spec
        # i.e. it is already known which incarnation / copy of the data will be created

        data_spec = _ctx_lookup(self.node.spec_id, ctx)

        data_copy = self._choose_copy(data_spec.data_item, data_spec.storage_def)
        file_storage = self.storage.get_file_storage(data_copy.storageKey)
        data_storage = self.storage.get_data_storage(data_copy.storageKey)

        # Make sure parent directory exists
        parent_dir = str(pathlib.PurePath(data_copy.storagePath).parent)
        file_storage.mkdir(parent_dir, recursive=True, exists_ok=True)

        # Item to be saved should exist in the current context, for now assume it is always Pandas
        data_item = _ctx_lookup(self.node.data_item_id, ctx)
        df = data_item.pandas

        # Assumption that dataset is a table, and not some other schema type

        data_storage.write_pandas_table(
            data_spec.data_def.schema.table, df,
            data_copy.storagePath, data_copy.storageFormat,
            storage_options={}, overwrite=False)

        return True


class ImportModelFunc(NodeFunction[meta.ModelDefinition]):

    def __init__(self, node: ImportModelNode, models: _models.ModelLoader):
        self.node = node
        self._models = models

        self._log = _util.logger_for_object(self)

    def __call__(self, ctx: NodeContext) -> meta.ModelDefinition:

        stub_model_def = meta.ModelDefinition(
            language=self.node.import_details.language,
            repository=self.node.import_details.repository,
            path=self.node.import_details.path,
            package=self.node.import_details.package,
            entryPoint=self.node.import_details.entryPoint,
            version=self.node.import_details.version)

        model_class = self._models.load_model_class(self.node.model_scope, stub_model_def)
        model_scan = self._models.scan_model(model_class)

        model_def = copy.copy(stub_model_def)
        model_def.parameters = model_scan.parameters
        model_def.inputs = model_scan.inputs
        model_def.outputs = model_scan.outputs

        return model_def


class ImportModelResultFunc(NodeFunction[ObjectMap]):

    def __init__(self, node: ImportModelResultNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> ObjectMap:

        model_def = _ctx_lookup(self.node.import_id, ctx)

        object_key = _util.object_key(self.node.object_id)
        object_def = meta.ObjectDefinition(meta.ObjectType.MODEL, model=model_def)

        return {object_key: object_def}


class RunModelFunc(NodeFunction):

    def __init__(self, node: RunModelNode, model_class: _api.TracModel.__class__):
        super().__init__()
        self.node = node
        self.model_class = model_class

    def __call__(self, ctx: NodeContext) -> NodeResult:

        model_def = self.node.model_def

        # Create a context containing only declared items in the current namespace, addressed by name

        def filter_ctx(node_id: NodeId):
            if node_id.namespace != self.node.id.namespace:
                return False
            if node_id.name in model_def.parameters or node_id.name in model_def.inputs:
                return True
            return False

        local_ctx = {nid.name: n.result for nid, n in ctx.items() if filter_ctx(nid)}

        # Add empty data views to the local context to hold model outputs
        # Assuming outputs are all defined with static schemas

        for output_name in model_def.outputs:
            blank_data_view = _data.DataView(schema=self.node.model_def.outputs[output_name].schema, parts={})
            local_ctx[output_name] = blank_data_view

        # Run the model against the mapped local context

        trac_ctx = _ctx.TracContextImpl(self.node.model_def, self.model_class, local_ctx)

        model = self.model_class()
        model.run_model(trac_ctx)

        # The node result is just the model outputs taken from the local context
        model_outputs = {
            name: obj for name, obj in local_ctx.items()
            if name in self.node.model_def.outputs}

        return model_outputs


class FunctionResolver:

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
        SetParametersNode: SetParametersFunc,
        DataViewNode: DataViewFunc,
        DataItemNode: DataItemFunc,
        ImportModelResultNode: ImportModelResultFunc,
        BuildJobResultNode: BuildJobResultFunc,
        SaveJobResultNode: SaveJobResultFunc,
        StaticDataSpecNode: StaticDataSpecFunc,
        DataResultNode: DataResultFunc}

    __node_mapping: tp.Dict[Node.__class__, __ResolveFunc] = {

        LoadDataNode: resolve_load_data,
        SaveDataNode: resolve_save_data,
        DynamicDataSpecNode: resolve_dynamic_data_spec,
        RunModelNode: resolve_run_model_node,
        ImportModelNode: resolve_import_model_node,

        SaveJobResultNode: lambda s, n: NoopFunc(),
        BuildJobResultNode: lambda s, n: NoopFunc(),
        JobNode: lambda s, n: NoopFunc()
    }
