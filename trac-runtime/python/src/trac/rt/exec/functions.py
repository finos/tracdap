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

from __future__ import annotations

import copy
import enum
import abc
import json
import yaml
import uuid

import trac.rt.api as _api
import trac.rt.config as _config
import trac.rt.exceptions as _ex
import trac.rt.impl.models as _models
import trac.rt.impl.storage as _storage
import trac.rt.impl.data as _data
import trac.rt.impl.util as _util
import trac.rt.impl.type_system as _types

import trac.rt.exec.context as _ctx
from trac.rt.exec.graph import *


NodeContext = tp.Dict[NodeId, object]  # Available prior node results when a node function is called
NodeResult = tp.Any  # Result of a node function (will be recorded against the node ID)

_T = tp.TypeVar('_T')
_N = tp.TypeVar("_N", bound=Node)


def _ctx_lookup(node_id: NodeId[_T], ctx: NodeContext) -> _T:
    # TODO: Error handling and type checking
    return ctx.get(node_id).result  # noqa


class NodeFunction(tp.Callable[[NodeContext], _T], abc.ABC):

    @abc.abstractmethod
    def __call__(self, ctx: NodeContext) -> _T:
        pass


class NoopFunc(NodeFunction):

    def __call__(self, _: NodeContext) -> NodeResult:
        return None


class IdentityFunc(NodeFunction):

    def __init__(self, node: IdentityNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> NodeResult:
        return ctx[self.node.src_id].result


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
        src_node_result = ctx[self.node.src_id].result
        src_item = src_node_result.get(self.node.src_item)
        return src_item


class BuildJobResultFunc(NodeFunction):

    def __init__(self, node: BuildJobResultNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> _config.JobResult:

        job_result = _config.JobResult()
        job_result.jobId = self.node.job_id
        job_result.status = _config.JobStatus.SUCCEEDED

        for output_name, output_id in self.node.outputs.items():

            save_output_node = _ctx_lookup(output_id, ctx)

            output_save_result: object = None
            output_spec: _data.DataItemSpec = _ctx_lookup(output_spec_id, ctx)

            output_mapping = object()  # self.node.job_config.outputs[output_name]

            output_data_id = None
            output_storage_id = None

            job_result.objects[output_data_id] = meta.ObjectDefinition(
                meta.ObjectType.DATA, data=output_spec.data_def)

            job_result.objects[output_storage_id] = meta.ObjectDefinition(
                meta.ObjectType.STORAGE, storage=output_spec.storage_def)


            job_result.objects[output_id.name] = output_result

        return job_result


class SaveJobResultFunc(NodeFunction):

    def __init__(self, node: SaveJobResultNode):
        super().__init__(node)

    def __call__(self, ctx: NodeContext) -> NodeResult:

        node.jo

        job_result = _config.JobResult()  # todo

        if not self.node.result_spec.save_result:
            return None

        # TODO: Full implementation
        class Dumper(json.JSONEncoder):
            def default(self, o: tp.Any) -> str:

                if isinstance(o, enum.Enum):
                    return o.name
                if isinstance(o, uuid.UUID):
                    return str(o)
                elif type(o).__module__.startswith("trac."):
                    return {**o.__dict__}
                else:
                    return super().default(o)

        if self.node.result_spec.result_format == "json":
            job_result_bytes = bytes(json.dumps(job_result, cls=Dumper, indent=4), "utf-8")
        elif self.node.result_spec.result_format == "yaml":
            job_result_bytes = bytes(yaml.dump(job_result), "utf-8")
        else:
            raise _ex.EUnexpected(f"Unsupported result format [{self.node.result_spec.result_format}]")

        job_key = _util.object_key(self.node.job_id)
        job_result_file = f"job_result_{job_key}.{self.node.result_spec.result_format}"
        job_result_path = pathlib \
            .Path(self.node.result_spec.result_dir) \
            .joinpath(job_result_file)

        _util.logger_for_object(self).info(f"Saving job result to [{job_result_path}]")

        with open(job_result_path, "xb") as result_stream:
            result_stream.write(job_result_bytes)

        return None


class SetParametersFunc(NodeFunction):

    def __init__(self, node: SetParametersNode):
        super().__init__()
        self.node = node

    def __call__(self, ctx: NodeContext) -> NodeResult:

        log = _util.logger_for_object(self)

        native_params = dict()

        for p_name, p_value in self.node.parameters.items():
            native_value = _types.decode_value(p_value)
            native_params[p_name] = native_value
            log.info(f"Set parameter [{p_name}] = {native_value} ({p_value.type.basicType.name})")

        return native_params


class DataViewFunc(NodeFunction):

    def __init__(self, node: DataViewNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> NodeResult:

        root_node = ctx.get(self.node.root_item)  # noqa
        root_item: _data.DataItem = root_node.result  # noqa
        root_part_key = _data.DataPartKey.for_root()

        return _data.DataView(self.node.schema, {root_part_key: [root_item]})


class DataItemFunc(NodeFunction):

    def __init__(self, node: DataItemNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> DataItemNode.id.result_type:

        data_view = _ctx_lookup(self.node.data_view_id, ctx)

        # TODO: Support selecting data item described by self.node

        # Selecting data item for part-root, delta=0
        part_key = _data.DataPartKey.for_root()
        part = data_view.parts[part_key]
        delta: _data.DataItem = part[0]  # selects delta=0

        return delta


class StaticDataSpecFunc(NodeFunction[_data.DataItemSpec]):

    def __init__(self, node: StaticDataSpecNode):
        self.node = node

    def __call__(self, ctx: NodeContext) -> _data.DataItemSpec:
        return self.node.data_spec


class DynamicDataSpecFunc(NodeFunction[_data.DataItemSpec]):

    def __init__(self, node: DynamicDataSpecNode):
        self.node = node
        self.part_key = ""
        self.is_delta = False

    def __call__(self, ctx: NodeContext) -> _data.DataItemSpec:

        prior_data_def: meta.DataDefinition = None
        prior_storage_def: meta.StorageDefinition = None
        prior_schema_def: meta.SchemaDefinition = None

        if prior_data_def is None:
            data_def = meta.DataDefinition()
            data_def.storageId = meta.TagSelector()  # TODO
            data_def.schema = # TODO
            data_def.schemaId = # TODO
        else:
            data_def = copy.copy(prior_data_def)

        if prior_storage_def is None:
            storage_def = meta.StorageDefinition()
        else:
            storage_def = copy.copy(prior_storage_def)

        delta = meta.DataDefinition.Delta()

        if self.part_key in data_def.parts:
            part = data_def.parts[self.part_key]
            if self.is_delta:
                snap_index = part.snap.snapIndex
                delta_index = len(part.snap.deltas)
            else:
                snap_index = part.snap.snapIndex + 1
                delta_index = 0

        data_item = f""

        storage_key = "TODO"  # TODO
        storage_path = ""  # from data item
        storage_format = "CSV"  # TODO

        storage_copy = meta.StorageCopy(
            storage_key, storage_path, storage_format,
            meta.CopyStatus.COPY_AVAILABLE)

        storage_incarnation = meta.StorageIncarnation(
            [storage_copy],
            incarnationIndex=0,
            incarnationTimestamp=meta.DatetimeValue(isoDatetime=""),  # TODO
            incarnationStatus=meta.IncarnationStatus.INCARNATION_AVAILABLE)

        storage_item = meta.StorageItem([storage_incarnation])
        storage_def.dataItems = {**storage_def.dataItems, data_item: storage_item}

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

        copy = next(filter(
            lambda c: c.copyStatus == meta.CopyStatus.COPY_AVAILABLE
            and self.storage.has_data_storage(c.storageKey),
            incarnation.copies), None)

        if copy is None:
            raise _ex.EValidationGap()

        return copy


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


class ImportModelFunc(NodeFunction):

    def __init__(self, node: ImportModelNode, models: _models.ModelLoader):
        self.node = node
        self._models = models

        self._log = _util.logger_for_object(self)

    def __call__(self, ctx: NodeContext) -> NodeResult:

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

        object_def = meta.ObjectDefinition(
            objectType=meta.ObjectType.MODEL,
            model=model_def)

        return object_def


class RunModelFunc(NodeFunction):

    def __init__(self, node: RunModelNode, job_config: _config.JobConfig, model_class: _api.TracModel.__class__):
        super().__init__()
        self.node = node
        self.job_config = job_config
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

    __ResolveFunc = tp.Callable[['FunctionResolver', _config.JobConfig, Node[_T]], NodeFunction[_T]]

    def __init__(self, models: _models.ModelLoader, storage: _storage.StorageManager):
        self._models = models
        self._storage = storage

    def resolve_node(self, job_config, node: Node[_T]) -> NodeFunction[_T]:

        basic_node_class = self.__basic_node_mapping.get(node.__class__)

        if basic_node_class:
            return basic_node_class(node)

        resolve_func = self.__node_mapping[node.__class__]

        if resolve_func is None:
            raise _ex.EUnexpected()

        return resolve_func(self, job_config, node)

    def resolve_load_data(self, job_config: _config.JobConfig, node: LoadDataNode):
        return LoadDataFunc(node, self._storage)

    def resolve_save_data(self, job_config: _config.JobConfig, node: SaveDataNode):
        return SaveDataFunc(node, self._storage)

    def resolve_import_model_node(self, job_config: _config.JobConfig, node: ImportModelNode):
        return ImportModelFunc(node, self._models)

    def resolve_run_model_node(self, job_config: _config.JobConfig, node: RunModelNode) -> NodeFunction:

        model_scope = _util.object_key(job_config.jobId)
        model_class = self._models.load_model_class(model_scope, node.model_def)

        # TODO: Verify model_class against model_def

        return RunModelFunc(node, job_config, model_class)

    __basic_node_mapping: tp.Dict[Node.__class__, NodeFunction.__class__] = {
        ContextPushNode: ContextPushFunc,
        ContextPopNode: ContextPopFunc,
        IdentityNode: IdentityFunc,
        KeyedItemNode: KeyedItemFunc,
        SetParametersNode: SetParametersFunc,
        DataViewNode: DataViewFunc,
        DataItemNode: DataItemFunc,
        BuildJobResultNode: BuildJobResultFunc}

    __node_mapping: tp.Dict[Node.__class__, __ResolveFunc] = {

        LoadDataNode: resolve_load_data,
        SaveDataNode: resolve_save_data,
        RunModelNode: resolve_run_model_node,
        ImportModelNode: resolve_import_model_node,

        SaveJobResultNode: lambda s, j, n: NoopFunc(),
        BuildJobResultNode: lambda s, j, n: NoopFunc(),
        JobNode: lambda s, j, n: NoopFunc()
    }
