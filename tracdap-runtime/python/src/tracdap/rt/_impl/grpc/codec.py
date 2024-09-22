#  Copyright 2024 Accenture Global Solutions Limited
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

import enum
import typing as tp

import tracdap.rt.exceptions as ex
import tracdap.rt.metadata as metadata

import tracdap.rt_gen.grpc.tracdap.metadata.type_pb2 as type_pb2
import tracdap.rt_gen.grpc.tracdap.metadata.object_id_pb2 as object_id_pb2
import tracdap.rt_gen.grpc.tracdap.metadata.object_pb2 as object_pb2
from tracdap.rt_gen.grpc.tracdap.metadata import model_pb2
import tracdap.rt_gen.grpc.tracdap.metadata.data_pb2 as data_pb2
import tracdap.rt_gen.grpc.tracdap.metadata.stoarge_pb2 as storage_pb2

from google.protobuf import message as _message


__METADATA_MAPPING = {
    metadata.TypeDescriptor: type_pb2.TypeDescriptor,
    metadata.Value: type_pb2.Value,
    metadata.DecimalValue: type_pb2.DecimalValue,
    metadata.DateValue: type_pb2.DateValue,
    metadata.DatetimeValue: type_pb2.DatetimeValue,
    metadata.ArrayValue: type_pb2.ArrayValue,
    metadata.MapValue: type_pb2.MapValue,
    metadata.TagHeader: object_id_pb2.TagHeader,
    metadata.TagSelector: object_id_pb2.TagSelector,
    metadata.ObjectDefinition: object_pb2.ObjectDefinition,
    metadata.ModelDefinition: model_pb2.ModelDefinition,
    metadata.ModelParameter: model_pb2.ModelParameter,
    metadata.ModelInputSchema: model_pb2.ModelInputSchema,
    metadata.ModelOutputSchema: model_pb2.ModelOutputSchema,
    metadata.SchemaDefinition: data_pb2.SchemaDefinition,
    metadata.TableSchema: data_pb2.TableSchema,
    metadata.FieldSchema: data_pb2.FieldSchema,
    metadata.PartKey: data_pb2.PartKey,
    metadata.DataDefinition: data_pb2.DataDefinition,
    metadata.DataDefinition.Part: data_pb2.DataDefinition.Part,
    metadata.DataDefinition.Snap: data_pb2.DataDefinition.Snap,
    metadata.DataDefinition.Delta: data_pb2.DataDefinition.Delta,
    metadata.StorageDefinition: storage_pb2.StorageDefinition,
    metadata.StorageIncarnation: storage_pb2.StorageIncarnation,
    metadata.StorageCopy: storage_pb2.StorageCopy,
    metadata.StorageItem: storage_pb2.StorageItem
}


_T_MSG = tp.TypeVar('_T_MSG', bound=_message.Message)


def encode_message(msg_class: _T_MSG.__class__, obj: tp.Any) -> _T_MSG:

    attrs = dict((k, encode(v)) for k, v in obj.__dict__.items())

    return msg_class(**attrs)


def encode(obj: tp.Any) -> tp.Any:

    # Translate TRAC domain objects into generic dict / list structures
    # These can be accepted by gRPC message constructors, do not try to build messages directly
    # Use shallow copies and builtins to minimize performance impact

    if obj is None:
        return None

    if isinstance(obj, str) or isinstance(obj, bool) or isinstance(obj, int) or isinstance(obj, float):
        return obj

    if isinstance(obj, enum.Enum):
        return obj.value

    if isinstance(obj, list):
        return list(map(encode, obj))

    if isinstance(obj, dict):
        return dict((k, encode(v)) for k, v in obj.items())

    msg_class = __METADATA_MAPPING.get(type(obj))

    if msg_class is None:
        raise ex.ETracInternal(f"No gRPC metadata mapping is available for type {type(obj).__name__}")

    attrs = dict((k, encode(v)) for k, v in obj.__dict__.items() if v is not None)

    return msg_class(**attrs)
