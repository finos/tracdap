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

def encode(obj: tp.Any) -> tp.Any:

    # Translate TRAC domain objects into generic dict / list structures
    # These can be accepted by gRPC message constructors, do not try to build messages directly
    # Use shallow copies and builtins to minimize performance impact

    if obj is None:
        return None

    if isinstance(obj, str) or isinstance(obj, bool) or isinstance(obj, int) or isinstance(obj, float):
        return obj

    if isinstance(obj, enum.Enum):
        return _encode_enum(obj)

    if isinstance(obj, list):
        return _encode_list(obj)

    if isinstance(obj, dict):
        return _encode_dict(obj)

    # Filter classes for TRAC domain objects (sanity check, not a watertight validation)
    if hasattr(obj, "__module__") and "tracdap" in obj.__module__:
        return _encode_object(obj)

    raise RuntimeError(f"Cannot encode object of type [{type(obj).__name__}] for gRPC")

def _encode_object(obj: object) -> dict:
    return dict(map(lambda kv: (kv[0], encode(kv[1])), obj.__dict__.items()))

def _encode_dict(kvs: dict) -> dict:
    return dict(map(lambda kv: (kv[0], encode(kv[1])), kvs.items()))

def _encode_list(xs: list) -> list:
    return list(map(encode, xs))

def _encode_enum(x: enum.Enum) -> int:
    # This is to handle domain object enums, which currently use tuples to provide enum value docs
    # A better solution would be to set __doc__ on enum values
    if isinstance(x.value, tuple):
        return x.value[0]
    else:
        return x.value

