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

import typing as _tp

import tracdap.rt.ext.external as _external
import tracdap.rt.ext.plugins as _plugins


class HttpPlugin(_external.IExternalSystem):

    def __init__(self, properties: _tp.Dict[str, str]):
        self.__properties = properties

    def supported_types(self) -> _tp.List[type]:
        pass

    def supported_args(self) -> _tp.Optional[_tp.Dict[str, type]]:
        pass

    def create_client(self, client_type: type, **client_args) -> _tp.Any:
        pass

    def close_client(self, client: _tp.Any):
        pass


_plugins.PluginManager.register_plugin(
    _external.IExternalSystem, HttpPlugin,
    protocols=["http", "https"])
