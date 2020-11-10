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

import abc
import typing as tp

from trac.metadata import TableDefinition
from trac.metadata import ModelParameter


class TracContext:

    @abc.abstractmethod
    def get_parameter(self, parameter_name: str) -> tp.Any:
        pass

    @abc.abstractmethod
    def get_dataset_schema(self, dataset_name: str) -> TableDefinition:
        pass

    @abc.abstractmethod
    def put_dataset_schema(self, dataset_name: str, schema: TableDefinition):
        pass


class TracModel:

    @abc.abstractmethod
    def define_parameters(self) -> tp.Dict[str, ModelParameter]:
        pass

    @abc.abstractmethod
    def define_inputs(self) -> tp.Dict[str, TableDefinition]:
        pass

    @abc.abstractmethod
    def define_outputs(self) -> tp.Dict[str, TableDefinition]:
        pass

    @abc.abstractmethod
    def run_model(self, ctx: TracContext):
        pass
