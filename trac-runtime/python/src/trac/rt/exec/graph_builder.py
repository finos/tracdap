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

import trac.rt.metadata as meta
from .graph import *


class GraphBuilder:

    @staticmethod
    def build_job() -> Graph:
        pass

    @staticmethod
    def build_calculation_job(
            job_def: meta.JobDefinition,
            metadata: tp.Dict[meta.TagSelector, meta.ObjectDefinition]):

        job_ctx_mapping = dict()
        job_ctx_push = ContextPushNode(job_ctx_mapping)

        job_inputs = list(map(GraphBuilder.build_data_load, job_def.input.values()))

        job_target = metadata[job_def.target]


    @staticmethod
    def build_data_load():
        pass

    def build_model_or_flow(self):
        pass

    def build_model(self):
        pass

    def build_flow(self):
        pass
