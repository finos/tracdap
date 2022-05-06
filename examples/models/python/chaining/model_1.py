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

import typing as tp
import tracdap.rt.api as trac


class FirstModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        pass

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        pass

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        pass

    def run_model(self, ctx: trac.TracContext):
        pass
