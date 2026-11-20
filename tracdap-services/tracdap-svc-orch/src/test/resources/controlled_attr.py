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

import re
import typing as tp

import tracdap.rt.api as trac
import tracdap.rt._impl.core.validation as _val  # noqa

# Stand in for a runtime that does not know controlled attributes, its model attribute check lets ctl_ names through
_val.StaticValidator._StaticValidator__controlled_identifier_pattern = re.compile("(?!)")


class ControlledAttrModel(trac.TracModel):

    """Declares a controlled attribute, as a model on a runtime without the controlled name check could"""

    def define_attributes(self) -> tp.Dict[str, trac.Value]:

        return trac.define_attributes(
            trac.A("ctl_this_forged", "not allowed"))

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        return {"controlled_output": trac.define_output_table(
            trac.F("string_field", trac.BasicType.STRING, label="STRING field"))}

    def run_model(self, ctx: trac.TracContext):
        pass
