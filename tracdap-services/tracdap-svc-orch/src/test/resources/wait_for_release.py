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

import pathlib
import time
import typing as tp

import tracdap.rt.api as trac


class WaitForRelease(trac.TracModel):

    """Logs a line, then waits until the test creates the release file, so the test sees the job running"""

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(
            trac.P("release_file", trac.STRING, "File the test creates to let the model finish"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {}

    def run_model(self, ctx: trac.TracContext):

        ctx.log.info("Waiting for release")

        release_file = pathlib.Path(ctx.get_parameter("release_file"))
        give_up = time.monotonic() + 300

        while not release_file.exists():
            if time.monotonic() > give_up:
                raise RuntimeError("The test did not release the model")
            time.sleep(0.2)

        ctx.log.info("Released")
