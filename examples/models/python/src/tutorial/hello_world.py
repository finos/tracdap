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

import typing as tp
import tracdap.rt.api as trac


class HelloWorldModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P(
                "meaning_of_life", trac.INTEGER,
                label="The answer to the ultimate question of life, the universe and everything"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {}

    def run_model(self, ctx: trac.TracContext):

        ctx.log().info("Hello world model is running")

        meaning_of_life = ctx.get_parameter("meaning_of_life")
        ctx.log().info(f"The meaning of life is {meaning_of_life}")


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(HelloWorldModel, "config/hello_world.yaml", "config/sys_config.yaml")
