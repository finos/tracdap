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

import enum

import typing as _tp
import dataclasses as _dc
import datetime as _dt

import tracdap.rt.api as trac


class EvolutionModel(enum.Enum):
    PERTURB = 1
    SCATTER = 2
    STOCHASTIC = 3

@_dc.dataclass
class ScenarioConfig:

    scenario_name: str
    default_weight: float
    evolution_model: EvolutionModel
    apply_smoothing: bool

@_dc.dataclass
class RunConfig:

    include_front_book: bool
    base_date: _dt.date

    base_scenario: ScenarioConfig
    stress_scenarios: dict[str, ScenarioConfig]


class StructModel(trac.TracModel):

    def define_parameters(self) -> _tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("t0_date", trac.DATE, "T0 date for projection"),
            trac.P("projection_period", trac.INTEGER, "Projection period (in months)"))

    def define_inputs(self) -> _tp.Dict[str, trac.ModelInputSchema]:

        run_config_struct = trac.define_struct(RunConfig)
        run_config = trac.define_input(run_config_struct, label="Run configuration")

        return {"run_config": run_config}


    def define_outputs(self) -> _tp.Dict[str, trac.ModelOutputSchema]:

        modified_config = trac.define_output_struct(RunConfig, label="Modified config for next model stage")
        return {"modified_config": modified_config}

    def run_model(self, ctx: trac.TracContext):

        run_config = ctx.get_struct("run_config", RunConfig)

        new_scenario = ScenarioConfig(
            scenario_name="hpi_shock",
            default_weight=1.0,
            evolution_model=EvolutionModel.STOCHASTIC,
            apply_smoothing=True)

        run_config.stress_scenarios["hpi_shock"] = new_scenario

        ctx.put_struct("modified_config", run_config)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(StructModel, "config/structured_objects.yaml", "config/sys_config.yaml")
