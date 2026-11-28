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

"""Models for the job progress tests, loaded from a local repo through the shim"""

import time
import typing as tp

import pandas as pd

import tracdap.rt.api as trac
import tracdap.rt.api.experimental as trac_x


MODEL_LINE = "Progress model [{}] ran"


class LogThenWait(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(trac.P("wait_seconds", trac.FLOAT, "Seconds to wait after logging"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {}

    def run_model(self, ctx: trac.TracContext):
        ctx.log.info(MODEL_LINE.format(type(self).__name__))
        time.sleep(ctx.get_parameter("wait_seconds"))


class LogManyLines(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(
            trac.P("line_count", trac.INTEGER, "Number of lines to log"),
            trac.P("line_length", trac.INTEGER, "Length of each line"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {}

    def run_model(self, ctx: trac.TracContext):
        for i in range(ctx.get_parameter("line_count")):
            ctx.log.info(f"line {i} ".ljust(ctx.get_parameter("line_length"), "x"))
        ctx.log.debug("debug line")


class GenerateAccounts(trac_x.TracDataImport):

    """Data import model whose output is added at runtime, so the graph is updated while the job runs"""

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {}

    def run_model(self, ctx: trac_x.TracDataContext):
        ctx.log.info(MODEL_LINE.format(type(self).__name__))
        ctx.add_data_import("generated_accounts")
        ctx.put_schema("generated_accounts", trac.define_schema(
            trac.F("id", trac.STRING, "Account ID", business_key=True),
            trac.F("amount", trac.FLOAT, "Amount")))
        ctx.put_pandas_table("generated_accounts", pd.DataFrame({"id": ["acc_001"], "amount": [1.0]}))


class SlowCount(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(trac.P("wait_seconds", trac.FLOAT, "Seconds to wait before counting"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"accounts": trac.define_input_table(
            trac.F("id", trac.STRING, "Account ID", business_key=True),
            trac.F("region", trac.STRING, "Region", categorical=True),
            trac.F("amount", trac.FLOAT, "Amount"),
            label="Accounts")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"account_count": trac.define_output_table(
            trac.F("account_count", trac.INTEGER, "Number of accounts"),
            label="Account count")}

    def run_model(self, ctx: trac.TracContext):
        ctx.log.info(MODEL_LINE.format(type(self).__name__))
        time.sleep(ctx.get_parameter("wait_seconds"))
        accounts = ctx.get_pandas_table("accounts")
        ctx.put_pandas_table("account_count", accounts[["id"]].count().to_frame("account_count"))
