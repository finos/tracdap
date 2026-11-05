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

import dataclasses as dc
import typing as tp

import tracdap.rt.api as trac


# Logged by every scenario model through ctx.log
MODEL_LOG_LINE = "Scenario model [{}] ran"


def _account_fields():
    return [
        trac.F("id", trac.STRING, "Account ID", business_key=True),
        trac.F("region", trac.STRING, "Region", categorical=True),
        trac.F("amount", trac.FLOAT, "Amount")]


def _total_fields():
    return [
        trac.F("region", trac.STRING, "Region", business_key=True, categorical=True),
        trac.F("total", trac.FLOAT, "Total amount")]


def _log_run(model: trac.TracModel, ctx: trac.TracContext):
    ctx.log.info(MODEL_LOG_LINE.format(type(model).__name__))


@dc.dataclass
class RunSettings:

    scale: float
    label: str


class FilterAccounts(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(trac.P("min_amount", trac.FLOAT, "Minimum amount", default_value=0.0))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"accounts": trac.define_input_table(*_account_fields(), label="Accounts")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"filtered_accounts": trac.define_output_table(*_account_fields(), label="Filtered accounts")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        accounts = ctx.get_pandas_table("accounts")
        filtered = accounts[accounts["amount"] >= ctx.get_parameter("min_amount")]
        ctx.put_pandas_table("filtered_accounts", filtered)


class SumByRegion(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"accounts": trac.define_input_table(*_account_fields(), label="Accounts")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"region_totals": trac.define_output_table(*_total_fields(), label="Totals by region")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        accounts = ctx.get_pandas_table("accounts")
        totals = accounts.groupby("region", as_index=False)["amount"].sum().rename(columns={"amount": "total"})
        ctx.put_pandas_table("region_totals", totals)


class CountAccounts(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"accounts": trac.define_input_table(*_account_fields(), label="Accounts")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"account_count": trac.define_output_table(
            trac.F("account_count", trac.INTEGER, "Number of accounts"),
            label="Account count")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        accounts = ctx.get_pandas_table("accounts")
        ctx.put_pandas_table("account_count", accounts[["id"]].count().to_frame("account_count"))


class ScaleAmountsA(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(trac.P("factor", trac.FLOAT, "Scaling factor", default_value=2.0))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"accounts": trac.define_input_table(*_account_fields(), label="Accounts")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"scaled_a": trac.define_output_table(*_account_fields(), label="Scaled accounts (A)")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        accounts = ctx.get_pandas_table("accounts")
        accounts["amount"] = accounts["amount"] * ctx.get_parameter("factor")
        ctx.put_pandas_table("scaled_a", accounts)


class ScaleAmountsB(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(trac.P("factor", trac.FLOAT, "Scaling factor", default_value=0.5))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"accounts": trac.define_input_table(*_account_fields(), label="Accounts")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"scaled_b": trac.define_output_table(*_account_fields(), label="Scaled accounts (B)")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        accounts = ctx.get_pandas_table("accounts")
        accounts["amount"] = accounts["amount"] / ctx.get_parameter("factor")
        ctx.put_pandas_table("scaled_b", accounts)


class CombineAccounts(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {
            "left": trac.define_input_table(*_account_fields(), label="Left accounts"),
            "right": trac.define_input_table(*_account_fields(), label="Right accounts")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"combined_accounts": trac.define_output_table(*_account_fields(), label="Combined accounts")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        left = ctx.get_pandas_table("left")
        right = ctx.get_pandas_table("right").set_index("id")
        left["amount"] = left["amount"] + left["id"].map(right["amount"])
        ctx.put_pandas_table("combined_accounts", left)


class SplitAccounts(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(trac.P("threshold", trac.FLOAT, "High value threshold", default_value=100.0))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"accounts": trac.define_input_table(*_account_fields(), label="Accounts")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {
            "high_value": trac.define_output_table(*_account_fields(), label="High value accounts"),
            "low_value": trac.define_output_table(*_account_fields(), label="Low value accounts"),
            "rejects": trac.define_output_table(*_account_fields(), label="Rejected accounts", optional=True)}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        accounts = ctx.get_pandas_table("accounts")
        threshold = ctx.get_parameter("threshold")
        ctx.put_pandas_table("high_value", accounts[accounts["amount"] >= threshold])
        ctx.put_pandas_table("low_value", accounts[accounts["amount"] < threshold])


class SelectAccounts(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {
            "source": trac.define_input_table(label="Any table with account columns", dynamic=True),
            "exclude_ids": trac.define_input_table(
                trac.F("id", trac.STRING, "Account ID", business_key=True),
                label="Accounts to exclude", optional=True)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"selected_accounts": trac.define_output_table(*_account_fields(), label="Selected accounts")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        source = ctx.get_pandas_table("source")[["id", "region", "amount"]]
        if ctx.has_dataset("exclude_ids"):
            source = source[~source["id"].isin(ctx.get_pandas_table("exclude_ids")["id"])]
        ctx.put_pandas_table("selected_accounts", source)


class ApplySettings(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"settings": trac.define_input_struct(RunSettings, label="Run settings")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"applied_settings": trac.define_output_struct(RunSettings, label="Applied settings")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        settings = ctx.get_struct("settings", RunSettings)
        ctx.put_struct("applied_settings", RunSettings(scale=settings.scale * 2, label=f"{settings.label} (applied)"))


class FailingModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"accounts": trac.define_input_table(*_account_fields(), label="Accounts")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"failed_accounts": trac.define_output_table(*_account_fields(), label="Never produced")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        raise RuntimeError("Scenario model failure")


class RenderReport(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"region_totals": trac.define_input_table(*_total_fields(), label="Totals by region")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"report": trac.define_output(trac.CommonFileTypes.TXT, label="Region report")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        totals = ctx.get_pandas_table("region_totals")
        lines = [f"{row.region}: {row.total}" for row in totals.itertuples()]
        ctx.put_file("report", "\n".join(lines).encode("utf-8"))


class AnnotateReport(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {"report": trac.define_input(trac.CommonFileTypes.TXT, label="Region report")}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"annotated_report": trac.define_output(trac.CommonFileTypes.TXT, label="Annotated report")}

    def run_model(self, ctx: trac.TracContext):
        _log_run(self, ctx)
        report = ctx.get_file("report")
        ctx.put_file("annotated_report", b"Region report\n" + report)
