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

import trac.rt.api as trac
import pyspark.sql.functions as f
import typing as tp


class HelloPyspark(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return {

            "filter_defaults": trac.P(
                "Exclude defaulted loans from the calculation",
                trac.BasicType.BOOLEAN, default_value=False)
        }

    def define_inputs(self) -> tp.Dict[str, trac.TableDefinition]:

        customer_loans = trac.define_table(
            trac.F("id", trac.BasicType.STRING, label="Customer account ID", business_key=True),
            trac.F("loan_amount", trac.BasicType.DECIMAL, label="Principal loan amount", format_code="CCY:EUR"),
            trac.F("total_pymnt", trac.BasicType.DECIMAL, label="Total amount repaid", format_code="CCY:EUR"),
            trac.F("region", trac.BasicType.STRING, label="Customer home region", categorical=True),
            trac.F("loan_condition_cat", trac.BasicType.INTEGER, label="Loan condition category", categorical=True))

        return {"customer_loans": customer_loans}

    def define_outputs(self) -> tp.Dict[str, trac.TableDefinition]:

        profit_by_region = trac.define_table(
            trac.F("region", trac.BasicType.STRING, label="Customer home region", categorical=True),
            trac.F("gross_profit", trac.BasicType.DECIMAL, label="Total gross profit", format_code="CCY:EUR"))

        return {"profit_by_region": profit_by_region}

    def run_model(self, ctx: trac.TracContext):

        filter_defaults = ctx.get_parameter("filter_defaults")

        customer_loans = ctx.get_spark_sql_dataset("customer_loans")

        if filter_defaults:
            customer_loans = customer_loans.filter(f.col("loan_condition_cat") == 0)

        customer_loans = customer_loans.withColumn("gross_profit", f.col("total_pymnt") - f.col("loan_amount"))

        profit_by_region = customer_loans \
            .groupBy("region") \
            .agg(f.col("region"), f.sum(f.col("gross_profit")).alias("gross_profit"))

        ctx.put_spark_sql_dataset("profit_by_region", profit_by_region)


if __name__ == "__main__":
    import trac.launch as launch
    launch.launch_model(HelloPyspark, "hello_pyspark.yaml", "examples/sys_config.yaml")
