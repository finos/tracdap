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
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import tracdap.rt.api as trac
import typing as tp

import tutorial.schemas as schemas


class CustomerDataFilter(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("filter_region", trac.STRING, label="Filter region"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        customer_loans_schema = trac.load_schema(schemas, "customer_loans.csv")
        unfiltered_loans = trac.define_input(customer_loans_schema, label="Unfiltered loans data")

        return { "unfiltered_loans": unfiltered_loans }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        customer_loans_schema = trac.load_schema(schemas, "customer_loans.csv")
        customer_loans = trac.define_output(customer_loans_schema, label="Filtered loans data")

        return { "customer_loans": customer_loans }

    def run_model(self, ctx: trac.TracContext):

        unfilter_loans = ctx.get_pandas_table("unfiltered_loans")
        filter_region = ctx.get_parameter("filter_region")

        customer_loans = unfilter_loans[unfilter_loans["region"] != filter_region]

        ctx.put_pandas_table("customer_loans", customer_loans)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_job( "config/chaining.yaml", "config/sys_config.yaml")
