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


import tracdap.rt.api as trac

import io
import typing as tp
import pandas as pd

import tutorial.schemas as schemas


class RuntimeMetadataReport(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters()

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        return {
            "customer_loans": trac.define_input(trac.load_schema(schemas, "customer_loans.csv"), label="Customer loans"),
            "account_filter": trac.define_input(trac.load_schema(schemas, "account_filter.csv"), label="Account filter")
        }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        return {
            "data_report": trac.define_output(trac.define_file_type("md", "text/markdown"))
        }

    def run_model(self, ctx: trac.TracContext):

        customer_loans = ctx.get_pandas_table("customer_loans")
        customer_loans_schema = ctx.get_schema("customer_loans")
        customer_loans_metadata = ctx.get_metadata("customer_loans")

        account_filter = ctx.get_pandas_table("account_filter")
        account_filter_schema = ctx.get_schema("account_filter")
        account_filter_metadata = ctx.get_metadata("account_filter")

        with ctx.put_file_stream("data_report") as report_bytes:
            with io.TextIOWrapper(report_bytes, encoding='utf-8') as report:

                report.write(f"# Data Report\n\n")

                self.write_dataset_report("customer_loans", customer_loans_metadata, customer_loans_schema, customer_loans, report)
                self.write_dataset_report("account_filter", account_filter_metadata, account_filter_schema, account_filter, report)

    @classmethod
    def write_dataset_report(
            cls, dataset_name: str, metadata: trac.RuntimeMetadata,
            schema: trac.SchemaDefinition, dataset: pd.DataFrame,
            report: tp.TextIO):

        report.write(f"## {dataset_name}\n\n")

        if metadata is not None:

            report.write(f"Object ID: {metadata.objectId.objectId}  \n")
            report.write(f"Object version: {metadata.objectId.objectVersion}  \n\n")

            report.write("Attributes:\n\n")

            report.write("| Key | Value |\n")
            report.write("| --- | ----- |\n")

            for attr_key, attr_value in metadata.attributes.items():
                report.write(f"| {attr_key} | {str(attr_value)} |\n")

            report.write("\n")

        else:
            report.write("Metadata not available\n\n")

        report.write("Columns:\n\n")

        report.write("| Column | TRAC Type | Categorical | Pandas DType |\n")
        report.write("| ------ | --------- | ----------- | ------------ |\n")

        for field in schema.table.fields:
            report.write(f"| {field.fieldName} | {field.fieldType.name} | {field.categorical} | {dataset[field.fieldName].dtype} |\n")

        report.write(f"\nRows: {len(dataset)  }\n\n")


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(RuntimeMetadataReport, "config/runtime_metadata.yaml", "config/sys_config.yaml")
