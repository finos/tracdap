#  Copyright 2024 Accenture Global Solutions Limited
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

import tutorial.schemas as schemas


class DataExportExample(trac.TracDataExport):

    def define_parameters(self):

        return trac.define_parameters(
            trac.P("clear_tables", trac.BOOLEAN, "Clear down existing content in the export tables", default_value=False),
            trac.P("export_comment", trac.STRING, "Comment for this export operation (optional)", default_value=""))

    def define_inputs(self):

        profit_by_region = trac.load_schema(schemas, "profit_by_region.csv")

        return {"profit_by_region": trac.ModelInputSchema(profit_by_region)}

    def run_export(self, ctx: trac.TracDataContext):

        # Get a dataset - set up as inputs the same as model run jobs
        profit_by_region = ctx.get_pandas_table("profit_by_region")

        # Add some fields to label the export
        pbr_augmented = self.add_export_metadata("profit_by_region", profit_by_region, ctx)

        # Get access to the storage interface - must be in the TRAC config as a storage location
        db_storage = ctx.get_data_storage("risk_dw")

        # Optional - clear out existing data if the clear_tables flag is set
        clear_tables = ctx.get_parameter("clear_tables")

        if clear_tables and db_storage.has_table("profit_by_region"):
            db_storage.clear_table("profit_by_region")

        # Write data into the table, create the table if required, append if the table already has data
        db_storage.write_table("profit_by_region", pbr_augmented, create_if_missing=True)

    def run_export_2(self, ctx: trac.TracDataContext):

        profit_by_region = ctx.get_pandas_table("profit_by_region")
        pbr_augmented = self.add_export_metadata("profit_by_region", profit_by_region, ctx)

        # Get access to the storage interface - must be in the TRAC config as a storage location
        cloud_storage = ctx.get_file_storage("risk_cloud_storage")

        # Storage path, relative to the root of the storage location
        storage_path = "risk_data/profit_by_region"
        file_name = storage_path + "/" + ctx.get_metadata("trac_job", "job_id")

        cloud_storage.mkdir(storage_path, recursive=True)

        with cloud_storage.write_byte_stream(file_name) as stream:
            pbr_augmented.to_parquet(stream)

    @staticmethod
    def add_export_metadata(dataset_name, dataset, ctx: trac.TracDataContext):

        pbr_update_time = ctx.get_metadata(dataset_name, "trac_update_time")
        pbr_update_job = ctx.get_metadata(dataset_name, "trac_update_job")

        export_job = ctx.get_metadata("trac_job", "job_id")

        business_division = ctx.get_metadata(dataset_name, "business_division")

        export_comment = ctx.get_parameter("export_comment")

        return dataset.assign(
            modified_time=pbr_update_time,
            trac_job=pbr_update_job,
            trac_export_job=export_job,
            business_division=business_division,
            comments=export_comment)
