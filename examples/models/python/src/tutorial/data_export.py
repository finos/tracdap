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

import tracdap.rt.api.experimental as trac

import tutorial.schemas as schemas


class DataExportExample(trac.TracDataExport):

    def define_parameters(self):

        return trac.define_parameters(
            trac.P("storage_key", trac.STRING, "TRAC external storage key"),
            trac.P("no_overwrite", trac.BOOLEAN, "Do not overwrite any files that already exist", default_value=False),
            trac.P("export_comment", trac.STRING, "Comment for this export operation (optional)", default_value=""))

    def define_inputs(self):

        profit_by_region = trac.load_schema(schemas, "profit_by_region.csv")

        return {"profit_by_region": trac.ModelInputSchema(profit_by_region)}

    def run_model(self, ctx: trac.TracDataContext):

        # Get a dataset - set up as inputs the same as model run jobs
        profit_by_region = ctx.get_pandas_table("profit_by_region")

        # Add some fields to label the export
        pbr_augmented = self.add_export_metadata("profit_by_region", profit_by_region, ctx)

        # Get access to the storage interface - must be in the TRAC config as a storage location
        storage_key = ctx.get_parameter("storage_key")
        storage = ctx.get_file_storage(storage_key)

        # Storage path, relative to the root of the storage location
        export_dir = "data_export_example"
        export_file = export_dir + "/profit_by_region.csv"

        storage.mkdir(export_dir, recursive=True)

        no_overwrite = ctx.get_parameter("no_overwrite")

        if no_overwrite and storage.exists(export_file):
            ctx.log().info(f"Export of [{export_file}] will be skipped because the file already exists")
            return

        with storage.write_byte_stream(export_file) as stream:
            pbr_augmented.to_csv(stream, index=False)

    @staticmethod
    def add_export_metadata(dataset_name, dataset, ctx: trac.TracDataContext):

        export_comment = ctx.get_parameter("export_comment")

        return dataset.assign(
            dataset_name=dataset_name,
            comments=export_comment)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(DataExportExample, "config/data_export.yaml", "config/sys_config.yaml")
