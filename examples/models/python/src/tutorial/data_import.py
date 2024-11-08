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
import tracdap.rt.api.experimental as trac

import pandas as pd
import pytz

import tutorial.schemas as schemas


class BulkDataImport(trac.TracDataImport):

    # trac_data_origin = import | upload | generated
    # trac_data_source = risk_dw
    # trac_data_key = economic_scenario
    # trac_data_category = standard | experimental | results

    trac.init_static()

    IMPORT_LOG_SCHEMA = trac.define_schema(
        trac.F("storage_path", trac.STRING, "Storage path", business_key=True),
        trac.F("file_name", trac.STRING, "File name"),
        trac.F("size", trac.INTEGER, "File size"),
        trac.F("mtime", trac.DATETIME, "Last modified time"))

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("storage_key", trac.STRING, "TRAC external storage key"),
            trac.P("business_date", trac.DATE, "Business date for imported datasets"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return dict()

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"import_log": trac.ModelOutputSchema(schema=self.IMPORT_LOG_SCHEMA)}

    def run_model(self, ctx: trac.TracDataContext):

        storage_key = ctx.get_parameter("storage_key")
        storage = ctx.get_file_storage(storage_key)
        root_dir = storage.stat(".")

        import_log = self.import_dir(ctx, storage, root_dir)

        ctx.put_pandas_table("import_log", pd.DataFrame(import_log))

    def import_dir(self, ctx: trac.TracDataContext, storage: trac.TracFileStorage, dir_info: trac.FileStat):

        import_log = []

        for entry in storage.ls(dir_info.storage_path):

            if entry.file_type == trac.FileType.DIRECTORY:
                log_entries = self.import_dir(ctx, storage, entry)
                import_log.extend(log_entries)
            else:
                log_entry = self.import_file(ctx, storage, entry)
                import_log.append(log_entry)

        return import_log

    def import_file(self, ctx: trac.TracDataContext, storage: trac.TracFileStorage, file: trac.FileStat):

        storage.read_bytes(file.storage_path)

        with storage.read_byte_stream(file.storage_path) as file_stream:

            storage_key = storage.get_storage_key()
            dataset_key = self.remove_extension(file.file_name)
            dataset = pd.read_parquet(file_stream)
            schema = trac.infer_schema(dataset)

            ctx.add_data_import(dataset_key)
            ctx.set_source_metadata(dataset_key, storage_key, file)
            ctx.set_schema(dataset_key, schema)

            business_date = ctx.get_parameter("business_date")
            ctx.set_attribute(dataset_key, "business_date", business_date)

            ctx.put_pandas_table(dataset_key, dataset)

        # Store modified time as un-zoned timestamp in UTC
        utc_mtime = file.mtime \
            .astimezone(pytz.UTC) \
            .replace(tzinfo=None)

        # Log entry for this file
        return {
            "storage_path": file.storage_path,
            "file_name": file.file_name,
            "size": file.size,
            "mtime": utc_mtime}

    @staticmethod
    def remove_extension(filename):

        if "." in filename:
            return filename.rsplit(".", 1)[0].replace(".", "_")
        else:
            return filename


class SelectiveDataImport(trac.TracDataImport):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("storage_key", trac.STRING, "TRAC external storage key"),
            trac.P("table_names", trac.array_type(trac.STRING), "List of tables to import"))

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        # No pre-defined outputs
        return dict()

    def run_model(self, ctx: trac.TracDataContext):

        storage_key = ctx.get_parameter("storage_key")
        storage = ctx.get_data_storage(storage_key, trac.PANDAS)

        table_names = ctx.get_parameter("table_names")

        for table_name in table_names:

            if storage.has_table(table_name):

                table = storage.read_table(table_name)
                schema = trac.infer_schema(table)

                ctx.add_data_import(table_name)
                ctx.set_source_metadata(table_name, storage_key, table_name)
                ctx.set_schema(table_name, schema)

                ctx.put_table(table_name, table)

            else:

                ctx.log().warning(f"Requested table [{table_name}] not found in storage [{storage_key}]")


class SimpleDataImport(trac.TracDataImport):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("storage_key", trac.STRING, "TRAC external storage key"),
            trac.P("source_file", trac.STRING, "Path of the source data file in external storage"))

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        customer_loans = trac.load_schema(schemas, "customer_loans.csv")

        return {"customer_loans": trac.ModelOutputSchema(customer_loans)}

    def run_model(self, ctx: trac.TracDataContext):

        storage_key = ctx.get_parameter("storage_key")
        storage = ctx.get_file_storage(storage_key)

        storage_file = ctx.get_parameter("source_file")

        with storage.read_byte_stream(storage_file) as file_stream:

            dataset = pd.read_parquet(file_stream)
            ctx.put_pandas_table("customer_loans", dataset)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(BulkDataImport, "config/data_import.yaml", "config/sys_config.yaml")
