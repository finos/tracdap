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
import pandas as pd

import tracdap.rt.api as trac


class DynamicSchemaInspection(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return dict()

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        return { "source_data": trac.define_input_table(dynamic=True, label="Unknown source dataset") }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        source_data_schema = trac.define_output_table(
            trac.F("column_name", trac.STRING, label="Column name"),
            trac.F("column_type", trac.STRING, label="TRAC column type"),
            trac.F("contains_nulls", trac.BOOLEAN, label="Whether the source column contains nulls"),
            label="Schema information for the source dataset")

        return { "source_data_schema": source_data_schema }

    def run_model(self, ctx: trac.TracContext):

        source_data = ctx.get_pandas_table("source_data")   # Source data as a regular dataframe
        source_schema = ctx.get_schema("source_data")       # TRAC schema for the source data

        # Get the column names and types discovered by TRAC
        columns = source_schema.table.fields
        column_names = [col.fieldName for col in columns]
        column_types = [col.fieldType.name for col in columns]

        # Use discovered column names to inspect the data
        contains_nulls = [source_data[col].isnull().values.any() for col in column_names]

        # Save results as a regular dataframe
        result = pd.DataFrame({
            "column_name": column_names,
            "column_type": column_types,
            "contains_nulls": contains_nulls})

        ctx.put_pandas_table("source_data_schema", result)


class DynamicGenerator(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("number_of_rows", trac.INTEGER, label="How many rows of data to generate"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        sample_columns = trac.define_input_table(
            trac.F("column_name", trac.STRING, label="Column name"),
            label="List of columns for generating sample data")

        return { "sample_columns": sample_columns }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        return { "dynamic_data_sample": trac.define_output_table(dynamic=True, label="Dynamically generated sample") }

    def run_model(self, ctx: trac.TracContext):

        sample_columns = ctx.get_pandas_table("sample_columns")
        number_of_rows = ctx.get_parameter("number_of_rows")

        sample_fields = []
        sample_data = dict()

        for column_index, column_name in enumerate(sample_columns["column_name"]):

            field_schema = trac.define_field(column_name, trac.INTEGER, label=f"Generated column {column_name}")
            sample_fields.append(field_schema)

            offset = column_index * number_of_rows
            column_values = range(offset, offset + number_of_rows)
            sample_data[column_name] = column_values

        sample_schema = trac.define_schema(sample_fields, schema_type=trac.SchemaType.TABLE_SCHEMA)

        ctx.put_schema("dynamic_data_sample", sample_schema)
        ctx.put_pandas_table("dynamic_data_sample", pd.DataFrame(sample_data))


class DynamicDataFilter(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("filter_column", trac.STRING, label="Filter colum"),
            trac.P("filter_value", trac.STRING, label="Filter value"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        return { "original_data": trac.define_input_table(dynamic=True, label="Original (unfiltered) data") }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        return { "filtered_data": trac.define_output_table(dynamic=True, label="Filtered (output) data") }

    def run_model(self, ctx: trac.TracContext):

        original_schema = ctx.get_schema("original_data")
        original_data = ctx.get_pandas_table("original_data")

        filter_column = ctx.get_parameter("filter_column")
        filter_value = ctx.get_parameter("filter_value")

        filtered_data = original_data[original_data[filter_column] != filter_value]

        ctx.put_schema("filtered_data", original_schema)
        ctx.put_pandas_table("filtered_data", filtered_data)
