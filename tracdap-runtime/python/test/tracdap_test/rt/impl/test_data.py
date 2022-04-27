#  Copyright 2022 Accenture Global Solutions Limited
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

import datetime as dt
import decimal
import unittest

import pyarrow as pa

import tracdap.rt.metadata as _meta
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.data as _data


class DataMappingTest(unittest.TestCase):

    @staticmethod
    def sample_schema():

        trac_schema = _meta.SchemaDefinition(
            _meta.SchemaType.TABLE,
            _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[
                _meta.FieldSchema("boolean_field", fieldType=_meta.BasicType.BOOLEAN),
                _meta.FieldSchema("integer_field", fieldType=_meta.BasicType.INTEGER),
                _meta.FieldSchema("float_field", fieldType=_meta.BasicType.FLOAT),
                _meta.FieldSchema("decimal_field", fieldType=_meta.BasicType.DECIMAL),
                _meta.FieldSchema("string_field", fieldType=_meta.BasicType.STRING),
                _meta.FieldSchema("date_field", fieldType=_meta.BasicType.DATE),
                _meta.FieldSchema("datetime_field", fieldType=_meta.BasicType.DATETIME),
            ]))

        return _types.trac_to_arrow_schema(trac_schema)

    @staticmethod
    def sample_data():

        return {
            "boolean_field": [True, False, True, False],
            "integer_field": [1, 2, 3, 4],
            "float_field": [1.0, 2.0, 3.0, 4.0],
            "decimal_field": [decimal.Decimal(1.0), decimal.Decimal(2.0), decimal.Decimal(3.0), decimal.Decimal(4.0)],
            "string_field": ["hello", "world", "what's", "up"],
            "date_field": [dt.date(2000, 1, 1), dt.date(2000, 1, 2), dt.date(2000, 1, 3), dt.date(2000, 1, 4)],
            "datetime_field": [
                dt.datetime(2000, 1, 1, 0, 0, 0), dt.datetime(2000, 1, 2, 1, 1, 1),
                dt.datetime(2000, 1, 3, 2, 2, 2), dt.datetime(2000, 1, 4, 3, 3, 3)]
        }

    def test_round_trip_basic(self):

        sample_schema = self.sample_schema()
        sample_data = self.sample_data()

        table = pa.Table.from_pydict(sample_data, sample_schema)  # noqa
        df = _data.DataMapping.arrow_to_pandas(table)

        # TODO: Check schema on DF, i.e. at midway point?

        rt = _data.DataMapping.pandas_to_arrow(df, self.sample_schema())

        self.assertEqual(sample_schema, rt.schema)
        self.assertEqual(table, rt)

    def test_round_trip_nulls(self):

        sample_schema = self.sample_schema()
        sample_data = self.sample_data()

        for col, values in sample_data.items():
            values[0] = None

        table = pa.Table.from_pydict(sample_data, sample_schema)  # noqa
        df = _data.DataMapping.arrow_to_pandas(table)

        # TODO: Check schema on DF, i.e. at midway point?

        rt = _data.DataMapping.pandas_to_arrow(df, self.sample_schema())

        self.assertEqual(sample_schema, rt.schema)
        self.assertEqual(table, rt)

    def test_round_trip_edge_cases(self):
        pass

    def test_coercion_decimal(self):
        pass

    def test_coercion_datetime(self):
        pass
