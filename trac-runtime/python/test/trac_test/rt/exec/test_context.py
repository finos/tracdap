#  Copyright 2021 Accenture Global Solutions Limited
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

import unittest


class TracContextTest(unittest.TestCase):

    """
    Test core functionality and error handling in the main TracContext class
    This test does not cover data conformity, which is covered by a separate test case
    TracContext is a fairly thin layer over a set of get/put operations,
    so the tests mainly cover runtime validation of parameters to those operations
    """

    # Getting params

    def test_get_parameter_ok(self):
        pass

    def test_get_parameter_types(self):
        pass

    def test_get_parameter_name_is_null(self):
        pass

    def test_get_parameter_name_invalid(self):
        pass

    def test_get_parameter_name_reserved(self):
        pass

    def test_get_parameter_unknown(self):
        pass

    # Getting tables

    def test_get_schema_ok(self):
        pass

    def test_get_schema_dynamic(self):
        pass

    def test_get_table_pandas_ok(self):
        pass

    def test_get_table_dynamic_schema(self):
        pass

    def test_get_table_pandas_conversion(self):
        pass

    def test_get_table_output_before_put(self):
        pass

    def test_get_table_name_is_null(self):
        pass

    def test_get_table_name_invalid(self):
        pass

    def test_get_table_name_reserved(self):
        pass

    def test_get_table_unknown(self):
        pass

    # Putting tables

    def test_put_schema_ok(self):
        pass

    def test_put_schema_not_dynamic(self):
        pass

    def test_put_schema_for_input(self):
        pass

    def put_table_pandas_ok(self):
        pass

    def put_table_pandas_dynamic_schema(self):
        pass

    def put_table_pandas_null(self):
        pass

    def put_table_pandas_not_a_dataframe(self):
        pass

    def put_table_pandas_no_rows(self):
        pass

    def test_put_table_name_is_null(self):
        pass

    def test_put_table_name_invalid(self):
        pass

    def test_put_table_name_reserved(self):
        pass

    def test_put_table_unknown(self):
        pass

    # Misc extra tests

    def test_get_log(self):
        pass
