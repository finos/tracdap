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
import typing as tp

import unittest


class SampleModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.declare_parameters(
            trac.P("param1", trac.BasicType.INTEGER, label="A first parameter"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        input1 = trac.declare_input_table(
            trac.F("field1", trac.BasicType.INTEGER, "Something about this field"),
            trac.F("field2", trac.BasicType.FLOAT, "Something about this other field"))

        return {"input1": input1}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        output1 = trac.declare_output_table(
            trac.F("field1", trac.BasicType.INTEGER, "Something about this field"),
            trac.F("field2", trac.BasicType.FLOAT, "Something about this other field"))

        return {"output1": output1}

    def run_model(self, ctx: trac.TracContext):

        pass


class ModelApiTest(unittest.TestCase):

    def setUp(self):

        self.sample_model = SampleModel()

    def test_sample_model(self):

        params = self.sample_model.define_parameters()

        self.assertIsInstance(params, dict)
        self.assertEqual(len(params), 1)
        self.assertIsInstance(next(iter(params.values())), trac.ModelParameter)


if __name__ == "__main__":
    unittest.main()
