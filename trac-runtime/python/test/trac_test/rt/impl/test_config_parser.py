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
import pathlib

import trac.rt.config as cfg
import trac.rt.impl.config_parser as cfg_p
import trac.rt.impl.util as util
import trac.rt.exceptions as ex


ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../../../..") \
    .resolve()

PYTHON_EXAMPLES_DIR = ROOT_DIR \
    .joinpath("examples/models/python")


class ConfigParserTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()

    def test_example_sys_config_ok(self):

        parser = cfg_p.ConfigParser(cfg.SystemConfig)

        raw_config_path = PYTHON_EXAMPLES_DIR.joinpath("sys_config.yaml")
        raw_config = parser.load_raw_config(raw_config_path, "system")
        sys_config = parser.parse(raw_config, raw_config_path.name)

        self.assertIsInstance(sys_config, cfg.SystemConfig)

    def test_example_job_config_ok(self):

        parser = cfg_p.ConfigParser(cfg.JobConfig)

        raw_config_path = PYTHON_EXAMPLES_DIR.joinpath("hello_pandas/hello_pandas.yaml")
        raw_config = parser.load_raw_config(raw_config_path, "job")
        job_config = parser.parse(raw_config, raw_config_path.name)

        self.assertIsInstance(job_config, cfg.JobConfig)

    def test_invalid_path(self):

        parser = cfg_p.ConfigParser(cfg.SystemConfig)

        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(None))      # noqa
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(object()))  # noqa
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(""))
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config("$%^&*--`!"))
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config("pigeon://pigeon-svr/lovely-pigeon.pg"))

    def test_config_file_not_found(self):

        nonexistent_path = PYTHON_EXAMPLES_DIR.joinpath("nonexistent.yaml")

        parser = cfg_p.ConfigParser(cfg.SystemConfig)
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(nonexistent_path))

    def test_config_file_is_a_folder(self):

        parser = cfg_p.ConfigParser(cfg.SystemConfig)
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(PYTHON_EXAMPLES_DIR))
