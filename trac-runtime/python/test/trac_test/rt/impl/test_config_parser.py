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
import tempfile
import random

import trac.rt.config as cfg
import trac.rt.impl.config_parser as cfg_p
import trac.rt.impl.util as util
import trac.rt.exec.dev_mode as dev_mode
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

        parser = cfg_p.ConfigParser(cfg.RuntimeConfig)

        raw_config_path = PYTHON_EXAMPLES_DIR.joinpath("sys_config.yaml")
        raw_config = parser.load_raw_config(raw_config_path, "system")
        sys_config = parser.parse(raw_config, raw_config_path.name)

        self.assertIsInstance(sys_config, cfg.RuntimeConfig)

    def test_example_job_config_ok(self):

        # Sample job config uses dev mode configuration, so supply DEV_MODE_JOB_CONFIG

        parser = cfg_p.ConfigParser(cfg.JobConfig, dev_mode.DEV_MODE_JOB_CONFIG)

        raw_config_path = PYTHON_EXAMPLES_DIR.joinpath("using_data/using_data.yaml")
        raw_config = parser.load_raw_config(raw_config_path, "job")
        job_config = parser.parse(raw_config, raw_config_path.name)

        self.assertIsInstance(job_config, cfg.JobConfig)

    def test_empty_sys_config_ok(self):

        parser = cfg_p.ConfigParser(cfg.RuntimeConfig)

        with tempfile.TemporaryDirectory() as td:

            yaml_path = pathlib.Path(td).joinpath("empty.yaml")
            yaml_path.touch()

            raw_config = parser.load_raw_config(yaml_path, "system")
            sys_config = parser.parse(raw_config, yaml_path.name)
            self.assertIsInstance(sys_config, cfg.RuntimeConfig)

            json_path = pathlib.Path(td).joinpath("empty.json")
            json_path.write_text("{}")

            raw_config = parser.load_raw_config(json_path, "system")
            sys_config = parser.parse(raw_config, json_path.name)
            self.assertIsInstance(sys_config, cfg.RuntimeConfig)

    def test_empty_job_config_ok(self):

        parser = cfg_p.ConfigParser(cfg.JobConfig)

        with tempfile.TemporaryDirectory() as td:

            yaml_path = pathlib.Path(td).joinpath("empty.yaml")
            yaml_path.touch()

            raw_config = parser.load_raw_config(yaml_path, "job")
            job_config = parser.parse(raw_config, yaml_path.name)
            self.assertIsInstance(job_config, cfg.JobConfig)

            json_path = pathlib.Path(td).joinpath("empty.json")
            json_path.write_text("{}")

            raw_config = parser.load_raw_config(json_path, "job")
            job_config = parser.parse(raw_config, yaml_path.name)
            self.assertIsInstance(job_config, cfg.JobConfig)

    def test_invalid_path(self):

        parser = cfg_p.ConfigParser(cfg.RuntimeConfig)

        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(None))      # noqa
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(object()))  # noqa
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(""))
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config("$%^&*--`!"))
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config("pigeon://pigeon-svr/lovely-pigeon.pg"))

    def test_config_file_not_found(self):

        nonexistent_path = PYTHON_EXAMPLES_DIR.joinpath("nonexistent.yaml")

        parser = cfg_p.ConfigParser(cfg.RuntimeConfig)
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(nonexistent_path))

    def test_config_file_is_a_folder(self):

        parser = cfg_p.ConfigParser(cfg.RuntimeConfig)
        self.assertRaises(ex.EConfigLoad, lambda: parser.load_raw_config(PYTHON_EXAMPLES_DIR))

    def test_config_file_garbled(self):

        parser = cfg_p.ConfigParser(cfg.RuntimeConfig)

        noise_bytes = 256
        noise = bytearray(random.getrandbits(8) for _ in range(noise_bytes))

        with tempfile.TemporaryDirectory() as td:

            yaml_path = pathlib.Path(td).joinpath("garbled.yaml")
            yaml_path.write_bytes(noise)

            self.assertRaises(ex.EConfigParse, lambda: parser.load_raw_config(yaml_path))

            json_path = pathlib.Path(td).joinpath("garbled.json")
            json_path.write_bytes(noise)

            self.assertRaises(ex.EConfigParse, lambda: parser.load_raw_config(json_path))

    def test_config_file_wrong_format(self):

        parser = cfg_p.ConfigParser(cfg.RuntimeConfig)

        with tempfile.TemporaryDirectory() as td:

            # Write YAML into a JSON file

            json_path = pathlib.Path(td).joinpath("garbled.json")
            json_path.write_text('foo: bar\n')

            self.assertRaises(ex.EConfigParse, lambda: parser.load_raw_config(json_path))

            # Valid YAML can include JSON, so parsing JSON as YAML is not an error!

    def test_invalid_config(self):

        with tempfile.TemporaryDirectory() as td:

            # Config files with unknown config values

            yaml_path = pathlib.Path(td).joinpath("garbled.yaml")
            yaml_path.write_text('foo: bar\n')

            json_path = pathlib.Path(td).joinpath("garbled.json")
            json_path.write_text('{ "foo": "bar",\n  "bar": 1}')

            sys_parser = cfg_p.ConfigParser(cfg.RuntimeConfig)
            job_parser = cfg_p.ConfigParser(cfg.JobConfig)

            sys_yaml_config = sys_parser.load_raw_config(yaml_path)
            sys_json_config = sys_parser.load_raw_config(json_path)
            job_yaml_config = job_parser.load_raw_config(yaml_path)
            job_json_config = job_parser.load_raw_config(json_path)

            self.assertRaises(ex.EConfigParse, lambda: sys_parser.parse(sys_yaml_config))
            self.assertRaises(ex.EConfigParse, lambda: sys_parser.parse(sys_json_config))
            self.assertRaises(ex.EConfigParse, lambda: job_parser.parse(job_yaml_config))
            self.assertRaises(ex.EConfigParse, lambda: job_parser.parse(job_json_config))
