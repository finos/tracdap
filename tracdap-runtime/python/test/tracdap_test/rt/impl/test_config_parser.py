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

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt._impl.config_parser as cfg_p  # noqa
import tracdap.rt._impl.util as util  # noqa
import tracdap.rt._exec.dev_mode as dev_mode  # noqa


ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../../../..") \
    .resolve()

PYTHON_EXAMPLES_DIR = ROOT_DIR \
    .joinpath("examples/models/python")


class ConfigParserTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()
        # Ensure the plugin for local config is loaded
        import tracdap.rt._plugins.config_local  # noqa

    def test_example_sys_config_ok(self):

        config_file = PYTHON_EXAMPLES_DIR.joinpath("config/sys_config.yaml").resolve()
        config_mgr = cfg_p.ConfigManager.for_root_config(config_file)

        sys_config = config_mgr.load_root_object( cfg.RuntimeConfig, config_file_name="system")

        self.assertIsInstance(sys_config, cfg.RuntimeConfig)

    def test_example_job_config_ok(self):

        # Sample job config uses dev mode configuration, so supply DEV_MODE_JOB_CONFIG

        config_mgr = cfg_p.ConfigManager.for_root_dir(PYTHON_EXAMPLES_DIR)

        job_config = config_mgr.load_config_object(
            "config/using_data.yaml", cfg.JobConfig,
            dev_mode_locations=dev_mode.DEV_MODE_JOB_CONFIG,
            config_file_name="job")

        self.assertIsInstance(job_config, cfg.JobConfig)

    def test_empty_sys_config_ok(self):

        with tempfile.TemporaryDirectory() as td:

            config_mgr = cfg_p.ConfigManager.for_root_dir(td)

            yaml_path = pathlib.Path(td).joinpath("empty.yaml")
            yaml_path.touch()

            sys_config = config_mgr.load_config_object("empty.yaml", cfg.RuntimeConfig, None, "system")
            self.assertIsInstance(sys_config, cfg.RuntimeConfig)

            json_path = pathlib.Path(td).joinpath("empty.json")
            json_path.write_text("{}")

            sys_config =  config_mgr.load_config_object("empty.json", cfg.RuntimeConfig, None, "system")
            self.assertIsInstance(sys_config, cfg.RuntimeConfig)

    def test_empty_job_config_ok(self):

        with tempfile.TemporaryDirectory() as td:

            config_mgr = cfg_p.ConfigManager.for_root_dir(td)

            yaml_path = pathlib.Path(td).joinpath("empty.yaml")
            yaml_path.touch()

            job_config = config_mgr.load_config_object("empty.yaml", cfg.JobConfig, None, "job")
            self.assertIsInstance(job_config, cfg.JobConfig)

            json_path = pathlib.Path(td).joinpath("empty.json")
            json_path.write_text("{}")

            job_config = config_mgr.load_config_object("empty.json", cfg.JobConfig, None, "job")
            self.assertIsInstance(job_config, cfg.JobConfig)

    def test_invalid_path(self):

        config_mgr = cfg_p.ConfigManager.for_root_dir(PYTHON_EXAMPLES_DIR)

        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_file(None))      # noqa
        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_file(object()))  # noqa
        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_file(""))
        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_file("$%^&*--`!"))
        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_file("pigeon://pigeon-svr/lovely-pigeon.pg"))

        # The same error should come for loading objects
        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_object("$%^&*--`!", cfg.RuntimeConfig))

    def test_config_file_not_found(self):

        nonexistent_path = PYTHON_EXAMPLES_DIR.joinpath("nonexistent.yaml")

        config_mgr = cfg_p.ConfigManager.for_root_dir(PYTHON_EXAMPLES_DIR)

        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_file(nonexistent_path))
        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_object(nonexistent_path, cfg.RuntimeConfig))

    def test_config_file_is_a_folder(self):

        config_mgr = cfg_p.ConfigManager.for_root_dir(PYTHON_EXAMPLES_DIR)

        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_file("config"))
        self.assertRaises(ex.EConfigLoad, lambda: config_mgr.load_config_object("config", cfg.RuntimeConfig))

    def test_config_file_garbled(self):

        noise_bytes = 256
        noise = bytearray(random.getrandbits(8) for _ in range(noise_bytes))

        with tempfile.TemporaryDirectory() as td:

            config_mgr = cfg_p.ConfigManager.for_root_dir(td)

            yaml_path = pathlib.Path(td).joinpath("garbled.yaml")
            yaml_path.write_bytes(noise)

            self.assertRaises(ex.EConfigParse, lambda: config_mgr.load_config_object(yaml_path, cfg.RuntimeConfig))

            json_path = pathlib.Path(td).joinpath("garbled.json")
            json_path.write_bytes(noise)

            self.assertRaises(ex.EConfigParse, lambda: config_mgr.load_config_object(json_path, cfg.RuntimeConfig))

            # Should not throw if not parsing the contents
            config_mgr.load_config_file("garbled.yaml")
            config_mgr.load_config_file("garbled.json")

    def test_config_file_wrong_format(self):

        with tempfile.TemporaryDirectory() as td:

            config_mgr = cfg_p.ConfigManager.for_root_dir(td)

            # Write YAML into a JSON file

            json_path = pathlib.Path(td).joinpath("garbled.json")
            json_path.write_text('foo: bar\n')

            self.assertRaises(ex.EConfigParse, lambda: config_mgr.load_config_object(json_path, cfg.RuntimeConfig))

            # Valid YAML can include JSON, so parsing JSON as YAML is not an error!

    def test_invalid_config(self):

        with tempfile.TemporaryDirectory() as td:

            config_mgr = cfg_p.ConfigManager.for_root_dir(td)

            # Config files with unknown config values

            yaml_path = pathlib.Path(td).joinpath("garbled.yaml")
            yaml_path.write_text('foo: bar\n')

            json_path = pathlib.Path(td).joinpath("garbled.json")
            json_path.write_text('{ "foo": "bar",\n  "bar": 1}')

            self.assertRaises(ex.EConfigParse, lambda: config_mgr.load_config_object(yaml_path, cfg.RuntimeConfig))
            self.assertRaises(ex.EConfigParse, lambda: config_mgr.load_config_object(json_path, cfg.RuntimeConfig))
            self.assertRaises(ex.EConfigParse, lambda: config_mgr.load_config_object(yaml_path, cfg.JobConfig))
            self.assertRaises(ex.EConfigParse, lambda: config_mgr.load_config_object(json_path, cfg.JobConfig))
