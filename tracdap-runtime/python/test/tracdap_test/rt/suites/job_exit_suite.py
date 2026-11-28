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

import json
import os
import pathlib
import platform
import shutil
import signal
import subprocess as sp
import sys
import tempfile
import time
import typing as tp
import unittest

import yaml

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.metadata as meta
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.plugins as plugins  # noqa
import tracdap.rt._impl.core.storage as storage  # noqa
import tracdap.rt._impl.core.util as util  # noqa

import tracdap_test.rt.jobs.job_exit_driver as driver
import tracdap_test.flow_scenarios.models as scenario_models


_TEST_DIR = pathlib.Path(__file__).parents[3]
_RUNTIME_DIR = _TEST_DIR.parent
_DRIVER = pathlib.Path(driver.__file__)
_ACCOUNTS_CSV = pathlib.Path(scenario_models.__file__).parent.joinpath("data/accounts.csv")

_MODEL_LINE = "Progress model [SlowCount] ran"
_JOB_STORE = "job_store"


class JobExitTestSuite:

    """
    Run a run-flow job as its own process and check the process ends, with job progress on

    The snapshot writer is a daemon thread, so process exit never waits for it, even while a write is in progress.
    Storage clients clean up at interpreter exit, which can happen while the writer is still writing.
    Each storage test class supplies the storage that the job's results and progress snapshot are written to.
    """

    TIME_LIMIT = 60.0
    WAIT_SECONDS = 6.0

    assertEqual: tp.Callable
    assertTrue: tp.Callable
    assertNotEqual: tp.Callable
    assertLess: tp.Callable
    assertIn: tp.Callable
    fail: tp.Callable

    tmp_dir: pathlib.Path
    store_prefix: str

    def _job_store(self) -> meta.ResourceDefinition:
        raise NotImplementedError()

    def _prepare_job_store(self):
        pass

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()
        plugins.PluginManagerImpl.register_core_plugins()

    def setUp(self) -> None:

        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = pathlib.Path(self._tmp.name)
        self.store_prefix = f"job_exit_test_{util.generate_uuid7()}"

        self.tmp_dir.joinpath("data/inputs").mkdir(parents=True)
        shutil.copy(_ACCOUNTS_CSV, self.tmp_dir.joinpath("data/inputs/accounts.csv"))

        self._prepare_job_store()

        manager_config = cfg.RuntimeConfig(resources={_JOB_STORE: self._job_store()})
        manager_config.properties["storage.default.location"] = _JOB_STORE
        self.store = storage.StorageManager(manager_config).get_file_storage(_JOB_STORE)

    def tearDown(self) -> None:

        self._tmp.cleanup()

        try:
            if self.store.exists(self.store_prefix):
                self.store.rmdir(self.store_prefix)
        except Exception:  # noqa
            pass

    def _write_configs(self, wait_seconds: float) -> tp.Tuple[pathlib.Path, pathlib.Path]:

        sys_config = {
            "properties": {"storage.default.location": "job_data", "storage.default.format": "CSV"},
            "resources": {
                "job_data": {
                    "resourceType": "INTERNAL_STORAGE", "protocol": "LOCAL",
                    "properties": {"rootPath": str(self.tmp_dir.joinpath("data"))}},
                _JOB_STORE: self._resource_config(self._job_store())}}

        flow = {
            "nodes": {
                "accounts": {"nodeType": "INPUT_NODE"},
                "slow_count": {"nodeType": "MODEL_NODE", "inputs": ["accounts"], "outputs": ["account_count"]},
                "account_count": {"nodeType": "OUTPUT_NODE"}},
            "edges": [
                {"source": {"node": "accounts"}, "target": {"node": "slow_count", "socket": "accounts"}},
                {"source": {"node": "slow_count", "socket": "account_count"}, "target": {"node": "account_count"}}]}

        job_config = {
            "job": {"runFlow": {
                "flow": str(self.tmp_dir.joinpath("flow.yaml")),
                "parameters": {"wait_seconds": wait_seconds},
                "inputs": {"accounts": "inputs/accounts.csv"},
                "outputs": {"account_count": "outputs/account_count.csv"},
                "models": {"slow_count": "tracdap_test.rt.jobs.progress_models.SlowCount"}}},
            "properties": {
                "result.enabled": "true",
                "result.logs.enabled": "true",
                "result.storage.location": _JOB_STORE,
                "result.storage.path": f"{self.store_prefix}/trac_job_result.json",
                "result.progress.path": f"{self.store_prefix}/{driver.PROGRESS_FILE_NAME}",
                "result.format": "JSON"}}

        self.tmp_dir.joinpath("flow.yaml").write_text(yaml.safe_dump(flow))
        self.tmp_dir.joinpath("sys_config.yaml").write_text(yaml.safe_dump(sys_config))
        self.tmp_dir.joinpath("job_config.yaml").write_text(yaml.safe_dump(job_config))

        return self.tmp_dir.joinpath("sys_config.yaml"), self.tmp_dir.joinpath("job_config.yaml")

    @staticmethod
    def _resource_config(resource: meta.ResourceDefinition) -> dict:
        return {"resourceType": resource.resourceType.name, "protocol": resource.protocol, "properties": resource.properties}

    def _start_job(self, wait_seconds: float, hold_progress_write: bool = False) -> sp.Popen:

        sys_config, job_config = self._write_configs(wait_seconds)
        args = [sys.executable, str(_DRIVER), str(sys_config), str(job_config)]

        if hold_progress_write:
            args.append("--hold-progress-write")

        env = {**os.environ, "PYTHONPATH": os.pathsep.join(
            str(_RUNTIME_DIR.joinpath(folder)) for folder in ["generated", "src", "test"])}

        return sp.Popen(
            args, env=env, cwd=self.tmp_dir, stdout=sp.PIPE, stderr=sp.STDOUT, text=True,
            start_new_session=platform.system() != "Windows")

    def _wait_for_exit(self, process: sp.Popen, started: float) -> str:

        try:
            output, _ = process.communicate(timeout=max(1.0, started + self.TIME_LIMIT - time.monotonic()))
        except sp.TimeoutExpired:
            process.kill()
            output, _ = process.communicate()
            self.fail(f"The job process did not exit within {self.TIME_LIMIT} seconds\n{output}")

        self.assertTrue("Fatal Python error" not in output, output)

        return output

    def _read_store(self, file_name: str) -> tp.Optional[str]:

        storage_path = f"{self.store_prefix}/{file_name}"

        if not self.store.exists(storage_path):
            return None

        return self.store.read_bytes(storage_path).decode("utf-8")

    def _wait_for_model_line(self, process: sp.Popen, started: float):

        while time.monotonic() < started + self.TIME_LIMIT and process.poll() is None:

            try:
                snapshot = self._read_store(driver.PROGRESS_FILE_NAME)
                if snapshot and _MODEL_LINE in [entry["message"] for entry in json.loads(snapshot)["logEntries"]]:
                    return
            except (ValueError, KeyError, ex.EStorage):
                pass  # A snapshot read while it is being written

            time.sleep(0.2)

        self.fail("The model's log line was not written to the progress snapshot while the job ran")

    def test_held_snapshot_write_exits(self):

        started = time.monotonic()
        process = self._start_job(0.0, hold_progress_write=True)
        output = self._wait_for_exit(process, started)

        self.assertIn(driver.HELD_WRITE_MESSAGE, output)
        self.assertEqual(0, process.returncode, output)

        job_result = json.loads(self._read_store("trac_job_result.json"))
        self.assertEqual("SUCCEEDED", job_result["result"]["statusCode"])

    def _check_signal_exit(self, signal_number: int):

        if platform.system() == "Windows":
            raise unittest.SkipTest("Job signals are not tested on Windows")

        started = time.monotonic()
        process = self._start_job(self.WAIT_SECONDS)

        self._wait_for_model_line(process, started)
        process.send_signal(signal_number)

        output = self._wait_for_exit(process, started)

        # The process ends once the running model returns, without a result
        self.assertNotEqual(0, process.returncode, output)
        self.assertTrue(self._read_store("trac_job_result.json") is None)

    def test_sigint_exits(self):
        self._check_signal_exit(signal.SIGINT)

    def test_sigterm_exits(self):
        self._check_signal_exit(signal.SIGTERM)
