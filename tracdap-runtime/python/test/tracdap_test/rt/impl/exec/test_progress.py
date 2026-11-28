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
import logging
import pathlib
import re
import threading
import time
import typing as tp
import unittest
import unittest.mock as mock

import yaml

import tracdap.rt.metadata as meta
import tracdap.rt._impl.core.config_parser as cfg_p  # noqa
import tracdap.rt._impl.exec.graph as graph  # noqa
import tracdap.rt._impl.exec.progress as progress  # noqa

import tracdap_test.flow_scenarios.models as scenario_models


NODE_LOGGER = progress.NODE_LOGGER_NAME
LOG_FIXTURES = pathlib.Path(scenario_models.__file__).parent.joinpath("logs")

# PlainFormatter lines: asctime [threadName] levelname name - message
_SAVED_LOG_LINE = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d+ \[[^]]+] \w+ (\S+) - (.*)$")
MODEL_STEP = (meta.JobStepType.STEP_MODEL, "")
LOAD_STEP = (meta.JobStepType.STEP_LOAD, "accounts")


def _record(
        name: str, msg: str, level: int = logging.INFO,
        step: tp.Optional[tp.Tuple[meta.JobStepType, str]] = None, event: tp.Optional[str] = None,
        thread: int = 1, exc_info=None) -> logging.LogRecord:

    record = logging.LogRecord(name, level, __file__, 0, msg, None, exc_info)
    record.thread = thread

    if step is not None:
        setattr(record, progress.STEP_TYPE_ATTR, step[0])
        setattr(record, progress.STEP_NAME_ATTR, step[1])
        setattr(record, progress.STEP_EVENT_ATTR, event)

    return record


def _step_record(event: str, step=MODEL_STEP, thread: int = 1, level=None) -> logging.LogRecord:

    func_type = {meta.JobStepType.STEP_MODEL: "RunModel", meta.JobStepType.STEP_LOAD: "LoadData"}[step[0]]
    level = level or (logging.ERROR if event == progress.STEP_FAILED else logging.INFO)

    return _record(NODE_LOGGER, f"{event} {func_type} [{step[1] or 'Model'}] / JOB-x", level, step, event, thread)


class _MemoryStorage:

    def __init__(self):
        self.writes: tp.List[tp.Tuple[str, bytes, str]] = []
        self.lock = threading.Lock()

    def write_bytes(self, storage_path: str, data: bytes):
        with self.lock:
            self.writes.append((storage_path, data, threading.current_thread().name))

    def last(self) -> dict:
        with self.lock:
            return json.loads(self.writes[-1][1].decode("utf-8"))


class LogSourceRuleTest(unittest.TestCase):

    def test_step_lines(self):

        for step_type in ["RunModel", "LoadData", "SaveData", "CaptureFile", "DecodeTable", "PreparePlacement", "PlaceFile"]:
            for event in ["START", "DONE", "FAILED"]:
                self.assertEqual(
                    meta.JobLogSource.LOG_SOURCE_STEP,
                    progress.log_source(NODE_LOGGER, f"{event} {step_type} [x] / JOB-x"))

    def test_plumbing_lines_are_engine(self):

        for message in ["SET Value [x] / JOB-x", "MAP DataView [a] -> [b] / JOB-x", "START ImportModel [x] / JOB-x",
                        "RESULT DataView [x] / JOB-x", "EVICT RunModel [x] / JOB-x", "PUSH DataView [a] -> [b] / x"]:
            self.assertEqual(meta.JobLogSource.LOG_SOURCE_ENGINE, progress.log_source(NODE_LOGGER, message))

    def test_capture_and_placement_detail_lines(self):

        self.assertEqual(
            meta.JobLogSource.LOG_SOURCE_STEP,
            progress.log_source("tracdap.rt._impl.core.capture.CaptureReader", "Captured [x] from [s] at [p]"))
        self.assertEqual(
            meta.JobLogSource.LOG_SOURCE_STEP,
            progress.log_source("tracdap.rt._impl.core.placement.PlacementWriter", "Placed [x] in [s] at [p]"))

    def test_shim_loggers_are_model(self):

        self.assertEqual(
            meta.JobLogSource.LOG_SOURCE_MODEL,
            progress.log_source("tracdap.shim._0.tutorial.hello_world.HelloWorldModel", "Hello"))

    def test_other_tracdap_loggers_are_engine(self):

        for logger_name in ["tracdap.rt._impl.exec.engine.GraphProcessor", "tracdap.rt._impl.exec.context.TracContextImpl",
                            "tracdap.rt._impl.exec.engine.JobProcessor"]:
            self.assertEqual(meta.JobLogSource.LOG_SOURCE_ENGINE, progress.log_source(logger_name, "Anything"))

    def test_platform_log_fixture(self):

        # The same saved log and classification are the fixture for the saved-log viewer in trac-ui
        saved_log = LOG_FIXTURES.joinpath("branch_failure.log").read_text(encoding="utf-8").splitlines()
        expected = yaml.safe_load(LOG_FIXTURES.joinpath("branch_failure.sources.yaml").read_text())

        entries = [match.groups() for match in map(_SAVED_LOG_LINE.match, saved_log) if match]
        sources = [progress.log_source(logger_name, message).name.replace("LOG_SOURCE_", "") for logger_name, message in entries]

        self.assertTrue(any(logger_name.startswith("tracdap.shim.") for logger_name, _ in entries))
        self.assertEqual(expected, sources)

    def test_other_loggers_are_model(self):

        for logger_name in ["tracdap_test.flow_scenarios.models.SumByRegion", "tutorial.hello_world.HelloWorldModel", "tracdap"]:
            self.assertEqual(meta.JobLogSource.LOG_SOURCE_MODEL, progress.log_source(logger_name, "Anything"))


class StepMappingTest(unittest.TestCase):

    def test_non_step_nodes(self):

        node_id = graph.NodeId("x", graph.NodeNamespace("JOB-x"), object)
        self.assertIsNone(progress.step_of(graph.IdentityNode(node_id, node_id)))
        self.assertIsNone(progress.step_extra(graph.IdentityNode(node_id, node_id), progress.STEP_START))


class ProgressHandlerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.handler = progress.ProgressHandler()

    def _handle(self, *records: logging.LogRecord):
        for record in records:
            self.handler.handle(record)

    def _steps(self) -> tp.Dict[tp.Tuple[meta.JobStepType, str], meta.JobStepStatus]:
        snapshot = self.handler.snapshot()
        return {(step.stepType, step.stepName): step for step in snapshot.steps}

    def test_step_moves_through_states(self):

        self._handle(_step_record(progress.STEP_START))
        step = self._steps()[MODEL_STEP]
        self.assertEqual(meta.JobStepStatusCode.STEP_RUNNING, step.statusCode)
        self.assertIsNotNone(step.startTime)
        self.assertIsNone(step.endTime)

        self._handle(_step_record(progress.STEP_DONE))
        step = self._steps()[MODEL_STEP]
        self.assertEqual(meta.JobStepStatusCode.STEP_SUCCEEDED, step.statusCode)
        self.assertIsNotNone(step.startTime)
        self.assertIsNotNone(step.endTime)

    def test_failed_and_skipped_steps(self):

        self._handle(_step_record(progress.STEP_START), _step_record(progress.STEP_FAILED))
        self._handle(_record("tracdap.rt._impl.exec.engine.GraphProcessor", "SKIP x", logging.WARNING,
                             LOAD_STEP, progress.STEP_SKIP, thread=2))

        steps = self._steps()
        self.assertEqual(meta.JobStepStatusCode.STEP_FAILED, steps[MODEL_STEP].statusCode)
        self.assertEqual(meta.JobStepStatusCode.STEP_SKIPPED, steps[LOAD_STEP].statusCode)
        self.assertIsNone(steps[LOAD_STEP].startTime)
        self.assertIsNotNone(steps[LOAD_STEP].endTime)

    def test_failed_is_final_in_either_order(self):

        for events in [[progress.STEP_DONE, progress.STEP_FAILED], [progress.STEP_FAILED, progress.STEP_DONE]]:
            with self.subTest(events=events):
                self.handler = progress.ProgressHandler()
                self._handle(_step_record(progress.STEP_START), *[_step_record(e, thread=3) for e in events])
                self.assertEqual(meta.JobStepStatusCode.STEP_FAILED, self._steps()[MODEL_STEP].statusCode)

    def test_step_not_started_has_no_entry(self):

        self._handle(_step_record(progress.STEP_START, LOAD_STEP))
        self.assertNotIn(MODEL_STEP, self._steps())

    def test_entries_attributed_to_running_step_on_thread(self):

        self._handle(
            _step_record(progress.STEP_START, thread=1),
            _record("tracdap.shim._0.models.Model", "model line", thread=1),
            _record("tracdap.shim._0.models.Model", "other thread", thread=2),
            _step_record(progress.STEP_DONE, thread=1),
            _record("tracdap.shim._0.models.Model", "after done", thread=1))

        entries = {entry.message: entry for entry in self.handler.snapshot().logEntries}

        self.assertEqual(meta.JobStepType.STEP_MODEL, entries["model line"].stepType)
        self.assertEqual("", entries["model line"].stepName)
        self.assertEqual(meta.JobLogSource.LOG_SOURCE_MODEL, entries["model line"].source)
        self.assertIsNone(entries["other thread"].stepType)
        self.assertIsNone(entries["after done"].stepType)

    def test_start_replaces_step_on_thread(self):

        self._handle(
            _step_record(progress.STEP_START, LOAD_STEP, thread=1),
            _step_record(progress.STEP_START, MODEL_STEP, thread=1),
            _record("tracdap.shim._0.models.Model", "model line", thread=1))

        entry = self.handler.snapshot().logEntries[-1]
        self.assertEqual(MODEL_STEP, (entry.stepType, entry.stepName))

    def test_failed_step_traceback_attributed(self):

        try:
            raise RuntimeError("Scenario model failure")
        except RuntimeError as e:
            error = e
            exc_info = (type(e), e, e.__traceback__)

        self._handle(
            _step_record(progress.STEP_START),
            _step_record(progress.STEP_FAILED),
            _record(NODE_LOGGER, str(error), logging.ERROR, MODEL_STEP, progress.STEP_FAILED, exc_info=exc_info))

        entry = self.handler.snapshot().logEntries[-1]

        self.assertEqual(MODEL_STEP, (entry.stepType, entry.stepName))
        self.assertEqual(meta.JobLogSource.LOG_SOURCE_ENGINE, entry.source)
        self.assertEqual("ERROR", entry.level)
        self.assertIn("Traceback", entry.message)
        self.assertIn("Scenario model failure", entry.message)

    def test_debug_records_left_out(self):

        logger = logging.Logger("tracdap.shim._0.models.Model", logging.DEBUG)
        logger.addHandler(self.handler)

        logger.debug("debug line")
        logger.info("info line")

        self.assertEqual(["info line"], [entry.message for entry in self.handler.snapshot().logEntries])

    def test_engine_entries_left_out_except_errors(self):

        self._handle(
            _record(NODE_LOGGER, "SET Value [x] / JOB-x"),
            _record("tracdap.rt._impl.exec.engine.GraphProcessor", "SKIP x", logging.WARNING),
            _record("tracdap.rt._impl.exec.engine.GraphProcessor", "engine error", logging.ERROR),
            _record("tracdap.rt._impl.exec.engine.JobProcessor", "engine critical", logging.CRITICAL))

        snapshot = self.handler.snapshot()

        self.assertEqual(["engine error", "engine critical"], [entry.message for entry in snapshot.logEntries])
        self.assertTrue(all(e.source == meta.JobLogSource.LOG_SOURCE_ENGINE for e in snapshot.logEntries))
        self.assertEqual(2, snapshot.totalLogEntries)

    def test_model_line_kept_through_engine_plumbing(self):

        self._handle(_record("tracdap.shim._0.models.Model", "first model line"))
        self._handle(*[_record(NODE_LOGGER, f"SET Value [p{i}] / JOB-x") for i in range(500)])

        self.assertEqual(["first model line"], [entry.message for entry in self.handler.snapshot().logEntries])

    def test_buffer_bounded_and_total_counted(self):

        self._handle(*[_record("tracdap.shim._0.models.Model", f"line {i}") for i in range(250)])
        self._handle(_record(NODE_LOGGER, "SET Value [x] / JOB-x"))

        snapshot = self.handler.snapshot()

        self.assertEqual(progress.ProgressHandler.MAX_LOG_ENTRIES, len(snapshot.logEntries))
        self.assertEqual("line 50", snapshot.logEntries[0].message)
        self.assertEqual("line 249", snapshot.logEntries[-1].message)
        self.assertEqual(250, snapshot.totalLogEntries)

    def test_long_entries_cut(self):

        self._handle(_record("tracdap.shim._0.models.Model", "x" * 5000))

        entry = self.handler.snapshot().logEntries[0]
        self.assertEqual(progress.ProgressHandler.MAX_ENTRY_LENGTH, len(entry.message))

    def test_snapshot_only_when_changed(self):

        self.assertIsNone(self.handler.snapshot())

        self._handle(_record("tracdap.shim._0.models.Model", "line"))
        self.assertTrue(self.handler.wake_up.is_set())
        self.assertIsNotNone(self.handler.snapshot())
        self.assertIsNone(self.handler.snapshot())

        self._handle(_record(NODE_LOGGER, "SET Value [x] / JOB-x"))
        self.assertIsNone(self.handler.snapshot())

    def test_handler_errors_dropped(self):

        logger = logging.Logger("tracdap.shim._0.models.Model")
        logger.addHandler(self.handler)

        with mock.patch.object(progress.ProgressHandler, "_process_record", side_effect=RuntimeError("handler failure")):
            logger.info("dropped line")

        logger.info("kept line")

        self.assertEqual(["kept line"], [entry.message for entry in self.handler.snapshot().logEntries])

    def test_snapshot_parses_back(self):

        try:
            raise RuntimeError("failure")
        except RuntimeError as e:
            exc_info = (type(e), e, e.__traceback__)

        self._handle(
            _step_record(progress.STEP_START, LOAD_STEP), _step_record(progress.STEP_DONE, LOAD_STEP),
            _step_record(progress.STEP_START), _record("tracdap.shim._0.models.Model", "model line ü"),
            _record(NODE_LOGGER, "failure", logging.ERROR, MODEL_STEP, progress.STEP_FAILED, exc_info=exc_info))

        snapshot = self.handler.snapshot()
        content = cfg_p.ConfigQuoter.quote_json(snapshot)
        parsed = cfg_p.ConfigParser(meta.JobProgress).parse(json.loads(content))

        self.assertEqual(snapshot, parsed)


class ProgressWriterTest(unittest.TestCase):

    def setUp(self) -> None:
        self.handler = progress.ProgressHandler()
        self.storage = _MemoryStorage()
        self.writer = progress.ProgressWriter(self.handler, self.storage, "1/2/3/trac_job_progress.json")

    def tearDown(self) -> None:
        self.writer.stop()
        self.writer.join(5)

    def _log(self, message: str):
        self.handler.handle(_record("tracdap.shim._0.models.Model", message))

    def _wait_for_writes(self, count: int, timeout: float = 5.0):
        deadline = time.monotonic() + timeout
        while len(self.storage.writes) < count and time.monotonic() < deadline:
            time.sleep(0.01)

    def test_writes_on_progress_thread(self):

        self.writer.start()
        self._log("line")
        self._wait_for_writes(1)

        path, _, thread_name = self.storage.writes[0]

        self.assertEqual("1/2/3/trac_job_progress.json", path)
        self.assertEqual("progress", thread_name)
        self.assertTrue(self.writer.daemon)

    def test_line_written_while_model_computes(self):

        self.writer.start()
        self._log("only line")
        self._wait_for_writes(1)

        self.assertEqual(["only line"], [e["message"] for e in self.storage.last()["logEntries"]])

    def test_writes_at_most_once_per_pause(self):

        with mock.patch.object(progress.ProgressWriter, "WRITE_PAUSE", 0.5):

            self.writer.start()

            started = time.monotonic()
            while time.monotonic() - started < 1.2:
                self._log("busy line")
                time.sleep(0.01)

            writes = len(self.storage.writes)

        # Three writes at most in 1.2 seconds: one at once, then one after each 0.5 second pause
        self.assertLessEqual(writes, 3)
        self.assertGreaterEqual(writes, 2)

    def test_newest_change_written(self):

        with mock.patch.object(progress.ProgressWriter, "WRITE_PAUSE", 0.3):

            self.writer.start()
            self._log("first")
            self._wait_for_writes(1)
            self._log("second")
            self._wait_for_writes(2)

        self.assertEqual(["first", "second"], [e["message"] for e in self.storage.last()["logEntries"]])

    def test_stop_returns_at_once_and_writes_final_change(self):

        self.writer.start()
        self._log("first")
        self._wait_for_writes(1)

        # Inside the pause: a change, then stop, which ends the pause
        self._log("last")

        started = time.monotonic()
        self.writer.stop()
        stop_time = time.monotonic() - started

        self.writer.join(1.0)

        self.assertLess(stop_time, 0.1)
        self.assertFalse(self.writer.is_alive())
        self.assertEqual(2, len(self.storage.writes))
        self.assertEqual("last", self.storage.last()["logEntries"][-1]["message"])

    def test_stop_without_changes_writes_nothing(self):

        self.writer.start()
        self.writer.stop()
        self.writer.join(1.0)

        self.assertFalse(self.writer.is_alive())
        self.assertEqual(0, len(self.storage.writes))

    def test_failed_write_retried_on_next_change(self):

        calls = []
        write_bytes = self.storage.write_bytes

        def flaky_write(storage_path, data):
            calls.append(data)
            if len(calls) == 1:
                raise IOError("Simulated storage failure")
            write_bytes(storage_path, data)

        with mock.patch.object(progress.ProgressWriter, "WRITE_PAUSE", 0.1), \
                mock.patch.object(self.storage, "write_bytes", side_effect=flaky_write):

            self.writer.start()
            self._log("first")
            while len(calls) < 1:
                time.sleep(0.01)
            self._log("second")
            while len(calls) < 2:
                time.sleep(0.01)

        self.assertTrue(self.writer.is_alive())
        self.assertEqual(["first", "second"], [e["message"] for e in self.storage.last()["logEntries"]])

    def test_other_error_ends_thread(self):

        with mock.patch.object(progress.ProgressHandler, "snapshot", side_effect=RuntimeError("writer failure")):
            self.writer.start()
            self._log("line")
            self.writer.join(2.0)

        self.assertFalse(self.writer.is_alive())

        # Logging still works once the writer has stopped
        self._log("line after")
        self.assertEqual("line after", self.handler.snapshot().logEntries[-1].message)

    def test_blocked_write_does_not_block_stop(self):

        release = threading.Event()

        def blocked_write(storage_path, data):
            release.wait(5)

        with mock.patch.object(self.storage, "write_bytes", side_effect=blocked_write):

            self.writer.start()
            self._log("line")
            time.sleep(0.1)

            started = time.monotonic()
            self.writer.stop()
            self.assertLess(time.monotonic() - started, 0.1)

            release.set()

    def test_many_threads_logging_while_writing(self):

        with mock.patch.object(progress.ProgressWriter, "WRITE_PAUSE", 0.0):

            self.writer.start()

            def log_lines(n):
                for i in range(500):
                    self.handler.handle(_record("tracdap.shim._0.models.Model", f"{n}-{i}", thread=n))

            threads = [threading.Thread(target=log_lines, args=(n,)) for n in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)

            self.assertFalse(any(thread.is_alive() for thread in threads))

            self.writer.stop()
            self.writer.join(5)

        self.assertFalse(self.writer.is_alive())
        self.assertEqual(4000, self.storage.last()["totalLogEntries"])


if __name__ == "__main__":
    unittest.main()
