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

import contextlib
import dataclasses as dc
import itertools
import json
import logging
import pathlib
import re
import tempfile
import threading
import time
import typing as tp
import unittest
import unittest.mock as mock

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.metadata as meta
import tracdap.rt._impl.core.config_parser as cfg_p  # noqa
import tracdap.rt._impl.core.storage as storage  # noqa
import tracdap.rt._impl.core.type_system as types  # noqa
import tracdap.rt._impl.core.util as util  # noqa
import tracdap.rt._impl.exec.engine as engine  # noqa
import tracdap.rt._impl.exec.graph as graph  # noqa
import tracdap.rt._impl.exec.graph_builder as graph_builder  # noqa
import tracdap.rt._impl.exec.progress as progress  # noqa
import tracdap.rt._impl.runtime as runtime  # noqa

import tracdap_test.rt.progress_switch as progress_switch
import tracdap_test.rt.jobs.test_job_logs as job_logs
import tracdap_test.rt.jobs.test_flow_scenarios as flow_scenarios
import tracdap_test.rt.jobs.test_capture_jobs as capture_jobs
import tracdap_test.rt.jobs.test_placement_jobs as placement_jobs
import tracdap_test.rt.jobs.test_flow_export_jobs as flow_export_jobs


_SCENARIO_LOG_LINE = "Scenario model [{}] ran"
_PROGRESS_LOG_LINE = "Progress model [{}] ran"

_JOB_LOG_LINE = re.compile(r"^\S+ \S+ \[[^]]+] (\w+) (\S+) - (.*)$")
_OBJECT_KEY = re.compile(r"\b(\w+)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-v\d+\b")
_SHIM_PREFIX = re.compile(r"\btracdap\.shim\._\d+\.")


def _int(value: int) -> meta.Value:
    return meta.Value(type=types.TypeMapping.python_to_trac(int), integerValue=value)


def _float(value: float) -> meta.Value:
    return meta.Value(type=types.TypeMapping.python_to_trac(float), floatValue=value)


def normalize_job_log(job_log: str) -> tp.List[str]:

    """Job log lines without their timestamps, thread names and object IDs, which differ from run to run"""

    lines = []

    for line in job_log.splitlines():

        match = _JOB_LOG_LINE.match(line)

        if match:
            level, logger_name, message = match.groups()
            line = f"{level} {_SHIM_PREFIX.sub('tracdap.shim.', logger_name)} - {message}"

        lines.append(_OBJECT_KEY.sub(r"\1-<id>", line))

    return lines


class Recording:

    """Records every log record the progress handler sees, with the step states after it"""

    def __init__(self):
        self.records: tp.List[logging.LogRecord] = []
        self.steps_after: tp.List[tp.Dict[tp.Tuple[meta.JobStepType, str], meta.JobStepStatusCode]] = []

    def events(self) -> tp.List[tp.Tuple[meta.JobStepType, str, str]]:
        return [
            (getattr(r, progress.STEP_TYPE_ATTR), getattr(r, progress.STEP_NAME_ATTR), getattr(r, progress.STEP_EVENT_ATTR))
            for r in self.records if hasattr(r, progress.STEP_EVENT_ATTR)]

    def messages(self) -> tp.List[str]:
        return [record.getMessage() for record in self.records]


@contextlib.contextmanager
def recording_handler() -> tp.Iterator[Recording]:

    recording = Recording()
    process_record = progress.ProgressHandler._process_record  # noqa

    def recorded(handler: progress.ProgressHandler, record: logging.LogRecord):
        process_record(handler, record)
        recording.records.append(record)
        recording.steps_after.append({key: step.statusCode for key, step in handler._steps.items()})  # noqa

    with mock.patch.object(progress.ProgressHandler, "_process_record", recorded):
        yield recording


class JobProgressTestBase(job_logs.JobLogTestBase):

    def _progress_model(
            self, model_class: str, parameters: tp.Dict[str, meta.BasicType],
            model_type: meta.ModelType = meta.ModelType.STANDARD_MODEL) \
            -> tp.Tuple[meta.TagHeader, meta.ObjectDefinition]:

        _, scenario_model = self._scenario_model("SumByRegion")

        model_def = meta.ModelDefinition(
            language="python", repository=self.SCENARIO_REPO_KEY,
            path="tracdap-runtime/python/test", version=scenario_model.model.version,
            entryPoint=f"tracdap_test.rt.jobs.progress_models.{model_class}",
            parameters={name: meta.ModelParameter(meta.TypeDescriptor(basic_type)) for name, basic_type in parameters.items()},
            modelType=model_type)

        return util.new_object_id(meta.ObjectType.MODEL), meta.ObjectDefinition(meta.ObjectType.MODEL, model=model_def)

    def _run_progress_model(self, model_class: str, parameters: tp.Dict[str, meta.Value], **run_args) \
            -> tp.Tuple[cfg.JobResult, tp.Optional[Exception]]:

        model_id, model_obj = self._progress_model(model_class, {n: v.type.basicType for n, v in parameters.items()})

        job_def = meta.JobDefinition(
            jobType=meta.JobType.RUN_MODEL,
            runModel=meta.RunModelJob(model=util.selector_for(model_id), parameters=parameters))

        return self._run_job(job_def, {util.object_key(model_id): model_obj}, **run_args)

    def _sum_by_region_job(self, model_class: str = "SumByRegion") -> tp.Tuple[meta.JobDefinition, dict, dict]:

        accounts_id, objects, mapping = self._capture_accounts()
        job_def, model_objects = self._run_model_job(model_class, inputs={"accounts": accounts_id})

        return job_def, {**objects, **model_objects}, mapping

    def _run_prepared(self, job: tp.Tuple[meta.JobDefinition, dict, dict], **run_args):

        job_result, job_error = self._run_job(*job, **run_args)

        return job_result, job_error, self._read_job_progress(job_result.jobId)

    def _run_sum_by_region(self, model_class: str = "SumByRegion", **run_args):

        return self._run_prepared(self._sum_by_region_job(model_class), **run_args)

    @staticmethod
    def _steps(job_progress: meta.JobProgress) -> tp.Dict[tp.Tuple[meta.JobStepType, str], meta.JobStepStatus]:
        return {(step.stepType, step.stepName): step for step in job_progress.steps}

    @staticmethod
    def _step_codes(job_progress: meta.JobProgress) -> tp.Dict[tp.Tuple[meta.JobStepType, str], meta.JobStepStatusCode]:
        return {(step.stepType, step.stepName): step.statusCode for step in job_progress.steps}

    def _assert_started_and_ended(self, recording: Recording, steps, end_event=progress.STEP_DONE):
        for step_type, step_name in steps:
            self.assertIn((step_type, step_name, progress.STEP_START), recording.events())
            self.assertIn((step_type, step_name, end_event), recording.events())


class JobProgressStepsTest(JobProgressTestBase):

    def test_run_model_steps(self):

        with recording_handler() as recording:
            job_result, job_error, job_progress = self._run_sum_by_region()

        self.assertIsNone(job_error)

        steps = {
            (meta.JobStepType.STEP_LOAD, "accounts"),
            (meta.JobStepType.STEP_MODEL, ""),
            (meta.JobStepType.STEP_SAVE, "region_totals")}

        self.assertEqual(
            {step: meta.JobStepStatusCode.STEP_SUCCEEDED for step in steps},
            {s: c for s, c in self._step_codes(job_progress).items() if s in steps})

        self._assert_started_and_ended(recording, steps)

        for step in job_progress.steps:
            self.assertIsNotNone(step.startTime)
            self.assertIsNotNone(step.endTime)

        model_line = _SCENARIO_LOG_LINE.format("SumByRegion")
        entry = next(e for e in job_progress.logEntries if e.message == model_line)

        self.assertEqual(meta.JobLogSource.LOG_SOURCE_MODEL, entry.source)
        self.assertEqual((meta.JobStepType.STEP_MODEL, ""), (entry.stepType, entry.stepName))

    def test_shim_model_lines(self):

        # The tutorial package is not imported by the test process, so the model loads through the shim
        model_def = meta.ModelDefinition(
            language="python", repository=self.SCENARIO_REPO_KEY, path="examples/models/python/src",
            entryPoint="tutorial.hello_world.HelloWorldModel", version=self._scenario_model("SumByRegion")[1].model.version,
            parameters={"input_number": meta.ModelParameter(meta.TypeDescriptor(meta.BasicType.INTEGER))},
            modelType=meta.ModelType.STANDARD_MODEL)

        model_id = util.new_object_id(meta.ObjectType.MODEL)
        job_def = meta.JobDefinition(
            jobType=meta.JobType.RUN_MODEL,
            runModel=meta.RunModelJob(model=util.selector_for(model_id), parameters={"input_number": _int(42)}))

        with recording_handler() as recording:
            job_result, job_error = self._run_job(
                job_def, {util.object_key(model_id): meta.ObjectDefinition(meta.ObjectType.MODEL, model=model_def)})

        self.assertIsNone(job_error)

        record = next(r for r in recording.records if r.getMessage() == "Hello world model is running")
        self.assertTrue(record.name.startswith("tracdap.shim."))

        entries = self._read_job_progress(job_result.jobId).logEntries
        model_entries = [e for e in entries if e.source == meta.JobLogSource.LOG_SOURCE_MODEL]

        self.assertEqual(["Hello world model is running", "The input number is 42"], [e.message for e in model_entries])
        self.assertTrue(all((e.stepType, e.stepName) == (meta.JobStepType.STEP_MODEL, "") for e in model_entries))

    def test_import_steps(self):

        self.tmp_dir.joinpath("staging", "loans.csv").write_text("id,amount\n1,10.5\n")

        with recording_handler() as recording:
            job_result, job_error = self._run_job(self._capture_job(("loans", "loans.csv")))

        self.assertIsNone(job_error)

        job_progress = self._read_job_progress(job_result.jobId)

        steps = {
            (meta.JobStepType.STEP_CAPTURE, "loans"),
            (meta.JobStepType.STEP_DECODE, "loans"),
            (meta.JobStepType.STEP_SAVE, "loans_file"),
            (meta.JobStepType.STEP_SAVE, "loans")}

        self.assertEqual({step: meta.JobStepStatusCode.STEP_SUCCEEDED for step in steps}, self._step_codes(job_progress))
        self._assert_started_and_ended(recording, steps)

        entry = next(e for e in job_progress.logEntries if e.message.startswith("Captured [loans]"))

        self.assertEqual(meta.JobLogSource.LOG_SOURCE_STEP, entry.source)
        self.assertEqual((meta.JobStepType.STEP_CAPTURE, "loans"), (entry.stepType, entry.stepName))

    def test_export_steps(self):

        self.tmp_dir.joinpath("staging", "loans.csv").write_text("id,amount\n1,10.5\n")

        capture_result, _ = self._run_job(self._capture_job(("loans", "loans.csv")))
        data_id = next(oid for oid in capture_result.objectIds if oid.objectId == capture_result.result.outputs["loans"].objectId)
        objects, mapping = self._result_objects(capture_result)

        placement = meta.PlacementTarget(
            dataId=util.selector_for(data_id),
            location=meta.ExternalLocation("outbound", "loans.csv"))

        job_def = meta.JobDefinition(
            jobType=meta.JobType.EXPORT_DATA,
            exportData=meta.ExportDataJob(placements={"loans": placement}))

        with recording_handler() as recording:
            job_result, job_error = self._run_job(job_def, objects, mapping)

        self.assertIsNone(job_error)

        job_progress = self._read_job_progress(job_result.jobId)

        steps = {
            (meta.JobStepType.STEP_LOAD, "loans"),
            (meta.JobStepType.STEP_PREPARE, "loans"),
            (meta.JobStepType.STEP_PLACE, "loans")}

        self.assertEqual({step: meta.JobStepStatusCode.STEP_SUCCEEDED for step in steps}, self._step_codes(job_progress))
        self._assert_started_and_ended(recording, steps)

        entry = next(e for e in job_progress.logEntries if e.message.startswith("Placed [loans]"))

        self.assertEqual(meta.JobLogSource.LOG_SOURCE_STEP, entry.source)
        self.assertEqual((meta.JobStepType.STEP_PLACE, "loans"), (entry.stepType, entry.stepName))

    def test_failed_and_skipped_steps(self):

        with recording_handler() as recording:
            job_result, job_error, job_progress = self._run_sum_by_region("FailingModel")

        self.assertIsNotNone(job_error)

        step_codes = self._step_codes(job_progress)

        self.assertEqual(meta.JobStepStatusCode.STEP_SUCCEEDED, step_codes[(meta.JobStepType.STEP_LOAD, "accounts")])
        self.assertEqual(meta.JobStepStatusCode.STEP_FAILED, step_codes[(meta.JobStepType.STEP_MODEL, "")])
        self.assertEqual(meta.JobStepStatusCode.STEP_SKIPPED, step_codes[(meta.JobStepType.STEP_SAVE, "failed_accounts")])

        model_failed = [
            r for r in recording.records
            if getattr(r, progress.STEP_EVENT_ATTR, None) == progress.STEP_FAILED
            and getattr(r, progress.STEP_TYPE_ATTR) == meta.JobStepType.STEP_MODEL]

        # The FAILED line, then the exception record with its traceback
        self.assertEqual(2, len(model_failed))
        self.assertIsNotNone(model_failed[1].exc_info)
        self.assertIn((meta.JobStepType.STEP_SAVE, "failed_accounts", progress.STEP_SKIP), recording.events())

        traceback_entry = next(e for e in job_progress.logEntries if "Traceback" in e.message)

        self.assertEqual("ERROR", traceback_entry.level)
        self.assertEqual(meta.JobLogSource.LOG_SOURCE_ENGINE, traceback_entry.source)
        self.assertEqual((meta.JobStepType.STEP_MODEL, ""), (traceback_entry.stepType, traceback_entry.stepName))
        self.assertIn("Scenario model failure", traceback_entry.message)

    def test_failed_capture_step(self):

        job_result, job_error = self._run_job(self._capture_job(("missing", "missing.csv")))

        self.assertIsNotNone(job_error)

        step_codes = self._step_codes(self._read_job_progress(job_result.jobId))

        self.assertEqual(meta.JobStepStatusCode.STEP_FAILED, step_codes[(meta.JobStepType.STEP_CAPTURE, "missing")])
        self.assertEqual(meta.JobStepStatusCode.STEP_SKIPPED, step_codes[(meta.JobStepType.STEP_DECODE, "missing")])
        self.assertEqual(meta.JobStepStatusCode.STEP_SKIPPED, step_codes[(meta.JobStepType.STEP_SAVE, "missing")])

    def test_collision_logs_failed_after_done(self):

        build_dynamic_outputs = graph_builder.GraphBuilder.build_dynamic_outputs

        def colliding_outputs(builder, source_id, output_names):
            update = build_dynamic_outputs(builder, source_id, output_names)
            update.nodes[source_id] = update.nodes[next(iter(update.nodes))]
            return update

        model_id, model_obj = self._progress_model("GenerateAccounts", {}, meta.ModelType.DATA_IMPORT_MODEL)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.IMPORT_DATA,
            importData=meta.ImportDataJob(model=util.selector_for(model_id)))

        with mock.patch.object(graph_builder.GraphBuilder, "build_dynamic_outputs", colliding_outputs), \
                recording_handler() as recording:

            job_result, job_error = self._run_job(job_def, {util.object_key(model_id): model_obj})

        # The collision path then hits the bundle lookup defect, so the job's outcome is not asserted here
        self.assertIsNotNone(job_error)

        model_events = [event for step_type, _, event in recording.events() if step_type == meta.JobStepType.STEP_MODEL]
        self.assertEqual([progress.STEP_START, progress.STEP_DONE, progress.STEP_FAILED], model_events)

        job_log = self._read_job_log(job_result)
        done_line = job_log.index("DONE RunModel [GenerateAccounts]")
        failed_line = job_log.index("FAILED RunModel [GenerateAccounts]")
        self.assertLess(done_line, failed_line)

        step_codes = self._step_codes(self._read_job_progress(job_result.jobId))
        self.assertEqual(meta.JobStepStatusCode.STEP_FAILED, step_codes[(meta.JobStepType.STEP_MODEL, "")])


class JobProgressEntriesTest(JobProgressTestBase):

    def test_engine_entries_left_out(self):

        job = self._sum_by_region_job()

        with recording_handler() as recording:
            job_result, job_error, job_progress = self._run_prepared(job)

        self.assertIsNone(job_error)

        engine_records = [
            r for r in recording.records
            if progress.log_source(r.name, r.getMessage()) == meta.JobLogSource.LOG_SOURCE_ENGINE]

        # Engine plumbing was logged, but none of it reached the snapshot
        self.assertTrue(engine_records)
        self.assertTrue(all(
            e.source != meta.JobLogSource.LOG_SOURCE_ENGINE or e.level == "ERROR"
            for e in job_progress.logEntries))

        kept = [r for r in recording.records if r not in engine_records]
        self.assertEqual(len(kept), job_progress.totalLogEntries)
        self.assertEqual([r.getMessage() for r in kept], [e.message for e in job_progress.logEntries])

    def test_debug_left_out_and_buffer_bounded(self):

        with recording_handler() as recording:
            job_result, job_error = self._run_progress_model(
                "LogManyLines", {"line_count": _int(250), "line_length": _int(3000)})

        self.assertIsNone(job_error)

        job_progress = self._read_job_progress(job_result.jobId)

        self.assertNotIn("debug line", recording.messages())
        self.assertNotIn("debug line", [e.message for e in job_progress.logEntries])

        model_lines = [e for e in job_progress.logEntries if e.message.startswith("line ")]
        kept_records = [
            r for r in recording.records
            if progress.log_source(r.name, r.getMessage()) != meta.JobLogSource.LOG_SOURCE_ENGINE]

        self.assertEqual(progress.ProgressHandler.MAX_LOG_ENTRIES, len(job_progress.logEntries))
        self.assertEqual(len(kept_records), job_progress.totalLogEntries)
        self.assertGreater(job_progress.totalLogEntries, 250)
        self.assertTrue(all(len(e.message) == progress.ProgressHandler.MAX_ENTRY_LENGTH for e in model_lines))

        # The job log keeps every line in full
        self.assertIn("line 0 ".ljust(3000, "x"), self._read_job_log(job_result))

    def test_progress_without_job_log(self):

        job_result, job_error, job_progress = self._run_sum_by_region(logs_enabled=False)

        self.assertIsNone(job_error)
        self.assertIsNone(job_result.result.logFileId)
        self.assertEqual(
            meta.JobStepStatusCode.STEP_SUCCEEDED,
            self._step_codes(job_progress)[(meta.JobStepType.STEP_MODEL, "")])
        self.assertIn(_SCENARIO_LOG_LINE.format("SumByRegion"), [e.message for e in job_progress.logEntries])

    def test_line_written_while_model_waits(self):

        writes: tp.List[tp.Tuple[float, meta.JobProgress]] = []
        write_progress = progress.ProgressWriter._write_progress  # noqa

        def recorded_write(writer, job_progress):
            writes.append((time.monotonic(), job_progress))
            write_progress(writer, job_progress)

        with mock.patch.object(progress.ProgressWriter, "_write_progress", recorded_write):
            started = time.monotonic()
            job_result, job_error = self._run_progress_model("LogThenWait", {"wait_seconds": _float(5.0)})
            ended = time.monotonic()

        self.assertIsNone(job_error)

        model_line = _PROGRESS_LOG_LINE.format("LogThenWait")
        first_written = next(t for t, p in writes if model_line in [e.message for e in p.logEntries])

        # Written while the model was still waiting, and at most once per pause
        self.assertLess(first_written, ended - 3.0)
        self.assertLessEqual(len(writes), (ended - started) / progress.ProgressWriter.WRITE_PAUSE + 2)


class JobProgressSafetyTest(JobProgressTestBase):

    def _run_reference(self) -> tp.Tuple[cfg.JobResult, tp.List[str]]:

        with progress_switch.progress_disabled():
            job_result, job_error, _ = self._run_sum_by_region(progress_enabled=False)

        self.assertIsNone(job_error)

        return job_result, normalize_job_log(self._read_job_log(job_result))

    def _assert_job_unchanged(self, job_result: cfg.JobResult, job_error, reference_log: tp.List[str]):

        self.assertIsNone(job_error)
        self.assertEqual(meta.JobStatusCode.SUCCEEDED, job_result.result.statusCode)
        self.assertEqual(reference_log, normalize_job_log(self._read_job_log(job_result)))

    def test_failed_snapshot_write(self):

        _, reference_log = self._run_reference()
        write_bytes = storage.CommonFileStorage.write_bytes

        def failing_write(file_storage, storage_path, data):
            if "trac_job_progress" in storage_path:
                raise ex.EStorageCommunication(f"Simulated storage failure for [{storage_path}]")
            return write_bytes(file_storage, storage_path, data)

        job = self._sum_by_region_job()

        with mock.patch.object(storage.CommonFileStorage, "write_bytes", failing_write):
            job_result, job_error, job_progress = self._run_prepared(job)

        self._assert_job_unchanged(job_result, job_error, reference_log)
        self.assertIsNone(job_progress)

    def test_handler_raises(self):

        _, reference_log = self._run_reference()
        job = self._sum_by_region_job()

        with mock.patch.object(progress.ProgressHandler, "_process_record", side_effect=RuntimeError("handler failure")):
            job_result, job_error, _ = self._run_prepared(job)

        self._assert_job_unchanged(job_result, job_error, reference_log)

    def test_writer_raises(self):

        _, reference_log = self._run_reference()
        job = self._sum_by_region_job()

        with mock.patch.object(progress.ProgressHandler, "snapshot", side_effect=RuntimeError("writer failure")):
            job_result, job_error, job_progress = self._run_prepared(job)

        self._assert_job_unchanged(job_result, job_error, reference_log)
        self.assertIsNone(job_progress)

    def test_snapshot_storage_lookup_raises(self):

        _, reference_log = self._run_reference()

        def failing_start(_engine, job_key, job_config):
            # Fails the same lookup _start_job_progress makes, the result save still finds its storage
            with mock.patch.object(storage.StorageManager, "get_file_storage", side_effect=ex.EStorageConfig("Simulated")):
                return start_job_progress(_engine, job_key, job_config)

        start_job_progress = engine.TracEngine._start_job_progress  # noqa

        job = self._sum_by_region_job()

        with mock.patch.object(engine.TracEngine, "_start_job_progress", failing_start):
            job_result, job_error, job_progress = self._run_prepared(job)

        self._assert_job_unchanged(job_result, job_error, reference_log)
        self.assertIsNone(job_progress)

    def test_handler_fails_to_build(self):

        _, reference_log = self._run_reference()
        job = self._sum_by_region_job()

        with mock.patch.object(progress.ProgressHandler, "__init__", side_effect=RuntimeError("handler failure")):
            job_result, job_error, job_progress = self._run_prepared(job)

        self._assert_job_unchanged(job_result, job_error, reference_log)
        self.assertIsNone(job_progress)

    def test_failed_result_save_stops_writer(self):

        stop = progress.ProgressWriter.stop
        job = self._sum_by_region_job()
        threads_before = set(threading.enumerate())

        with mock.patch.object(engine.TracEngine, "_save_job_result", side_effect=RuntimeError("save failure")), \
                mock.patch.object(progress.ProgressWriter, "stop", autospec=True, side_effect=stop) as stop_calls, \
                self.assertRaises(Exception):

            self._run_job(*job)

        self.assertEqual(1, stop_calls.call_count)
        self._wait_for_progress_writers()
        self.assertFalse(any(t.name == "progress" for t in set(threading.enumerate()) - threads_before))

    def test_result_disabled_stops_writer(self):

        stop = progress.ProgressWriter.stop
        submit_job = progress_switch._ORIGINAL_SUBMIT  # noqa

        def result_disabled(rt, job_config: cfg.JobConfig):
            job_config.properties[cfg_p.ConfigKeys.RESULT_ENABLED] = "false"
            return submit_job(rt, job_config)

        model_id, model_obj = self._progress_model("LogThenWait", {"wait_seconds": meta.BasicType.FLOAT})
        job_def = meta.JobDefinition(
            jobType=meta.JobType.RUN_MODEL,
            runModel=meta.RunModelJob(model=util.selector_for(model_id), parameters={"wait_seconds": _float(0.0)}))
        threads_before = set(threading.enumerate())

        with mock.patch.object(runtime.TracRuntime, "submit_job", result_disabled), \
                mock.patch.object(progress.ProgressWriter, "stop", autospec=True, side_effect=stop) as stop_calls, \
                self.assertRaises(FileNotFoundError):

            # No result is saved, so there is nothing to read back
            self._run_job(job_def, {util.object_key(model_id): model_obj})

        self._wait_for_progress_writers()

        self.assertEqual(1, stop_calls.call_count)
        self.assertFalse(any(t.name == "progress" for t in set(threading.enumerate()) - threads_before))

    def test_failed_log_save_keeps_outcome(self):

        stop = progress.ProgressWriter.stop

        job = self._sum_by_region_job()

        with self.fail_log_save(), \
                mock.patch.object(progress.ProgressWriter, "stop", autospec=True, side_effect=stop) as stop_calls:

            job_result, job_error, job_progress = self._run_prepared(job)

        self.assertIsNone(job_error)
        self.assertEqual(meta.JobStatusCode.SUCCEEDED, job_result.result.statusCode)
        self.assertIsNone(job_result.result.logFileId)
        self.assertEqual(1, stop_calls.call_count)
        self.assertIsNotNone(job_progress)

    def test_blocked_snapshot_does_not_hold_job(self):

        release = threading.Event()
        write_bytes = storage.CommonFileStorage.write_bytes

        def blocked_write(file_storage, storage_path, data):
            if "trac_job_progress" in storage_path:
                release.wait(30)
            return write_bytes(file_storage, storage_path, data)

        try:
            job = self._sum_by_region_job()

            with mock.patch.object(storage.CommonFileStorage, "write_bytes", blocked_write), \
                    mock.patch.object(job_logs.JobLogTestBase, "_wait_for_progress_writers"):

                started = time.monotonic()
                job_result, job_error, _ = self._run_prepared(job)
                elapsed = time.monotonic() - started

            self.assertIsNone(job_error)
            self.assertEqual(meta.JobStatusCode.SUCCEEDED, job_result.result.statusCode)
            self.assertLess(elapsed, 20)

        finally:
            release.set()
            self._wait_for_progress_writers()

    def test_progress_disabled(self):

        writers = []
        writer_init = progress.ProgressWriter.__init__

        def recorded_init(writer, *args, **kwargs):
            writers.append(writer)
            writer_init(writer, *args, **kwargs)

        job = self._sum_by_region_job()

        with progress_switch.progress_disabled(), \
                mock.patch.object(progress.ProgressWriter, "__init__", recorded_init):

            job_result, job_error, job_progress = self._run_prepared(job, progress_enabled=False)

        self.assertIsNone(job_error)
        self.assertIsNone(job_progress)
        self.assertEqual([], writers)


class JobProgressWriterTest(JobProgressTestBase):

    def test_writes_on_progress_thread(self):

        threads = []
        write_progress = progress.ProgressWriter._write_progress  # noqa

        def recorded_write(writer, job_progress):
            threads.append(threading.current_thread().name)
            write_progress(writer, job_progress)

        job = self._sum_by_region_job()
        threads_before = set(threading.enumerate())

        with mock.patch.object(progress.ProgressWriter, "_write_progress", recorded_write):
            job_result, job_error, _ = self._run_prepared(job)

        self.assertIsNone(job_error)
        self.assertTrue(threads)
        self.assertEqual({"progress"}, set(threads))

        # The job's writer has made its final write and exited
        self.assertFalse(any(t.name == "progress" for t in set(threading.enumerate()) - threads_before))

    def test_child_jobs_share_parent_writer(self):

        writers = []
        writer_init = progress.ProgressWriter.__init__

        def recorded_init(writer, *args, **kwargs):
            writers.append(writer)
            writer_init(writer, *args, **kwargs)

        first_id, first_model = self._progress_model("LogThenWait", {"wait_seconds": meta.BasicType.FLOAT})
        second_id, second_model = self._progress_model("LogManyLines", {
            "line_count": meta.BasicType.INTEGER, "line_length": meta.BasicType.INTEGER})

        def child_job(model_id, parameters):
            return meta.JobDefinition(
                jobType=meta.JobType.RUN_MODEL,
                runModel=meta.RunModelJob(model=util.selector_for(model_id), parameters=parameters))

        job_def = meta.JobDefinition(
            jobType=meta.JobType.JOB_GROUP,
            jobGroup=meta.JobGroup(
                jobGroupType=meta.JobGroupType.SEQUENTIAL_JOB_GROUP,
                sequential=meta.SequentialJobGroup(jobs=[
                    child_job(first_id, {"wait_seconds": _float(0.0)}),
                    child_job(second_id, {"line_count": _int(3), "line_length": _int(10)})])))

        objects = {util.object_key(first_id): first_model, util.object_key(second_id): second_model}

        with mock.patch.object(progress.ProgressWriter, "__init__", recorded_init), \
                recording_handler() as recording:

            job_result, job_error = self._run_job(job_def, objects)

        self.assertIsNone(job_error)

        # One writer for the whole group, still writing after the first child has finished
        self.assertEqual(1, len(writers))

        # Job group results carry no job ID yet, the group's snapshot is the only one written
        progress_files = list(self.tmp_dir.joinpath("data", "results").glob("*/trac_job_progress.json"))
        self.assertEqual(1, len(progress_files))

        job_progress = cfg_p.ConfigParser(meta.JobProgress).parse(json.loads(progress_files[0].read_text()))
        messages = [e.message for e in job_progress.logEntries]

        self.assertIn(_PROGRESS_LOG_LINE.format("LogThenWait"), messages)
        self.assertIn("line 2 xxx", messages)
        self.assertLess(
            recording.messages().index(_PROGRESS_LOG_LINE.format("LogThenWait")),
            recording.messages().index("line 0 xxx"))

    def test_running_shutdown_with_live_writer(self):

        sys_config = self._sys_config()
        model_id, model_obj = self._progress_model("LogThenWait", {"wait_seconds": meta.BasicType.FLOAT})

        job_id = util.new_object_id(meta.ObjectType.JOB)
        job_config = cfg.JobConfig(
            job_id, meta.JobDefinition(
                jobType=meta.JobType.RUN_MODEL,
                runModel=meta.RunModelJob(model=util.selector_for(model_id), parameters={"wait_seconds": _float(1.0)})),
            resultId=util.new_object_id(meta.ObjectType.RESULT),
            objects={util.object_key(model_id): model_obj})

        job_config.properties[cfg_p.ConfigKeys.RESULT_STORAGE_LOCATION] = self.RESULT_STORAGE_KEY
        job_config.properties[cfg_p.ConfigKeys.RESULT_PROGRESS_PATH] = self.RESULT_PROGRESS_PATH.format(util.object_key(job_id))

        # Stop the runtime while the job is still running, the job's own finalize never runs
        trac_runtime = runtime.TracRuntime(sys_config, scratch_dir=self.tmp_dir.joinpath("scratch"))
        trac_runtime.pre_start()
        trac_runtime.start(wait=True)

        try:
            trac_runtime.submit_job(job_config)
            time.sleep(0.3)
        finally:
            trac_runtime.stop()

        writer_threads = [thread for thread in threading.enumerate() if thread.name == "progress"]

        try:
            self.assertTrue(writer_threads)
            self.assertTrue(all(thread.daemon for thread in writer_threads))
        finally:
            # The writer outlives the stopped runtime, stop it so later tests don't see it
            for thread in writer_threads:
                thread.stop()
                thread.join(5)


class FlowScenarioProgressTest(flow_scenarios.FlowScenarioDevModeTest):

    """Progress of run-flow jobs, using the run-flow scenario catalogue"""

    # Run here only the progress tests, the catalogue itself runs in FlowScenarioDevModeTest
    test_scenarios = None
    test_type_only_not_inferred = None

    def _run_progress_scenario(self, scenario_name: str) -> tp.Tuple[cfg.JobResult, meta.JobProgress, str]:

        scenario_dir = flow_scenarios.SCENARIOS_DIR.joinpath(scenario_name)
        job_spec = flow_scenarios._read_yaml(scenario_dir.joinpath("job.yaml"))  # noqa

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = pathlib.Path(tmp)
            _, job_result, job_error = self._run_job(tmp_dir, scenario_dir, job_spec, progress_enabled=True)
            return job_result, self._read_progress(tmp_dir), self._read_saved_log(tmp_dir, job_result)

    def _saved_log(self, scenario_name: str, progress_enabled: bool) -> tp.List[str]:

        scenario_dir = flow_scenarios.SCENARIOS_DIR.joinpath(scenario_name)
        job_spec = flow_scenarios._read_yaml(scenario_dir.joinpath("job.yaml"))  # noqa

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = pathlib.Path(tmp)
            _, job_result, _ = self._run_job(tmp_dir, scenario_dir, job_spec, progress_enabled)
            return normalize_job_log(self._read_saved_log(tmp_dir, job_result))

    @staticmethod
    def _model_steps(job_progress: meta.JobProgress) -> tp.Dict[str, meta.JobStepStatusCode]:
        return {s.stepName: s.statusCode for s in job_progress.steps if s.stepType == meta.JobStepType.STEP_MODEL}

    def test_model_steps_named_by_flow_node(self):

        # fan_out runs SumByRegion in two nodes, sum_all and sum_filtered
        job_result, job_progress, _ = self._run_progress_scenario("fan_out")

        self.assertEqual(meta.JobStatusCode.SUCCEEDED, job_result.result.statusCode)
        self.assertEqual(
            {name: meta.JobStepStatusCode.STEP_SUCCEEDED for name in ["filter", "sum_all", "sum_filtered", "count_filtered"]},
            self._model_steps(job_progress))

        load_steps = {s.stepName for s in job_progress.steps if s.stepType == meta.JobStepType.STEP_LOAD}
        save_steps = {s.stepName for s in job_progress.steps if s.stepType == meta.JobStepType.STEP_SAVE}

        self.assertEqual({"accounts"}, load_steps)
        self.assertEqual({"totals_all", "totals_filtered", "account_count"}, save_steps)

        lines = {e.message: e for e in job_progress.logEntries}
        model_line = lines[_SCENARIO_LOG_LINE.format("FilterAccounts")]
        self.assertEqual((meta.JobStepType.STEP_MODEL, "filter"), (model_line.stepType, model_line.stepName))

    def test_queued_step_has_no_entry(self):

        with recording_handler() as recording:
            self._run_progress_scenario("fan_out")

        model_starts = [
            (record, steps) for record, steps in zip(recording.records, recording.steps_after)
            if getattr(record, progress.STEP_EVENT_ATTR, None) == progress.STEP_START
            and getattr(record, progress.STEP_TYPE_ATTR) == meta.JobStepType.STEP_MODEL]

        self.assertEqual(4, len(model_starts))

        # Models run one at a time on the model pool, so when each starts the later ones have no entry
        for index, (record, steps) in enumerate(model_starts):
            model_steps = {key: code for key, code in steps.items() if key[0] == meta.JobStepType.STEP_MODEL}
            self.assertEqual(index + 1, len(model_steps))
            self.assertEqual(index, sum(code == meta.JobStepStatusCode.STEP_SUCCEEDED for code in model_steps.values()))

    def _check_branch_failure(self, job_result: cfg.JobResult, job_progress: meta.JobProgress):

        self.assertEqual(meta.JobStatusCode.FAILED, job_result.result.statusCode)

        model_steps = self._model_steps(job_progress)

        self.assertEqual(meta.JobStepStatusCode.STEP_SUCCEEDED, model_steps["good"])
        self.assertEqual(meta.JobStepStatusCode.STEP_FAILED, model_steps["bad"])
        self.assertEqual(meta.JobStepStatusCode.STEP_SKIPPED, model_steps["count"])

    def test_independent_branches(self):

        job_result, job_progress, _ = self._run_progress_scenario("branch_failure")
        self._check_branch_failure(job_result, job_progress)

    def test_independent_branch_after_failure(self):

        is_viable_node = engine.GraphProcessor._is_viable_node.__func__  # noqa

        # Hold the good branch back until the bad branch has failed
        def bad_branch_first(cls, node: engine._EngineNode, graph: engine._EngineContext):  # noqa
            if isinstance(node.node, graph_builder.RunModelNode) and node.node.step_name == "good":
                bad_failed = any(n.name == "bad" or n.namespace.name == "MODEL = bad" for n in graph.failed_nodes)
                if not bad_failed:
                    return False
            return is_viable_node(cls, node, graph)

        with recording_handler() as recording, \
                mock.patch.object(engine.GraphProcessor, "_is_viable_node", classmethod(bad_branch_first)):

            job_result, job_progress, _ = self._run_progress_scenario("branch_failure")

        self._check_branch_failure(job_result, job_progress)

        model_events = [
            (name, event) for step_type, name, event in recording.events() if step_type == meta.JobStepType.STEP_MODEL]

        self.assertLess(model_events.index(("bad", progress.STEP_FAILED)), model_events.index(("good", progress.STEP_START)))

    def test_model_line_kept_through_plumbing(self):

        # Count what the snapshot keeps for the chain, then keep exactly that many entries
        _, job_progress, _ = self._run_progress_scenario("chain")
        kept_entries = job_progress.totalLogEntries

        with mock.patch.object(progress.ProgressHandler, "MAX_LOG_ENTRIES", kept_entries), \
                recording_handler() as recording:

            _, job_progress, _ = self._run_progress_scenario("chain")

        engine_records = [
            r for r in recording.records
            if progress.log_source(r.name, r.getMessage()) == meta.JobLogSource.LOG_SOURCE_ENGINE]

        first_line = _SCENARIO_LOG_LINE.format("FilterAccounts")
        messages = [e.message for e in job_progress.logEntries]

        # The filter model logs before the plumbing for later nodes, which would push it out if it were kept
        self.assertGreater(len(engine_records), 0)
        self.assertEqual(kept_entries, len(job_progress.logEntries))
        self.assertIn(first_line, messages)
        self.assertGreater(
            len([r for r in recording.records if r.getMessage() != first_line][recording.messages().index(first_line):]),
            len(messages) - messages.index(first_line))

    def test_saved_log_unchanged(self):

        for scenario_name in ["chain", "branch_failure"]:
            with self.subTest(scenario_name):

                without_progress = self._saved_log(scenario_name, progress_enabled=False)
                with_progress = self._saved_log(scenario_name, progress_enabled=True)

                # Independent nodes start in no fixed order, so the lines are compared without their order
                self.assertEqual(sorted(without_progress), sorted(with_progress))

    def test_node_logger_lines(self):

        lines = self._saved_log("branch_failure", progress_enabled=True)

        def line(level: str, logger_name: str, message: str):
            self.assertIn(f"{level} {logger_name} - {message}", lines)

        def line_starting(level: str, logger_name: str, message: str):
            prefix = f"{level} {logger_name} - {message}"
            self.assertTrue(any(x.startswith(prefix) for x in lines), f"No line starting [{prefix}]")

        node_logger = "tracdap.rt._impl.exec.engine.NodeProcessor"
        graph_logger = "tracdap.rt._impl.exec.engine.GraphProcessor"

        line("INFO", node_logger, "START LoadData [accounts:LOAD] / JOB-<id>")
        line("INFO", node_logger, "DONE LoadData [accounts:LOAD] / JOB-<id>")
        line("INFO", node_logger, "START RunModel [SumByRegion] / MODEL = good, JOB-<id>")
        line("INFO", node_logger, "DONE RunModel [SumByRegion] / MODEL = good, JOB-<id>")
        line("ERROR", node_logger, "FAILED RunModel [FailingModel] / MODEL = bad, JOB-<id>")
        line_starting("ERROR", node_logger, "There was an unhandled error in the model: Scenario model failure")
        line("WARNING", graph_logger, "SKIP CountAccounts / MODEL = count, JOB-<id> (upstream failure)")
        line("INFO", "tracdap_test.flow_scenarios.models.SumByRegion", "Scenario model [SumByRegion] ran")


@contextlib.contextmanager
def fixed_object_ids() -> tp.Iterator[None]:

    """Allocate the same object IDs and storage paths each time, so two builds of a job give the same graph"""

    counter = itertools.count()
    timestamp = meta.DatetimeValue("2026-11-22T00:00:00+00:00")

    def fixed_id(object_type: meta.ObjectType) -> meta.TagHeader:
        return meta.TagHeader(object_type, f"00000000-0000-7000-8000-{next(counter):012d}", 1, timestamp, 1, timestamp)

    def fixed_version(object_id: meta.TagHeader) -> meta.TagHeader:
        return dc.replace(object_id, objectVersion=object_id.objectVersion + 1, objectTimestamp=timestamp, tagVersion=1, tagTimestamp=timestamp)

    with mock.patch.object(util, "new_object_id", fixed_id), \
            mock.patch.object(util, "new_object_version", fixed_version), \
            mock.patch.object(storage.random.Random, "randint", lambda _, a, b: a):
        yield


@contextlib.contextmanager
def unnamed_steps() -> tp.Iterator[None]:

    """Build step nodes without their step names"""

    def unnamed(node_class):

        class UnnamedNode(node_class):
            def __init__(self, *args, step_name="", **kwargs):  # noqa
                super().__init__(*args, **kwargs)

        return UnnamedNode

    with mock.patch.object(graph_builder, "RunModelNode", unnamed(graph_builder.RunModelNode)), \
            mock.patch.object(graph_builder, "LoadDataNode", unnamed(graph_builder.LoadDataNode)), \
            mock.patch.object(graph_builder, "SaveDataNode", unnamed(graph_builder.SaveDataNode)):
        yield


class GraphStepsTest(unittest.TestCase):

    """The step names leave every graph unchanged, and every step node gets its name"""

    @classmethod
    def setUpClass(cls) -> None:
        job_logs.log.configure_logging()

    @staticmethod
    def _node_fields(node: graph.Node) -> dict:
        return {f.name: getattr(node, f.name) for f in dc.fields(node) if f.name != "step_name"}

    def _built_with_and_without_names(self, build: tp.Callable[[], tp.Union[graph.Graph, graph.GraphUpdate]]):

        with fixed_object_ids(), unnamed_steps():
            unnamed = build()

        with fixed_object_ids():
            named = build()

        # Same node IDs, same nodes apart from the step name
        self.assertEqual(set(unnamed.nodes.keys()), set(named.nodes.keys()))

        for node_id, node in named.nodes.items():
            unnamed_node = unnamed.nodes[node_id]
            unnamed_type = type(unnamed_node)
            self.assertIs(type(node), unnamed_type.__base__ if unnamed_type.__name__ == "UnnamedNode" else unnamed_type)
            self.assertEqual(self._node_fields(node), self._node_fields(unnamed_node), f"Node [{node_id}]")

        return named

    @staticmethod
    def _steps(job_graph: tp.Union[graph.Graph, graph.GraphUpdate]) -> tp.List[tp.Tuple[meta.JobStepType, str]]:
        return sorted(filter(None, map(progress.step_of, job_graph.nodes.values())), key=lambda s: (s[0].value, s[1]))

    @staticmethod
    def _sorted(*steps) -> tp.List[tp.Tuple[meta.JobStepType, str]]:
        return sorted(steps, key=lambda s: (s[0].value, s[1]))

    def test_run_model_steps(self):

        data_selector, objects, mapping = placement_jobs.PlacementGraphTest._dataset()  # noqa
        schema = meta.ModelInputSchema(objectType=meta.ObjectType.DATA, schema=placement_jobs._LOANS_SCHEMA)  # noqa
        outputs = {"loan_totals": meta.ModelOutputSchema(objectType=meta.ObjectType.DATA, schema=placement_jobs._LOANS_SCHEMA)}  # noqa

        def build():
            model_id = util.new_object_id(meta.ObjectType.MODEL)
            model_def = meta.ModelDefinition(
                language="python", repository="trac_integrated", entryPoint="tracdap_test.unit_test_model.UnitTestModel",
                inputs={"loans": schema}, outputs=outputs)
            job_def = meta.JobDefinition(
                jobType=meta.JobType.RUN_MODEL,
                runModel=meta.RunModelJob(model=util.selector_for(model_id), inputs={"loans": data_selector}))
            job_config = cfg.JobConfig(
                jobId=util.new_object_id(meta.ObjectType.JOB), job=job_def,
                objects={**objects, util.object_key(model_id): meta.ObjectDefinition(meta.ObjectType.MODEL, model=model_def)},
                objectMapping=mapping)
            sys_config = cfg.RuntimeConfig(properties={cfg_p.ConfigKeys.STORAGE_DEFAULT_LOCATION: "test_data"})
            return graph_builder.GraphBuilder(sys_config, job_config).build_job(job_def)

        job_graph = self._built_with_and_without_names(build)

        self.assertEqual(
            self._sorted(
                (meta.JobStepType.STEP_LOAD, "loans"),
                (meta.JobStepType.STEP_MODEL, ""),
                (meta.JobStepType.STEP_SAVE, "loan_totals")),
            self._steps(job_graph))

        # Dynamic outputs added while the model runs are named by output
        model_node = next(n for n in job_graph.nodes.values() if isinstance(n, graph.RunModelNode))

        def build_update():
            builder = graph_builder.GraphBuilder.dynamic(model_node.graph_context)
            return builder.build_dynamic_outputs(model_node.id, ["extra_a", "extra_b"])

        update = self._built_with_and_without_names(build_update)

        self.assertEqual(
            self._sorted((meta.JobStepType.STEP_SAVE, "extra_a"), (meta.JobStepType.STEP_SAVE, "extra_b")),
            self._steps(update))

    def test_flow_steps(self):

        # The harness supplies no data for the flow input, so the flow has a model step only
        job_graph = self._built_with_and_without_names(
            lambda: flow_export_jobs.FlowExportGraphTest._build_graph(meta.ModelType.DATA_EXPORT_MODEL, True))  # noqa

        self.assertEqual([(meta.JobStepType.STEP_MODEL, "export")], self._steps(job_graph))

    def test_capture_steps(self):

        schema = capture_jobs._LOANS_SCHEMA  # noqa

        job_graph = self._built_with_and_without_names(
            lambda: capture_jobs.CaptureGraphTest._build({"loans": capture_jobs._declared("loans.csv", schema)})[0])  # noqa

        self.assertEqual(
            self._sorted(
                (meta.JobStepType.STEP_CAPTURE, "loans"),
                (meta.JobStepType.STEP_DECODE, "loans"),
                (meta.JobStepType.STEP_SAVE, "loans"),
                (meta.JobStepType.STEP_SAVE, "loans_file")),
            self._steps(job_graph))

    def test_placement_steps(self):

        data_selector, objects, mapping = placement_jobs.PlacementGraphTest._dataset()  # noqa

        def build():
            export_data = meta.ExportDataJob(placements={"loans_csv": placement_jobs._placement(data_selector, "out/loans.csv")})  # noqa
            return placement_jobs.PlacementGraphTest._build(export_data, objects, mapping)  # noqa

        job_graph = self._built_with_and_without_names(build)

        # The placement's dataset is loaded under the placement name
        self.assertEqual(
            self._sorted(
                (meta.JobStepType.STEP_LOAD, "loans_csv"),
                (meta.JobStepType.STEP_PREPARE, "loans_csv"),
                (meta.JobStepType.STEP_PLACE, "loans_csv")),
            self._steps(job_graph))


if __name__ == "__main__":
    unittest.main()
