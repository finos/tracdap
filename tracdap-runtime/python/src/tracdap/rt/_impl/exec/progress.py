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

import collections
import dataclasses as dc
import datetime as dt
import logging
import re
import threading
import typing as tp

import tracdap.rt.metadata as _meta
import tracdap.rt._impl.core.config_parser as _cfg_p
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.exec.graph as _graph


# Step fields carried as "extra" on step log records, prefixed to avoid clashing with LogRecord attributes
STEP_TYPE_ATTR = "trac_step_type"
STEP_NAME_ATTR = "trac_step_name"
STEP_EVENT_ATTR = "trac_step_event"

STEP_START = "START"
STEP_DONE = "DONE"
STEP_FAILED = "FAILED"
STEP_SKIP = "SKIP"

_STEP_NODE_TYPES: tp.Dict[type, tp.Tuple[_meta.JobStepType, tp.Callable[[tp.Any], str]]] = {
    _graph.RunModelNode: (_meta.JobStepType.STEP_MODEL, lambda n: n.step_name),
    _graph.LoadDataNode: (_meta.JobStepType.STEP_LOAD, lambda n: n.step_name),
    _graph.SaveDataNode: (_meta.JobStepType.STEP_SAVE, lambda n: n.step_name),
    _graph.CaptureFileNode: (_meta.JobStepType.STEP_CAPTURE, lambda n: n.capture_name),
    _graph.DecodeTableNode: (_meta.JobStepType.STEP_DECODE, lambda n: n.capture_name),
    _graph.PreparePlacementNode: (_meta.JobStepType.STEP_PREPARE, lambda n: n.placement_name),
    _graph.PlaceFileNode: (_meta.JobStepType.STEP_PLACE, lambda n: n.placement_name),
}


def step_of(node: _graph.Node) -> tp.Optional[tp.Tuple[_meta.JobStepType, str]]:

    step_type = _STEP_NODE_TYPES.get(type(node))

    if step_type is None:
        return None

    return step_type[0], step_type[1](node)


def step_extra(node: _graph.Node, step_event: str) -> tp.Optional[tp.Dict[str, tp.Any]]:

    step = step_of(node)

    if step is None:
        return None

    return {STEP_TYPE_ATTR: step[0], STEP_NAME_ATTR: step[1], STEP_EVENT_ATTR: step_event}


# The log source rule, applied in the same order by the saved-log view in trac-ui

NODE_LOGGER_NAME = "tracdap.rt._impl.exec.engine.NodeProcessor"

_STEP_LINE = re.compile(
    r"^(START|DONE|FAILED) (RunModel|LoadData|SaveData|CaptureFile|DecodeTable|PreparePlacement|PlaceFile) \[")

_STEP_DETAIL_LOGGERS = ("tracdap.rt._impl.core.capture.", "tracdap.rt._impl.core.placement.")
_SHIM_LOGGERS = "tracdap.shim."
_ENGINE_LOGGERS = "tracdap."


def log_source(logger_name: str, message: str) -> _meta.JobLogSource:

    if logger_name == NODE_LOGGER_NAME and _STEP_LINE.match(message):
        return _meta.JobLogSource.LOG_SOURCE_STEP

    if logger_name.startswith(_STEP_DETAIL_LOGGERS):
        return _meta.JobLogSource.LOG_SOURCE_STEP

    if logger_name.startswith(_SHIM_LOGGERS):
        return _meta.JobLogSource.LOG_SOURCE_MODEL

    if logger_name.startswith(_ENGINE_LOGGERS):
        return _meta.JobLogSource.LOG_SOURCE_ENGINE

    return _meta.JobLogSource.LOG_SOURCE_MODEL


def _datetime_value(timestamp: float) -> _meta.DatetimeValue:

    return _meta.DatetimeValue(dt.datetime.fromtimestamp(timestamp, dt.timezone.utc).isoformat())


class ProgressHandler(logging.Handler):

    """
    Build the progress of a job from its log records

    Every record is processed in constant time, under the lock that logging.Handler.handle() holds
    Records are never written out here, the writer thread does that
    """

    MAX_LOG_ENTRIES = 200
    MAX_ENTRY_LENGTH = 2000

    def __init__(self):

        super().__init__(logging.INFO)

        self._steps: tp.Dict[tp.Tuple[_meta.JobStepType, str], _meta.JobStepStatus] = dict()
        self._thread_steps: tp.Dict[int, tp.Tuple[_meta.JobStepType, str]] = dict()
        self._log_entries: tp.Deque[_meta.JobLogEntry] = collections.deque(maxlen=self.MAX_LOG_ENTRIES)
        self._total_log_entries = 0

        self._changed = False
        self._wake_up = threading.Event()

    @property
    def wake_up(self) -> threading.Event:
        return self._wake_up

    def emit(self, record: logging.LogRecord):

        # Logging must never fail because of progress
        try:
            self._process_record(record)
        except Exception:  # noqa
            pass

    def snapshot(self) -> tp.Optional[_meta.JobProgress]:

        """Copy the progress if it has changed since the last snapshot, otherwise return None"""

        self.acquire()

        try:

            if not self._changed:
                return None

            steps = [dc.replace(step) for step in self._steps.values()]
            log_entries = list(self._log_entries)
            total_log_entries = self._total_log_entries

            self._changed = False

        finally:
            self.release()

        return _meta.JobProgress(
            steps=steps, logEntries=log_entries,
            totalLogEntries=total_log_entries,
            updateTime=_datetime_value(dt.datetime.now(dt.timezone.utc).timestamp()))

    def _process_record(self, record: logging.LogRecord):

        changed = False

        step_type = getattr(record, STEP_TYPE_ATTR, None)
        step_name = getattr(record, STEP_NAME_ATTR, None)
        step_event = getattr(record, STEP_EVENT_ATTR, None)

        if step_type is not None and step_name is not None:
            step = (step_type, step_name)
            changed = self._process_step_event(step, step_event, record)
        else:
            step = self._thread_steps.get(record.thread)

        message = record.getMessage()
        source = log_source(record.name, message)

        if source != _meta.JobLogSource.LOG_SOURCE_ENGINE or record.levelno >= logging.ERROR:
            self._log_entries.append(self._log_entry(record, message, source, step))
            self._total_log_entries += 1
            changed = True

        if changed:
            self._changed = True
            self._wake_up.set()

    def _process_step_event(self, step, step_event: str, record: logging.LogRecord) -> bool:

        step_status = self._steps.get(step)
        timestamp = _datetime_value(record.created)

        if step_event == STEP_START:
            self._thread_steps[record.thread] = step

        elif step_event in (STEP_DONE, STEP_FAILED):
            if self._thread_steps.get(record.thread) == step:
                del self._thread_steps[record.thread]

        # A failed step stays failed, whatever order its records arrive in
        if step_status is not None and step_status.statusCode == _meta.JobStepStatusCode.STEP_FAILED:
            return False

        if step_event == STEP_START:
            status_code = _meta.JobStepStatusCode.STEP_RUNNING
        elif step_event == STEP_DONE:
            status_code = _meta.JobStepStatusCode.STEP_SUCCEEDED
        elif step_event == STEP_FAILED:
            status_code = _meta.JobStepStatusCode.STEP_FAILED
        elif step_event == STEP_SKIP:
            status_code = _meta.JobStepStatusCode.STEP_SKIPPED
        else:
            return False

        if step_status is None:
            step_status = _meta.JobStepStatus(stepType=step[0], stepName=step[1])
            self._steps[step] = step_status

        step_status.statusCode = status_code

        if status_code == _meta.JobStepStatusCode.STEP_RUNNING:
            step_status.startTime = timestamp
        else:
            step_status.endTime = timestamp

        return True

    def _log_entry(self, record: logging.LogRecord, message: str, source: _meta.JobLogSource, step):

        if record.exc_info:
            if not record.exc_text:
                record.exc_text = logging.Formatter().formatException(record.exc_info)
            message = message + "\n" + record.exc_text

        if len(message) > self.MAX_ENTRY_LENGTH:
            message = message[:self.MAX_ENTRY_LENGTH]

        return _meta.JobLogEntry(
            timestamp=_datetime_value(record.created),
            level=logging.getLevelName(record.levelno),
            source=source,
            stepType=step[0] if step else None,
            stepName=step[1] if step else None,
            message=message)


class ProgressWriter(threading.Thread):

    """
    Write the progress snapshot of a job to storage, whenever it changes

    Writes happen at most once per pause, only on this thread, never while the handler's lock is held
    """

    WRITE_PAUSE = 2.0

    def __init__(self, handler: ProgressHandler, storage, storage_path: str):

        super().__init__(name="progress", daemon=True)

        self._log = _logging.logger_for_object(self)
        self._handler = handler
        self._storage = storage
        self._storage_path = storage_path
        self._stop_requested = threading.Event()

    @property
    def handler(self) -> ProgressHandler:
        return self._handler

    def stop(self):

        self._stop_requested.set()
        self._handler.wake_up.set()

    def run(self):

        try:

            while True:

                self._handler.wake_up.wait()
                self._handler.wake_up.clear()

                stopping = self._stop_requested.is_set()

                progress = self._handler.snapshot()

                if progress is not None:
                    self._write_progress(progress)

                if stopping:
                    return

                self._stop_requested.wait(self.WRITE_PAUSE)

        except Exception as e:
            self._log.exception(f"Job progress stopped for [{self._storage_path}]: {str(e)}")

    def _write_progress(self, progress: _meta.JobProgress):

        try:
            content = _cfg_p.ConfigQuoter.quote_json(progress)
            self._storage.write_bytes(self._storage_path, content.encode("utf-8"))
        except Exception as e:
            self._log.warning(f"Job progress could not be written for [{self._storage_path}]: {str(e)}")
