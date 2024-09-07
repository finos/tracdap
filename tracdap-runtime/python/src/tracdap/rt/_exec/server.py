#  Copyright 2024 Accenture Global Solutions Limited
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

import asyncio
import threading
import typing as tp

import tracdap.rt.config as config
import tracdap.rt.exceptions as ex
import tracdap.rt._exec.actors as actors
import tracdap.rt._impl.grpc.codec as codec  # noqa
import tracdap.rt._impl.util as util  # noqa

# Check whether gRPC is installed before trying to load any of the generated modules
try:
    import grpc.aio  # noqa
    import google.protobuf.message as _msg  # noqa
except ImportError:
    raise ex.EStartup("The runtime API server cannot be enabled because gRPC libraries are not installed")

# Imports for gRPC generated code, these are managed by build_runtime.py for distribution
import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2 as runtime_pb2
import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2_grpc as runtime_grpc


class RuntimeApiServer(runtime_grpc.TracRuntimeApiServicer):

    # Default timeout values in seconds
    __DEFAULT_STARTUP_TIMEOUT = 5.0
    __DEFAULT_SHUTDOWN_TIMEOUT = 10.0
    __DEFAULT_REQUEST_TIMEOUT = 10.0

    def __init__(self, system: actors.ActorSystem, port: int):

        self.__log = util.logger_for_object(self)

        self.__system = system
        self.__engine_id = system.main_id()
        self.__agent: tp.Optional[ApiAgent] = None

        self.__port = port
        self.__request_timeout = self.__DEFAULT_REQUEST_TIMEOUT  # Not configurable atm
        self.__server: tp.Optional[grpc.aio.Server] = None
        self.__server_thread: tp.Optional[threading.Thread] = None
        self.__event_loop: tp.Optional[asyncio.AbstractEventLoop] = None

        self.__start_signal: tp.Optional[threading.Event] = None
        self.__stop_signal: tp.Optional[asyncio.Event] = None

    def start(self, startup_timeout: float = None):

        if self.__start_signal is not None:
            return

        timeout = startup_timeout or self.__DEFAULT_SHUTDOWN_TIMEOUT

        self.__start_signal = threading.Event()
        self.__server_thread = threading.Thread(target=self.__server_main, name="api_server", daemon=True)
        self.__server_thread.start()

        try:
            self.__start_signal.wait(timeout)
        except TimeoutError as e:
            raise ex.EStartup("Runtime API failed to start") from e

    def stop(self, shutdown_timeout: float = None):

        if self.__server is None:
            return

        timeout = shutdown_timeout or self.__DEFAULT_SHUTDOWN_TIMEOUT

        self.__event_loop.call_soon_threadsafe(lambda: self.__stop_signal.set())
        self.__server_thread.join(timeout)

        if self.__server_thread.is_alive():
            self.__log.warning("Runtime API server did not go down cleanly")

    def __server_main(self):

        self.__event_loop = asyncio.new_event_loop()
        self.__event_loop.run_until_complete(self.__server_main_async())
        self.__event_loop.close()

    async def __server_main_async(self):

        server_address = f"[::]:{self.__port}"

        # Asyncio events must be created inside the event loop for Python <= 3.9
        self.__stop_signal = asyncio.Event()

        # Agent using asyncio, so must be created inside the event loop
        self.__agent = ApiAgent()
        self.__system.spawn_agent(self.__agent)

        self.__server = grpc.aio.server()
        self.__server.add_insecure_port(server_address)
        runtime_grpc.add_TracRuntimeApiServicer_to_server(self, self.__server)

        await self.__server.start()
        await self.__agent.started()

        self.__start_signal.set()

        self.__log.info(f"Runtime API server is up and listening on port [{self.__port}]")

        await asyncio.create_task(self.__stop_signal.wait())

        self.__log.info(f"Shutdown signal received, runtime API server is going down...")

        await self.__server.stop(self.__DEFAULT_SHUTDOWN_TIMEOUT)
        self.__server = None

        self.__log.info("Runtime API server has gone down cleanly")

    async def listJobs(self, request: runtime_pb2.ListJobsRequest, context: grpc.aio.ServicerContext):

        request_task = ListJobsRequest(self.__engine_id, request, context)
        self.__agent.threadsafe().spawn(request_task)

        return await request_task.complete(self.__request_timeout)

    async def getJobStatus(self, request: runtime_pb2.JobInfoRequest, context: grpc.ServicerContext):

        request_task = GetJobStatusRequest(self.__engine_id, request, context)
        self.__agent.threadsafe().spawn(request_task)

        return await request_task.complete(self.__request_timeout)

    async def getJobDetails(self, request: runtime_pb2.JobInfoRequest, context: grpc.ServicerContext):

        request_task = GetJobStatusRequest(self.__engine_id, request, context)
        self.__agent.threadsafe().spawn(request_task)

        return await request_task.complete(self.__request_timeout)


_T_REQUEST = tp.TypeVar("_T_REQUEST", bound=_msg.Message)
_T_RESPONSE = tp.TypeVar("_T_RESPONSE", bound=_msg.Message)


class ApiAgent(actors.ThreadsafeActor):

    # API Agent is the parent actor that will be used to spawn API requests
    # It must be created inside the asyncio event loop

    def __init__(self):
        super().__init__()
        self._event_loop = asyncio.get_event_loop()
        self.__start_signal = asyncio.Event()

    def on_start(self):
        self._event_loop.call_soon_threadsafe(lambda: self.__start_signal.set())

    async def started(self):
        await self.__start_signal.wait()


class ApiRequest(actors.ThreadsafeActor, tp.Generic[_T_REQUEST, _T_RESPONSE]):

    # API request is the bridge between asyncio events (gRPC) and actor messages (TRAC runtime engine)
    # Requests objects must be created inside the asyncio event loop

    _log = None

    def __init__(
            self, engine_id, method: str, request: _T_REQUEST,
            context: grpc.aio.ServicerContext):

        super().__init__()

        self._engine_id = engine_id
        self._method = method
        self._request = request
        self._response: tp.Optional[_T_RESPONSE] = None
        self._error: tp.Optional[Exception] = None
        self._grpc_code = grpc.StatusCode.OK
        self._grpc_message = ""

        self._context = context
        self._event_loop = asyncio.get_event_loop()
        self._completion = asyncio.Event()

        self._log.info("API call start: %s()", self._method)

    def _mark_complete(self):

        self._event_loop.call_soon_threadsafe(lambda: self._completion.set())

    def on_stop(self):

        if self.state() == actors.ActorState.ERROR:
            self._error = self.error()

        self._mark_complete()

    async def complete(self, request_timeout: float) -> _T_RESPONSE:

        try:

            completion_task = asyncio.create_task(self._completion.wait())
            await asyncio.wait_for(completion_task, request_timeout)

            if self._error:
                raise self._error

            elif self._grpc_code != grpc.StatusCode.OK:
                self._log.info("API call failed: %s() %s %s", self._method, self._grpc_code.name, self._grpc_message)
                self._context.set_code(self._grpc_code)
                self._context.set_details(self._grpc_message)

            elif self._response is not None:
                self._log.info("API call succeeded: %s()", self._method)
                return self._response

            else:
                raise ex.EUnexpected()

        except TimeoutError:
            self._completion.set()
            self._context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
            self._context.set_details("The TRAC runtime engine did not respond")
            self._log.error("API call failed: %s() %s", self._method, "The TRAC runtime engine did not respond")
            raise

        except Exception as e:
            self._context.set_code(grpc.StatusCode.INTERNAL)
            self._context.set_details("Internal server error")
            self._log.error("API call failed: %s() %s", self._method, str(e))
            self._log.exception(e)
            raise

        finally:
            self.threadsafe().stop()


ApiRequest._log = util.logger_for_class(ApiRequest)


class ListJobsRequest(ApiRequest[runtime_pb2.ListJobsRequest, runtime_pb2.ListJobsResponse]):

    def __init__(self, engine_id, request, context):
        super().__init__(engine_id, "get_job_list", request, context)

    def on_start(self):
        self.actors().send(self._engine_id, "get_job_list")

    @actors.Message
    def job_list(self, job_list):

        self._response = runtime_pb2.ListJobsResponse(
            jobs=codec.encode(job_list))

        self._mark_complete()


class GetJobStatusRequest(ApiRequest[runtime_pb2.JobInfoRequest, runtime_pb2.JobStatus]):

    def __init__(self, engine_id, request, context):

        super().__init__(engine_id, "get_job_status", request, context)

        if request.HasField("jobKey"):
            self._job_key = self._request.jobKey
        elif request.HasField("jobSelector"):
            self._job_key = util.object_key(self._request.jobSelector)
        else:
            raise ex.EValidation("Bad request: Neither jobKey nor jobSelector is specified")

    def on_start(self):
        self.actors().send(self._engine_id, "get_job_details", self._job_key, details=False)

    @actors.Message
    def job_details(self, job_details: tp.Optional[config.JobResult]):

        if job_details is None:
            self._grpc_code = grpc.StatusCode.NOT_FOUND
            self._grpc_message = f"Job not found: [{self._job_key}]"

        else:
            self._response = runtime_pb2.JobStatus(
                jobId=codec.encode(job_details.jobId),
                statusCode=codec.encode(job_details.statusCode),
                statusMessage=codec.encode(job_details.statusMessage))

        self._mark_complete()
