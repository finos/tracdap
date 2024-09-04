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
import concurrent.futures
import threading
import typing as tp

import tracdap.rt.exceptions as ex
import tracdap.rt._exec.actors as actors
import tracdap.rt._exec.engine as engine
import tracdap.rt._impl.grpc.codec as codec  # noqa
import tracdap.rt._impl.util as util  # noqa

# Check whether gRPC is installed before trying to load any of the generated modules
try:
    import grpc.aio
    import google.protobuf.message as _msg
except ImportError:
    raise ex.EStartup("The runtime API server cannot be enabled because gRPC libraries are not installed")

# Imports for gRPC generated code, these are managed by build_runtime.py for distribution
import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2 as runtime_pb2
import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2_grpc as runtime_grpc


class ApiRequest(actors.Actor):

    def __init__(
            self, method: str, request: _msg.Message,
            response_type: _msg.Message.__class__,
            context: grpc.aio.ServicerContext):

        super().__init__()

        self._api_target = actors.ActorId()  # TODO

        self._method = method
        self._request = request
        self._response_type = response_type
        self._context = context

    def on_start(self):
        self.actors().send(self._api_target, self._method, self._request)

    @actors.Message
    def api_response(self, response: tp.Any):

        try:
            response_msg = self._response_type(codec.encode(response))
            self._context.write(response_msg)
            self._context.set_code(grpc.StatusCode.OK)

        except Exception:
            self._context.set_code(grpc.StatusCode.INTERNAL)


class RuntimeApiServer:

    __THREAD_POOL_DEFAULT_SIZE = 2
    __THREAD_NAME_PREFIX = "server-"
    __DEFAULT_SHUTDOWN_TIMEOUT = 10.0  # seconds
    __DEFAULT_REQUEST_TIMEOUT = 10.0

    def __init__(self, system: actors.ActorSystem, port: int, n_workers: int = None):

        self.__log = util.logger_for_object(self)

        self.__system = system
        self.__port = port

        self.__server: tp.Optional[grpc.aio.Server] = None
        self.__server_thread: tp.Optional[threading.Thread] = None
        self.__server_signal: tp.Optional[asyncio.Event] = None
        self.__event_loop: tp.Optional[asyncio.AbstractEventLoop] = None

    def start(self):

        self.__server_thread = threading.Thread(target=self.__server_control, name="api_server", daemon=True)
        self.__server_thread.start()

    def __server_control(self):

        self.__server_signal = asyncio.Event()

        self.__event_loop = asyncio.new_event_loop()
        self.__event_loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(thread_name_prefix="api_server"))
        self.__event_loop.run_until_complete(self.__server_main())
        self.__event_loop.close()

    async def __server_main(self):

        socket = f"[::]:{self.__port}"

        self.__server = grpc.aio.server()
        self.__server.add_insecure_port(socket)
        runtime_grpc.add_TracRuntimeApiServicer_to_server(self, self.__server)
        await self.__server.start()

        self.__log.info(f"Runtime API server is up and listening on port [{self.__port}]")

        await asyncio.create_task(self.__server_signal.wait())

        self.__log.info(f"Shutdown signal received, runtime API server is going down...")

        await self.__server.stop(self.__DEFAULT_SHUTDOWN_TIMEOUT)
        self.__server = None

        self.__log.info("Runtime API server has gone down cleanly")

    def stop(self, shutdown_timeout: float = None):

        if self.__server is None:
            return

        timeout = shutdown_timeout or self.__DEFAULT_SHUTDOWN_TIMEOUT

        self.__event_loop.call_soon_threadsafe(lambda: self.__server_signal.set())
        self.__server_thread.join(timeout)

        if self.__server_thread.is_alive():
            self.__log.warning("Runtime API server did not go down cleanly")

    async def listJobs(self, request: runtime_pb2.ListJobsRequest, context: grpc.aio.ServicerContext):

        self.__log.info("API Request: listJobs")

        return runtime_pb2.ListJobsResponse()

        # request = ApiRequest("list_jobs", request, runtime_pb2.ListJobsResponse, context)
        # self.__system.send("api_request", request)

    async def getJobStatus(self, request: runtime_pb2.BatchJobStatusRequest, context: grpc.ServicerContext):

        request = ApiRequest("get_job_status", request, runtime_pb2.JobStatus, context)
        self.__system.send("api_request", request)

    async def getJobDetails(self, request, context):

        request = ApiRequest("get_job_details", request, runtime_pb2.JobStatus, context)
        self.__system.send("api_request", request)
