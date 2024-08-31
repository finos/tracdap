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

import typing as tp
import concurrent.futures as futures
import uuid

import tracdap.rt._impl.grpc.codec as grpc_codec
import tracdap.rt.metadata as meta

# Imports for gRPC generated code, these are managed by build_runtime.py for distribution
import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2 as runtime_pb2
import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2_grpc as runtime_grpc
import grpc


class RuntimeApiServer(runtime_grpc.TracRuntimeApiServicer):

    __THREAD_POOL_DEFAULT_SIZE = 2
    __THREAD_NAME_PREFIX = "server-"
    __DEFAULT_SHUTDOWN_TIMEOUT = 10.0  # seconds

    def __init__(self, port: int, n_workers: int = None):
        self.__port = port
        self.__n_workers = n_workers or self.__THREAD_POOL_DEFAULT_SIZE
        self.__server: tp.Optional[grpc.Server] = None
        self.__thread_pool: tp.Optional[futures.ThreadPoolExecutor] = None

    def listJobs(self, request, context):

        # TODO: This is a dummy implementation to start testing server connection

        job_id = meta.TagHeader(meta.ObjectType.JOB, str(uuid.uuid4()), objectVersion=1, tagVersion=1)
        status_code = meta.JobStatusCode.RUNNING
        status_message = "In progress"

        job_status = runtime_pb2.JobStatus(
            jobId=grpc_codec.encode(job_id),
            statusCode=grpc_codec.encode(status_code),
            statusMessage=status_message)

        response = runtime_pb2.ListJobsResponse(jobs=[job_status])

        return response

    def getJobStatus(self, request: runtime_pb2.BatchJobStatusRequest, context: grpc.ServicerContext):
        return super().getJobStatus(request, context)

    def getJobDetails(self, request, context):
        return super().getJobDetails(request, context)

    def start(self):

        self.__thread_pool = futures.ThreadPoolExecutor(
            max_workers=self.__n_workers,
            thread_name_prefix=self.__THREAD_NAME_PREFIX)

        self.__server = grpc.server(self.__thread_pool)

        socket = f"[::]:{self.__port}"
        self.__server.add_insecure_port(socket)

        runtime_grpc.add_TracRuntimeApiServicer_to_server(self, self.__server)

        self.__server.start()

    def stop(self, shutdown_timeout: float = None):

        grace = shutdown_timeout or self.__DEFAULT_SHUTDOWN_TIMEOUT

        if self.__server is not None:
            self.__server.stop(grace)

        if self.__thread_pool is not None:
            self.__thread_pool.shutdown()
