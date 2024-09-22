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

import pathlib
import subprocess as sp
import unittest

import tracdap.rt.metadata as meta
import tracdap.rt.config as config
import tracdap.rt._exec.runtime as runtime # noqa
import tracdap.rt._impl.util as util  # noqa

import grpc
import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2 as runtime_pb2
import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2_grpc as runtime_grpc

_ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../../../..") \
    .resolve()


class RuntimeApiServerTest(unittest.TestCase):

    UNIT_TEST_API_PORT=10000
    UNIT_TEST_ADDRESS = f"localhost:{UNIT_TEST_API_PORT}"

    SYS_CONFIG = config.RuntimeConfig(
        runtimeApi=config.ServiceConfig(
            enabled=True,
            port=UNIT_TEST_API_PORT),
        repositories={
            "unit_test_repo": config.PluginConfig(
                protocol="local",
                properties={
                    "repoUrl": str(_ROOT_DIR)
                })},
        storage=config.StorageConfig())

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()

    def test_server_start_stop(self):

        with runtime.TracRuntime(self.SYS_CONFIG):
            pass

    def test_list_jobs_empty(self):

        with runtime.TracRuntime(self.SYS_CONFIG):
            with grpc.insecure_channel(self.UNIT_TEST_ADDRESS) as channel:

                client = runtime_grpc.TracRuntimeApiStub(channel)
                request = runtime_pb2.RuntimeListJobsRequest()
                response: runtime_pb2.RuntimeListJobsResponse = client.listJobs(request)

                self.assertIsInstance(response, runtime_pb2.RuntimeListJobsResponse)
                self.assertEqual(0, len(response.jobs))

    def test_get_job_status(self):

        commit_hash_proc = sp.run(["git", "rev-parse", "HEAD"], stdout=sp.PIPE)
        commit_hash = commit_hash_proc.stdout.decode('utf-8').strip()

        job_id = util.new_object_id(meta.ObjectType.JOB)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.IMPORT_MODEL,
            importModel=meta.ImportModelJob(
                language="python",
                repository="unit_test_repo",
                package="trac-example-models",
                version=commit_hash,
                entryPoint="tutorial.using_data.UsingDataModel",
                path="examples/models/python/src"))

        job_config = config.JobConfig(job_id, job_def)

        with runtime.TracRuntime(self.SYS_CONFIG) as rt:
            with grpc.insecure_channel(self.UNIT_TEST_ADDRESS) as channel:

                rt.submit_job(job_config)
                rt.wait_for_job(job_id)

                client = runtime_grpc.TracRuntimeApiStub(channel)
                request = runtime_pb2.RuntimeJobInfoRequest(jobKey=util.object_key(job_id))
                response: runtime_pb2.RuntimeJobStatus = client.getJobStatus(request)

                self.assertEqual(job_id.objectId, response.jobId.objectId)
                self.assertEqual(meta.JobStatusCode.SUCCEEDED.value, response.statusCode)

    def test_get_job_status_missing(self):

        job_id = util.new_object_id(meta.ObjectType.JOB)

        with runtime.TracRuntime(self.SYS_CONFIG):
            with grpc.insecure_channel(self.UNIT_TEST_ADDRESS) as channel:

                client = runtime_grpc.TracRuntimeApiStub(channel)
                request = runtime_pb2.RuntimeJobInfoRequest(jobKey=util.object_key(job_id))

                with self.assertRaises(grpc.RpcError) as error_ctx:
                    client.getJobStatus(request)

                self.assertEqual(grpc.StatusCode.NOT_FOUND, error_ctx.exception.code())

    def test_get_job_result(self):

        commit_hash_proc = sp.run(["git", "rev-parse", "HEAD"], stdout=sp.PIPE)
        commit_hash = commit_hash_proc.stdout.decode('utf-8').strip()

        job_id = util.new_object_id(meta.ObjectType.JOB)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.IMPORT_MODEL,
            importModel=meta.ImportModelJob(
                language="python",
                repository="unit_test_repo",
                package="trac-example-models",
                version=commit_hash,
                entryPoint="tutorial.using_data.UsingDataModel",
                path="examples/models/python/src"))

        job_config = config.JobConfig(job_id, job_def)

        with runtime.TracRuntime(self.SYS_CONFIG) as rt:
            with grpc.insecure_channel(self.UNIT_TEST_ADDRESS) as channel:

                rt.submit_job(job_config)
                rt.wait_for_job(job_id)

                client = runtime_grpc.TracRuntimeApiStub(channel)
                request = runtime_pb2.RuntimeJobInfoRequest(jobKey=util.object_key(job_id))
                response: runtime_pb2.RuntimeJobResult = client.getJobResult(request)

                self.assertEqual(job_id.objectId, response.jobId.objectId)
                self.assertEqual(meta.JobStatusCode.SUCCEEDED.value, response.statusCode)
                self.assertTrue(len(response.results) > 0)
