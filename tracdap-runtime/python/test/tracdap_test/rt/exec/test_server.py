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
import unittest

import tracdap.rt.config as config
import tracdap.rt.metadata as meta
import tracdap.rt._exec.runtime as trac_rt

import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2 as runtime_pb2
import tracdap.rt_gen.grpc.tracdap.api.internal.runtime_pb2_grpc as runtime_grpc
import grpc

_ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../../../..") \
    .resolve()



class EmbeddedJobsTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_embedded_job(self):

        sys_config = config.RuntimeConfig(
            repositories={
                "tutorials": config.PluginConfig(
                    protocol="local",
                    properties={
                        "repoUrl": str(_ROOT_DIR.joinpath("examples/models/python"))
                    })},
            storage=config.StorageConfig())

        rt_args = {
            "server_enabled": True,
            "server_port": 10000
        }

        with trac_rt.TracRuntime(sys_config, **rt_args):

            address = f"localhost:{rt_args['server_port']}"
            channel = grpc.insecure_channel(address)
            client = runtime_grpc.TracRuntimeApiStub(channel)

            request = runtime_pb2.ListJobsRequest()
            response: runtime_pb2.ListJobsResponse = client.listJobs(request)

            self.assertEqual(1, len(response.jobs))
            self.assertEqual(meta.JobStatusCode.RUNNING.value[0], response.jobs[0].statusCode)
