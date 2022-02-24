#  Copyright 2022 Accenture Global Solutions Limited
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
import tempfile

import trac.rt.config as cfg
import trac.rt.metadata as meta
import trac.rt.exec.runtime as runtime
import trac.rt.impl.util as util


class ImportModelTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()

    def setUp(self) -> None:

        repos = {
            "trac_git_repo": cfg.RepositoryConfig(
                repoType="git",
                repoUrl="https://github.com/accenture/trac")
        }

        self.sys_config = cfg.RuntimeConfig(repositories=repos)
        # self.trac_runtime = runtime.TracRuntime(self.sys_config)

    def test_import_from_git_ok(self):

        job_id = util.new_object_id(meta.ObjectType.JOB)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.IMPORT_MODEL,
            importModel=meta.ImportModelJob(
                language="python",
                repository="trac_git_repo",
                path="examples/models/python/hello_world",
                entryPoint="hello_world.HelloWorldModel",
                version="main"))

        job_config = cfg.JobConfig(job_id, job_def)

        with tempfile.TemporaryDirectory() as tmpdir:

            trac_runtime = runtime.TracRuntime(
                self.sys_config, job_config,
                job_result_dir=tmpdir,
                job_result_format="json")

            trac_runtime.pre_start()

            with trac_runtime as rt:
                rt.submit_batch()
                rt.wait_for_shutdown()
