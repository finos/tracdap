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
import subprocess as sp

import tracdap.rt.config as cfg
import tracdap.rt.metadata as meta
import tracdap.rt.exec.runtime as runtime
import tracdap.rt.impl.util as util


class ImportModelTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()

    def setUp(self) -> None:

        repo_url_proc = sp.run(["git", "config", "--get", "remote.origin.url"], stdout=sp.PIPE)
        commit_hash_proc = sp.run(["git", "rev-parse", "HEAD"], stdout=sp.PIPE)

        if repo_url_proc.returncode != 0 or commit_hash_proc.returncode != 0:
            raise RuntimeError("Could not discover details of the current git repo")

        self.repo_url = repo_url_proc.stdout.decode('utf-8').strip()
        self.commit_hash = commit_hash_proc.stdout.decode('utf-8').strip()

        repos = {
            "unit_test_repo": cfg.RepositoryConfig(
                repoType="git",
                repoUrl=self.repo_url)
        }

        self.sys_config = cfg.RuntimeConfig(repositories=repos)

    def test_import_from_git_ok(self):

        job_id = util.new_object_id(meta.ObjectType.JOB)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.IMPORT_MODEL,
            importModel=meta.ImportModelJob(
                language="python",
                repository="unit_test_repo",
                path="examples/models/python/hello_world",
                entryPoint="hello_world.HelloWorldModel",
                version=self.commit_hash))

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
