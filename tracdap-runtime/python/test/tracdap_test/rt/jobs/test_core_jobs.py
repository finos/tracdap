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
import pathlib
import subprocess as sp

import tracdap.rt.config as cfg
import tracdap.rt.metadata as meta
import tracdap.rt._exec.runtime as runtime  # noqa
import tracdap.rt._exec.dev_mode as dev_mode  # noqa
import tracdap.rt._impl.type_system as types  # noqa
import tracdap.rt._impl.util as util  # noqa


class CoreJobsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()

    def setUp(self) -> None:

        commit_hash_proc = sp.run(["git", "rev-parse", "HEAD"], stdout=sp.PIPE)
        self.commit_hash = commit_hash_proc.stdout.decode('utf-8').strip()

        current_repo_url = pathlib.Path(__file__) \
            .joinpath("../../../../../../..") \
            .resolve()

        repos = {
            "unit_test_repo": cfg.PluginConfig(
                protocol="local",
                properties={"repoUrl": str(current_repo_url)})
        }

        storage = cfg.StorageConfig(
            buckets={
                "storage_1": cfg.PluginConfig(
                    protocol="LOCAL",
                    properties={"rootPath": str(current_repo_url.joinpath("examples/models/python/data"))}
                )
            },
            defaultBucket="storage_1",
            defaultFormat="CSV"
        )

        self.sys_config = cfg.RuntimeConfig(repositories=repos, storage=storage)

    def test_import_model_job(self):

        job_id = util.new_object_id(meta.ObjectType.JOB)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.IMPORT_MODEL,
            importModel=meta.ImportModelJob(
                language="python",
                repository="unit_test_repo",
                package="trac-example-models",
                version=self.commit_hash,
                entryPoint="tutorial.using_data.UsingDataModel",
                path="examples/models/python/src"))

        job_config = cfg.JobConfig(job_id, job_def)

        with tempfile.TemporaryDirectory() as tmpdir:

            trac_runtime = runtime.TracRuntime(
                self.sys_config,
                job_result_dir=tmpdir,
                job_result_format="json")

            trac_runtime.pre_start()

            with trac_runtime as rt:
                rt.submit_job(job_config)
                rt.wait_for_job(job_id)

    def test_run_model_job(self):

        job_id, job_config = self._build_run_model_job_config()

        with tempfile.TemporaryDirectory() as tmpdir:

            scratch_dir = pathlib.Path(tmpdir)

            job_config = dev_mode.DevModeTranslator.translate_job_config(
                self.sys_config, job_config, scratch_dir, None, None)

            trac_runtime = runtime.TracRuntime(
                self.sys_config,
                job_result_dir=tmpdir,
                job_result_format="json")

            trac_runtime.pre_start()

            with trac_runtime as rt:
                rt.submit_job(job_config)
                rt.wait_for_job(job_id)

    def test_run_model_job_external_schemas(self):

        job_id, job_config = self._build_run_model_job_config()

        with tempfile.TemporaryDirectory() as tmpdir:

            scratch_dir = pathlib.Path(tmpdir)

            job_config = dev_mode.DevModeTranslator.translate_job_config(
                self.sys_config, job_config, scratch_dir, None, None)

            # Make the input dataset use an external schema

            input_id = job_config.job.runModel.inputs["customer_loans"]
            input_data_def = util.get_job_resource(input_id, job_config)

            input_schema_id = util.new_object_id(meta.ObjectType.SCHEMA)
            input_schema = meta.ObjectDefinition(meta.ObjectType.SCHEMA, schema=input_data_def.data.schema)
            job_config.resources[util.object_key(input_schema_id)] = input_schema

            input_data_def.data.schemaId = util.selector_for(input_schema_id)
            input_data_def.data.schema = None

            # Now continue with the job as normal

            trac_runtime = runtime.TracRuntime(
                self.sys_config,
                job_result_dir=tmpdir,
                job_result_format="json")

            trac_runtime.pre_start()

            with trac_runtime as rt:
                rt.submit_job(job_config)
                rt.wait_for_job(job_id)

    def _build_run_model_job_config(self):

        job_id = util.new_object_id(meta.ObjectType.JOB)
        model_id = util.new_object_id(meta.ObjectType.MODEL)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.RUN_MODEL,
            runModel=meta.RunModelJob(
                model=util.selector_for(model_id),
                parameters={
                    "eur_usd_rate": meta.Value(floatValue=1.0, type=types.TypeMapping.python_to_trac(float)),
                    "default_weighting": meta.Value(floatValue=2.0, type=types.TypeMapping.python_to_trac(float)),
                    "filter_defaults": meta.Value(booleanValue=True, type=types.TypeMapping.python_to_trac(bool))
                },
                # Let dev mode translator sort out the data / storage definitions
                inputs={"customer_loans": "inputs/loan_final313_100.csv"},  # noqa
                outputs={"profit_by_region": "outputs/hello_pandas/profit_by_region.csv"}  # noqa
            ))

        model_def = meta.ObjectDefinition(
            objectType=meta.ObjectType.MODEL,
            model=meta.ModelDefinition(
                language="python",
                repository="unit_test_repo",
                package="trac-example-models",
                version=self.commit_hash,
                entryPoint="tutorial.using_data.UsingDataModel",
                path="examples/models/python/src",
                parameters={
                    "eur_usd_rate": meta.ModelParameter(paramType=types.TypeMapping.python_to_trac(float)),
                    "default_weighting": meta.ModelParameter(paramType=types.TypeMapping.python_to_trac(float)),
                    "filter_defaults": meta.ModelParameter(paramType=types.TypeMapping.python_to_trac(bool)),
                },
                inputs={
                    "customer_loans": meta.ModelInputSchema(meta.SchemaDefinition(
                        schemaType=meta.SchemaType.TABLE,
                        table=meta.TableSchema(fields=[
                            meta.FieldSchema("id", fieldType=meta.BasicType.STRING, businessKey=True),
                            meta.FieldSchema("loan_amount", fieldType=meta.BasicType.DECIMAL),
                            meta.FieldSchema("total_pymnt", fieldType=meta.BasicType.DECIMAL),
                            meta.FieldSchema("region", fieldType=meta.BasicType.STRING, categorical=True),
                            meta.FieldSchema("loan_condition_cat", fieldType=meta.BasicType.INTEGER)
                        ])))
                },
                outputs={
                    "profit_by_region": meta.ModelOutputSchema(meta.SchemaDefinition(
                        schemaType=meta.SchemaType.TABLE,
                        table=meta.TableSchema(fields=[
                            meta.FieldSchema("region", fieldType=meta.BasicType.STRING, categorical=True),
                            meta.FieldSchema("gross_profit", fieldType=meta.BasicType.DECIMAL)
                        ])))
                }
            ))

        job_config = cfg.JobConfig(job_id, job_def)
        job_config.resources[util.object_key(model_id)] = model_def

        return job_id, job_config

