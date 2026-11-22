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

import unittest

import tracdap.rt.metadata as meta

import tracdap_test.rt.suites.job_exit_suite as job_exit
import tracdap_test.rt.plugins.int_storage_azure as int_storage_azure


class BlobJobExitTest(job_exit.JobExitTestSuite, unittest.TestCase):

    """Job exit with the results and progress snapshot in Azure Blob storage"""

    def _job_store(self) -> meta.ResourceDefinition:
        return meta.ResourceDefinition(
            resourceType=meta.ResourceType.INTERNAL_STORAGE, protocol="BLOB",
            properties=int_storage_azure.BlobArrowStorageTest._properties_from_env())  # noqa


if __name__ == "__main__":
    unittest.main()
