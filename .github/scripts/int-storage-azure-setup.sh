#!/bin/sh

# Licensed to the Fintech Open Source Foundation (FINOS) under one or
# more contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright ownership.
# FINOS licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# List bucket contents to check the connection
# (bucket may be empty, otherwise limit to first 10 objects)

az storage blob list \
    --auth-mode login \
    --account-name ${TRAC_AZURE_STORAGE_ACCOUNT} \
    --container-name ${TRAC_AZURE_CONTAINER} \
    --num-results 10

# Apache Arrow's Azure FS implementation doesn't support workflow identities from OIDC
# However, it is possible to generate a SAS token once logged in, and use that

# Arrow GCP FS used to have a similar issue, which was eventually addressed
# https://github.com/apache/arrow/issues/34595

SAS_TOKEN_EXPIRY=`date -v+1d +%Y-%m-%d`

SAS_TOKEN=`az storage container generate-sas \
  --account-name ${TRAC_AZURE_STORAGE_ACCOUNT} \
  --name ${TRAC_AZURE_CONTAINER} \
  --expiry ${SAS_TOKEN_EXPIRY} \
  --permissions rwdl`

echo "TRAC_AZURE_CREDENTIALS=sas_token" >> ${GITHUB_ENV}
echo "TRAC_AZURE_SAS_TOKEN=${SAS_TOKEN}" >> ${GITHUB_ENV}
echo "TRAC_AZURE_SAS_TOKEN_EXPIRY=900" >> ${GITHUB_ENV}
