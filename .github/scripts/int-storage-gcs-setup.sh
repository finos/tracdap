#!/bin/bash

# Copyright 2023 Accenture Global Solutions Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# GCP CLI uses bash profile to setup CLI paths
# So this script requires bash rather than just sh

. ~/.bashrc

# Log in to GCP using the credentials provided by the GitHub Action for Google Auth

gcloud auth login --cred-file ${GOOGLE_GHA_CREDS_PATH}

# List bucket contents to check the connection

gsutil ls gs://${TRAC_GCP_BUCKET} | head

# Apache Arrow's GCP FS implementation doesn't support external accounts
# https://github.com/apache/arrow/issues/34595

# So, set up access token auth by configuring the ENV vars read by TRAC integration tests

echo "TRAC_GCP_CREDENTIALS=access_token" >> ${GITHUB_ENV}
echo "TRAC_GCP_ACCESS_TOKEN=`gcloud auth print-access-token`" >> ${GITHUB_ENV}
echo "TRAC_GCP_ACCESS_TOKEN_EXPIRY=900" >> ${GITHUB_ENV}
